"""Single source of truth for how an APEX social post is counted.

The daily Slack report and the marketing dashboard are supposed to publish the
same numbers. They did not, because each had its own copy of the rules: the
dashboard grew quote-post handling, owned-post dedupe and 2nd-degree tracking
that the Slack report never got, and the keyword list drifted between the two.
By August the two disagreed by ~2.9M impressions on the same month.

Everything that decides *whether a post counts* and *which column it lands in*
lives here, and both surfaces import it. Nothing in this module calls an API, so
it is cheap to import and safe to unit-test.

Bucket resolution, first match wins:

  1. one of our own accounts wrote it        -> that account's column
  2. an outside account quoted our APEX post -> Quote posts
  3. an outside post names an APEX keyword   -> 3rd Party
  4. otherwise                               -> not counted

2nd degree (someone quoting a third party who quoted us) is tracked separately
and deliberately excluded from every total.
"""

import re
from collections import defaultdict

TWEET_ID_RE = re.compile(r"/status/(\d+)")

APEX_KEYWORDS = [
    "apex-agents", "apex agents", "apex-agents-aa",
    "apex-swe", "apex swe",
    "apex-ace", "apex ace",
    "apex-accounting", "apex accounting",
]

# Cache `account` labels meaning "we posted this".
OWN_POST_SOURCES = {"Mercor", "Mercor Twitter", "Brendan Foody", "Adarsh"}

# Handle -> column, for quotes written by us. A quote of our own post is our own
# reach, so it belongs in the author's column and not in Quote posts.
OWN_X_ACCOUNTS = {
    "mercor_ai": "Mercor Twitter",
    "brendanfoody": "Brendan",
    "adarsh_exe": "Adarsh",
}

# Columns that roll into the X total. Quote posts is included; 2nd degree is not.
X_BUCKETS = ["Mercor Twitter", "Brendan", "Adarsh", "3rd Party", "Quote posts"]


def matches_apex(text):
    """True if a post is about an APEX benchmark.

    Either a named benchmark keyword, or "apex" and "mercor" together — the X
    search query already asks for `(apex mercor)`, so without that clause the
    filter paid for results and then discarded them.
    """
    t = (text or "").lower()
    if any(kw in t for kw in APEX_KEYWORDS):
        return True
    return "apex" in t and "mercor" in t


def tweet_id(link):
    m = TWEET_ID_RE.search(link or "")
    return m.group(1) if m else None


def dedupe_owned(sprout_posts, cache_posts):
    """Drop the X-cache copy of any post Sprout already reports.

    Our own posts reach us twice: Sprout reports them as owned-profile posts, and
    the X cache picks them up through the retweet/quote discovery paths. Counting
    both double-counts every viral own post — this is what overstated July by
    1.85M on the @mercor_ai Grok post. Sprout is authoritative for owned
    profiles, so it wins.

    Returns (posts, dropped) with Sprout first so it takes precedence.
    """
    posts, seen, dropped = [], set(), []
    for p in list(sprout_posts) + list(cache_posts):
        tid = tweet_id(p.get("perma_link"))
        if tid:
            if tid in seen:
                dropped.append(p)
                continue
            seen.add(tid)
        posts.append(p)
    return posts, dropped


def attach_quoted_ids(cache_posts, cache):
    """Copy each cached tweet's stored quoted_id onto its post dict."""
    tweets = cache.get("tweets", {})
    for p in cache_posts:
        tid = tweet_id(p.get("perma_link"))
        if tid:
            p["quoted_id"] = tweets.get(tid, {}).get("quoted_id")
    return cache_posts


def our_post_ids(posts):
    """Tweet ids of posts we published. Sprout posts carry no `source`."""
    out = set()
    for p in posts:
        src = p.get("source")
        if src is not None and src not in OWN_POST_SOURCES:
            continue
        tid = tweet_id(p.get("perma_link"))
        if tid:
            out.add(tid)
    return out


def stored_quotes(cache, posts):
    """Quote rows already discovered, restricted to quotes of OUR posts.

    Reads cache["quote_scans"] only — no API calls — so both surfaces can report
    quotes without either of them burning the shared X rate limit.
    """
    ours = our_post_ids(posts)
    seen, rows = set(), []
    for src, scan in (cache.get("quote_scans") or {}).items():
        if src not in ours:
            continue                      # quotes of a third party are 2nd degree
        for q in scan.get("quotes", []):
            qid = tweet_id(q.get("link"))
            if not qid or qid in seen:
                continue
            seen.add(qid)
            rows.append(q)
    return rows


def classify_quotes(quotes, posts):
    """Assign quotes to buckets and say which cached posts to drop.

    Returns (bucketed, kept, dropped_count) where bucketed is [(column, row)].

    Two ways a quote of ours is recognised: it came back from the quote_tweets
    endpoint, or a cached post's stored quoted_id points at one of our posts. The
    second path exists because quote_tweets is paginated and page 1 of a viral
    post can hide the biggest quote — that is how @elonmusk's 2.6M quote sat in
    3rd Party for weeks.
    """
    bucketed, quote_ids = [], set()
    for q in quotes:
        qid = tweet_id(q.get("link"))
        if qid:
            quote_ids.add(qid)
        handle = (q.get("handle") or "").lstrip("@").lower()
        bucketed.append((OWN_X_ACCOUNTS.get(handle, "Quote posts"), q))

    ours = our_post_ids(posts)
    kept, reclassified = [], 0
    for p in posts:
        tid = tweet_id(p.get("perma_link"))
        if tid and tid in quote_ids:
            continue                       # already counted as a quote row
        quoted = p.get("quoted_id")
        if (tid and quoted and quoted in ours
                and p.get("source") not in OWN_POST_SOURCES):
            metrics = p.get("metrics", {})
            bucketed.append(("Quote posts", {
                "date": (p.get("created_time") or "")[:10],
                "handle": p.get("handle"),
                "text": (p.get("text") or "")[:160],
                "link": p.get("perma_link") or "",
                "impressions": metrics.get("lifetime.impressions", 0),
                "engagements": metrics.get("lifetime.engagements", 0),
            }))
            reclassified += 1
            continue
        kept.append(p)
    return bucketed, kept, reclassified


def apply_quotes(monthly, post_log, bucketed):
    """Fold quote rows into an existing monthly table from build_report."""
    for bucket, q in bucketed:
        month = (q.get("date") or "")[:7]
        if not month:
            continue
        imp = q.get("impressions", 0) or 0
        eng = q.get("engagements", 0) or 0
        monthly[month][f"{bucket} Impressions"] += imp
        monthly[month]["Total Impressions"] += imp
        monthly[month]["Twitter Total Impressions"] += imp
        monthly[month]["Total Engagements"] += eng
        post_log.append({
            "date": q.get("date", ""), "account": bucket, "handle": q.get("handle"),
            "impressions": imp, "engagements": eng,
            "link": q.get("link", ""), "text": (q.get("text") or "")[:120],
        })
    return monthly, post_log


# Bump when a change to the rules above legitimately alters historic numbers.
# merge_with_baseline keeps the higher of fresh vs baseline for past months, to
# stop a failed API call silently deleting history. The side effect is that an
# over-count gets cemented forever and no correction can ever land. Stamping the
# baseline with the version that produced it lets a real correction through
# exactly once, while keeping the API-blip protection every other run.
LOGIC_VERSION = "2026-08-03.quotes-dedupe-2nddegree"


def validate_monthly(monthly, tolerance=2):
    """Invariants that must hold before numbers are published anywhere.

    Returns a list of human-readable problems; empty means good. Cheap and
    dependency-free so the Slack report can run it right before it sends, and
    refuse to publish rather than publish something wrong.
    """
    # build_report writes per-account keys and roll-ups into the same dict, so a
    # naive "every LinkedIn key" sum counts LinkedIn Total on top of its own parts.
    rollups = {"Total Impressions", "Twitter Total Impressions",
               "LinkedIn Total Impressions", "Total Engagements"}
    problems = []
    for month in sorted(monthly):
        m = monthly[month]
        x = sum(m.get(f"{b} Impressions", 0) for b in X_BUCKETS)
        li = sum(v for k, v in m.items()
                 if k.endswith(" Impressions") and "LinkedIn" in k
                 and k not in rollups)
        stated_x = m.get("Twitter Total Impressions", 0)
        stated_total = m.get("Total Impressions", 0)

        if abs(x - stated_x) > tolerance:
            problems.append(f"{month}: X columns sum to {x:,} but X total says {stated_x:,}")
        if abs((x + li) - stated_total) > tolerance:
            problems.append(f"{month}: columns sum to {x + li:,} but total says "
                            f"{stated_total:,}")
        for k, v in m.items():
            if k.endswith("Impressions") and v < 0:
                problems.append(f"{month}: {k} is negative ({v:,})")
    return problems


def second_degree_by_month(cache):
    """Reach of quotes of third-party posts that themselves quote us.

    Reported alongside the columns and in none of the totals: it is real APEX
    conversation but it is not our post being seen.
    """
    out = defaultdict(int)
    for scan in (cache.get("second_degree_scans") or {}).values():
        for q in scan.get("quotes", []):
            month = (q.get("date") or "")[:7]
            if month:
                out[month] += q.get("impressions", 0) or 0
    return dict(out)
