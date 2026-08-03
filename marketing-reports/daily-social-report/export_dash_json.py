#!/usr/bin/env python3
"""Write ~/marketing-dash/data/social.json for the dashboard's Social & Sponsorships tab.

This is the dashboard version of the daily APEX social Slack report. It imports
slack_report.py rather than reimplementing it, so the two can never drift — but
it does NOT touch the Slack job, which keeps running as-is.

Three blocks of output:
  apex          the APEX social report — YTD totals, monthly impressions by
                account, MTD top posts. Same numbers the Slack report sends.
  placements    paid 3rd-party placements from placements.json, with X metrics
                pulled live and manual YouTube / podcast numbers merged in.
  earned        3rd-party APEX mentions already in tweet_cache.json, plus any
                Mercor mention from a partner account (earned amplification off
                a paid relationship).

Usage:
    python3 export_dash_json.py [--out PATH] [--no-refresh]

--no-refresh skips the Twitter/Sprout discovery calls and builds from the
existing tweet cache. Use it when iterating on the dashboard.
"""
import argparse
import json
import os
import pathlib
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

import requests

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
# Append, do NOT insert: the repo root also holds a slack_report.py, and putting
# it ahead of HERE makes `import slack_report` resolve to the wrong copy.
sys.path.append(str(HERE.parent.parent))
import slack_report as rpt  # noqa: E402  (module-level is just env + constants)
import apex_social  # noqa: E402  the shared bucket rules, imported by both surfaces

PLACEMENTS_PATH = HERE / "placements.json"
DEFAULT_OUT = pathlib.Path.home() / "marketing-dash" / "data" / "social.json"

# The Slack report groups Twitter accounts into these buckets; keep the same
# names so the dashboard table and the Slack table are directly comparable.
TWITTER_BUCKETS = ["Mercor Twitter", "Brendan", "Adarsh", "3rd Party", "Quote posts"]


# --- X --------------------------------------------------------------------

def x_get(path, params):
    token = rpt.TWITTER_BEARER_TOKEN
    if not token:
        return None
    try:
        r = requests.get(f"https://api.twitter.com/2/{path}",
                         headers={"Authorization": f"Bearer {token}"},
                         params=params, timeout=30)
    except Exception as e:
        print(f"  X request failed: {e}")
        return None
    if "quote_tweets" in path:
        global _quote_quota_left
        rem = r.headers.get("x-rate-limit-remaining")
        if rem is not None:
            try:
                _quote_quota_left = int(rem)
            except ValueError:
                pass
        elif r.status_code == 429:
            _quote_quota_left = 0
    if not r.ok:
        print(f"  X {path} returned {r.status_code}: {r.text[:200]}")
        return None
    return r.json()


def fetch_tweets(ids):
    """Live public metrics for specific tweet ids. 100 per call."""
    out = {}
    ids = [i for i in ids if i]
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        d = x_get("tweets", {
            "ids": ",".join(chunk),
            "tweet.fields": "created_at,public_metrics,text",
        })
        for t in (d or {}).get("data", []):
            out[t["id"]] = t
    return out


TWEET_ID_RE = re.compile(r"/status/(\d+)")


def resolve_handles(rows):
    """Fill in the @handle behind each post, in place.

    The tweet cache stores an account *bucket* ("3rd Party"), not the author, so
    every outside mention reads as the same anonymous row. The author is one
    expansion away on a batch tweet lookup — 100 ids per call — so resolve it at
    export time rather than changing the cache schema the Slack report shares.
    """
    ids = []
    for r in rows:
        m = TWEET_ID_RE.search(r.get("link") or "")
        if m:
            r["_tid"] = m.group(1)
            ids.append(m.group(1))

    handles = {}
    for i in range(0, len(ids), 100):
        d = x_get("tweets", {
            "ids": ",".join(ids[i:i + 100]),
            "tweet.fields": "author_id",
            "expansions": "author_id",
            "user.fields": "username,name",
        })
        users = {u["id"]: u for u in ((d or {}).get("includes") or {}).get("users", [])}
        for t in (d or {}).get("data", []):
            u = users.get(t.get("author_id"))
            if u:
                handles[t["id"]] = {"handle": "@" + u["username"], "name": u.get("name")}

    missing = 0
    for r in rows:
        tid = r.pop("_tid", None)
        h = handles.get(tid)
        if h:
            r["handle"], r["authorName"] = h["handle"], h["name"]
        elif tid:
            # Deleted, protected or suspended — the row is still real, we just
            # can't name it. Better than silently showing the wrong author.
            missing += 1
    if missing:
        print(f"  {missing} post(s) had no resolvable author (deleted or protected)")
    return rows


def metrics_of(tweet):
    m = tweet.get("public_metrics", {}) if tweet else {}
    return {
        "impressions": m.get("impression_count", 0),
        "likes": m.get("like_count", 0),
        "reposts": m.get("retweet_count", 0),
        "replies": m.get("reply_count", 0),
        "quotes": m.get("quote_count", 0),
        "bookmarks": m.get("bookmark_count", 0),
        "engagements": (m.get("like_count", 0) + m.get("retweet_count", 0)
                        + m.get("reply_count", 0) + m.get("quote_count", 0)),
    }


# X allows 75 quote_tweets calls per 15-minute window on an app-only bearer, and
# it is one call per source post, so no single run can cover every post. The
# Sponsorships pass calls the same endpoint later in the run out of the same
# window, so its calls are reserved here rather than hardcoded — a new placement
# in placements.json would otherwise silently 429 the sponsorship numbers.
QUOTE_WINDOW_LIMIT = 75
QUOTE_FRESH_RESCAN = 25

# Cache `account` labels that mean "we posted this". Anything else in the cache
# (notably "3rd Party") is someone else's post and is not a quote-scan source.
OWN_POST_SOURCES = apex_social.OWN_POST_SOURCES

# Live quota left in the current quote_tweets window, from X's response headers.
# A static budget only holds when the run starts on a fresh window; if an earlier
# run already spent it, the discovery scan would eat the sponsorship reserve and
# 20VC's numbers would silently drop. Tracking the header makes the reserve real.
_quote_quota_left = None


def sponsorship_quote_calls():
    try:
        registry = json.load(open(PLACEMENTS_PATH))
        return sum(len(p.get("tweet_ids", []))
                   for p in registry.get("placements", []))
    except (OSError, ValueError):
        return 12


def quote_call_budget():
    return max(QUOTE_FRESH_RESCAN,
               QUOTE_WINDOW_LIMIT - sponsorship_quote_calls() - 2)


def apex_quote_posts(posts, refresh, cache):
    """Quote tweets of our own APEX posts.

    The Slack report counts retweets of Mercor accounts but not quotes, so
    someone quoting an APEX leaderboard post with their own commentary was
    invisible unless their text happened to contain an APEX keyword. Walk our
    own APEX posts and ask X who quoted them.

    Each post's result is remembered under cache["quote_scans"], so coverage
    accumulates across runs instead of being permanently capped at the newest N.
    Every run rescans the newest QUOTE_FRESH_RESCAN posts — that is where new
    quotes actually land — and spends the rest of quote_call_budget() on whichever
    posts have gone longest without a scan. Reporting then reads from the stored
    set, so a --no-refresh run still sees every quote found so far.
    """
    budget = quote_call_budget()
    scans = cache.setdefault("quote_scans", {})

    # OUR posts only. The cache also holds third-party tweets, and scanning
    # those made "Quote posts" mean "quotes of anyone we tracked" rather than
    # "quotes of one of our posts" — and burned ~80% of the call budget on posts
    # that are not ours. Sprout posts carry no `source`; cache posts carry the
    # account label, so anything labelled 3rd Party is excluded here.
    own = []
    for p in posts:
        link = p.get("perma_link") or ""
        if "x.com" not in link and "twitter.com" not in link:
            continue
        source = p.get("source")
        if source is not None and source not in OWN_POST_SOURCES:
            continue
        m = TWEET_ID_RE.search(link)
        if m:
            own.append((p.get("created_time", ""), m.group(1)))
    own.sort(reverse=True)

    if refresh:
        fresh = [tid for _, tid in own[:QUOTE_FRESH_RESCAN]]
        # Never-scanned posts sort first on "" — they get the backlog budget.
        backlog = sorted((tid for _, tid in own[QUOTE_FRESH_RESCAN:]),
                         key=lambda t: scans.get(t, {}).get("scanned_at", ""))
        now = datetime.now(timezone.utc).isoformat()
        reserve = sponsorship_quote_calls()
        failed = stopped = 0
        for tid in fresh + backlog[:max(0, budget - len(fresh))]:
            # Leave the Sponsorships pass its calls even when an earlier run in
            # this window already spent most of the quota.
            if _quote_quota_left is not None and _quote_quota_left <= reserve:
                stopped += 1
                continue
            res = fetch_quote_tweets(tid)
            if res is None:
                failed += 1          # keep the previous result; never blank it out
                continue
            scans[tid] = {"scanned_at": now, "quotes": res}
        if failed:
            print(f"  {failed} quote scan(s) failed — previous results kept")
        if stopped:
            print(f"  {stopped} scan(s) skipped to protect the "
                  f"{reserve}-call sponsorship reserve")

    # Return every quote found. Which bucket each one lands in is decided by
    # classify_quotes(), so that Quote posts / 3rd Party / own-account columns
    # stay mutually exclusive.
    seen, rows = set(), []
    for _, tid in own:
        for q in scans.get(tid, {}).get("quotes", []):
            qid = TWEET_ID_RE.search(q["link"]).group(1)
            if qid in seen:
                continue
            seen.add(qid)
            rows.append(q)

    scanned = sum(1 for _, tid in own if tid in scans)
    unscanned = len(own) - scanned
    print(f"  {len(rows)} quote posts from {scanned}/{len(own)} of our APEX posts"
          + (f" ({unscanned} not scanned yet)" if unscanned else " (full coverage)"))
    return rows, unscanned, len(own)


# Our own X accounts. A quote written by one of these is our own reach, not
# third-party amplification, so it belongs in that account's column.
OWN_X_ACCOUNTS = apex_social.OWN_X_ACCOUNTS


def backfill_quoted_ids(cache, refresh):
    """Record, per cached tweet, which tweet it quotes.

    The quote_tweets endpoint is the wrong way round for this: it lists quotes of
    a post one page at a time and is not paginated here, so a post with hundreds
    of quotes hides its biggest one (this is exactly how @elonmusk's 2.6M quote
    of the APEX-SWE post stayed classified as 3rd Party). Asking the other
    direction — what does this tweet quote — is exact and costs one call per 100
    cached tweets. Only fetched once per tweet, then cached.
    """
    tweets = cache.get("tweets", {})
    todo = [tid for tid, t in tweets.items() if "quoted_id" not in t]
    if not todo or not refresh:
        return 0
    resolved = 0
    for i in range(0, len(todo), 100):
        batch = todo[i:i + 100]
        d = x_get("tweets", {"ids": ",".join(batch),
                             "tweet.fields": "referenced_tweets"})
        if d is None:
            break
        got = {t["id"]: t for t in d.get("data", [])}
        for tid in batch:
            refs = (got.get(tid) or {}).get("referenced_tweets") or []
            quoted = next((r["id"] for r in refs if r["type"] == "quoted"), None)
            # None is a real answer ("quotes nothing"); store it so we never re-ask.
            tweets[tid]["quoted_id"] = quoted
            if quoted:
                resolved += 1
    print(f"  resolved quoted_id for {len(todo)} cached tweet(s); {resolved} are quotes")
    return resolved


def classify_quotes(quotes, posts):
    """Delegates to apex_social so the dashboard and the Slack report cannot
    disagree about which bucket a post belongs in. See apex_social for the rules."""
    return apex_social.classify_quotes(quotes, posts)


# --- blocks ---------------------------------------------------------------

def build_apex(refresh):
    """The APEX social report, straight out of slack_report.py's own functions."""
    cache = rpt.load_tweet_cache()
    before = len(cache.get("tweets", {}))

    if refresh:
        rpt.discover_personal_tweets(cache)
        rpt.discover_third_party_mentions(cache)
        rpt.discover_watched_account_tweets(cache)
        rpt.discover_apex_retweets(cache)
        rpt.refresh_tweet_impressions(cache)
        rpt.save_tweet_cache(cache)
    after = len(cache.get("tweets", {}))
    if after < before:
        # The report's own review rule: the cache must never shrink.
        print(f"  WARNING: tweet cache shrank {before} -> {after}")

    # get_sprout_profiles returns (ids, id -> account-name map)
    profile_ids, profile_map = rpt.get_sprout_profiles() if refresh else ([], {})
    # Sprout and the X cache both carry our own owned-account posts: the cache
    # picks them up through the retweet/quote discovery path, and Sprout reports
    # them as owned-profile posts. Counting both double-counts every viral own
    # post (verified: the Jul 10 Grok/APEX-SWE post at 1.85M appeared in each).
    # Sprout is authoritative for owned profiles, so it wins and the cache copy
    # is dropped.
    backfill_quoted_ids(cache, refresh)
    cache_posts = rpt.cache_to_posts(cache)
    # cache_to_posts does not carry quoted_id through; attach it by tweet id so
    # classify_quotes can bucket on it.
    for p in cache_posts:
        m = TWEET_ID_RE.search(p.get("perma_link") or "")
        if m:
            p["quoted_id"] = cache["tweets"].get(m.group(1), {}).get("quoted_id")
    sprout_posts = (rpt.get_all_posts(profile_ids, start_date="2026-01-01")
                    if profile_ids else [])
    posts, seen_tweet_ids, owned_dupes = [], set(), 0
    for p in sprout_posts + cache_posts:
        m = TWEET_ID_RE.search(p.get("perma_link") or "")
        if m:
            if m.group(1) in seen_tweet_ids:
                owned_dupes += 1
                continue
            seen_tweet_ids.add(m.group(1))
        posts.append(p)
    if owned_dupes:
        print(f"  dropped {owned_dupes} tweet(s) counted in both Sprout and the X cache")

    quotes, quotes_unscanned, quote_sources = apex_quote_posts(posts, refresh, cache)
    if refresh:
        # apex_quote_posts writes into cache["quote_scans"]; the save above ran
        # before it, so persist again or the backlog never advances.
        rpt.save_tweet_cache(cache)

    # Assign each quote to its bucket and drop the keyword-search copies of the
    # same tweets, so build_report never sees a post that is counted as a quote.
    bucketed, posts, dropped = classify_quotes(quotes, posts)
    own_quotes = sum(1 for b, _ in bucketed if b != "Quote posts")
    print(f"  quote routing: {len(bucketed) - own_quotes} to Quote posts, "
          f"{own_quotes} to our own account columns, "
          f"{dropped} keyword-search duplicate(s) dropped")

    monthly, post_log = rpt.build_report(posts, profile_map)

    # 2nd-degree: quotes of third-party posts that themselves quote us (someone
    # quoting @elonmusk quoting our APEX post). Real APEX conversation, but one
    # step removed from our own reach, so it is reported alongside and never inside.
    second_degree = defaultdict(int)
    for s in cache.get("second_degree_scans", {}).values():
        for q in s.get("quotes", []):
            second_degree[(q.get("date") or "")[:7]] += q.get("impressions", 0) or 0
    if second_degree:
        print(f"  2nd-degree reach: {sum(second_degree.values()):,} "
              f"across {len(second_degree)} month(s)")

    for bucket, q in bucketed:
        month = q["date"][:7]
        monthly[month][f"{bucket} Impressions"] += q["impressions"]
        monthly[month]["Total Impressions"] += q["impressions"]
        monthly[month]["Twitter Total Impressions"] += q["impressions"]
        monthly[month]["Total Engagements"] += q["engagements"]
        post_log.append({
            "date": q["date"], "account": bucket, "handle": q.get("handle"),
            "impressions": q["impressions"], "engagements": q["engagements"],
            "link": q["link"], "text": q["text"][:120],
        })

    # build_report writes both per-account keys ("Adarsh Impressions") and
    # roll-ups ("LinkedIn Total Impressions") into the same dict. Drop the
    # roll-ups or "LinkedIn Total" shows up as if it were an account.
    rollups = {"Total", "Twitter Total", "LinkedIn Total"}
    accounts = sorted({k[:-len(" Impressions")] for m in monthly.values()
                       for k in m if k.endswith(" Impressions")} - rollups)
    linkedin_accounts = [a for a in accounts if "LinkedIn" in a]

    months = []
    for month in sorted(monthly):
        m = monthly[month]
        months.append({
            "month": month,
            "label": datetime.strptime(month, "%Y-%m").strftime("%b %Y"),
            "byAccount": {a: m.get(f"{a} Impressions", 0)
                          for a in TWITTER_BUCKETS + linkedin_accounts},
            "twitter": m.get("Twitter Total Impressions", 0),
            "linkedin": m.get("LinkedIn Total Impressions", 0),
            "impressions": m.get("Total Impressions", 0),
            "engagements": m.get("Total Engagements", 0),
            # Deliberately a sibling of byAccount, not a member of it: 2nd-degree
            # reach is NOT ours and must never roll into X total or Impressions.
            # Keeping it out of byAccount is also what keeps the reconciler's
            # "columns sum to the total" check meaningful.
            "secondDegree": second_degree.get(month, 0),
        })

    current = datetime.now(timezone.utc).strftime("%Y-%m")
    mtd = sorted((p for p in post_log if p["date"].startswith(current)),
                 key=lambda p: p["impressions"], reverse=True)

    return {
        "cacheSize": after,
        "currentMonth": current,
        "ytdImpressions": sum(m["impressions"] for m in months),
        "ytdEngagements": sum(m["engagements"] for m in months),
        "twitterAccounts": TWITTER_BUCKETS,
        "quotePosts": len(quotes),
        "quotesSkipped": quotes_unscanned,
        "quoteSourcePosts": quote_sources,
        "quoteSourcesScanned": quote_sources - quotes_unscanned,
        # Recorded so reconcile.py can assert the dedupe actually ran, rather
        # than inferring it from a post list that is truncated to 300 rows.
        "ownedDupesDropped": owned_dupes,
        "quoteDupesDropped": dropped,
        "linkedinAccounts": linkedin_accounts,
        "months": months,
        "topPostsMtd": resolve_handles(mtd[:10]),
        "posts": sorted(post_log, key=lambda p: p["date"], reverse=True)[:300],
    }


def fetch_quote_tweets(tweet_id):
    """Amplification of a sponsored post — quote tweets are the one form of
    downstream reach X will give us for free, and they belong in the total."""
    d = x_get(f"tweets/{tweet_id}/quote_tweets", {
        "max_results": 100, "tweet.fields": "created_at,public_metrics,text,author_id",
        "expansions": "author_id", "user.fields": "username,name",
    })
    if d is None:
        # Request failed (rate limit, network). Distinct from "no quotes" — the
        # caller must not persist this over a previous successful scan.
        return None
    users = {u["id"]: u for u in ((d or {}).get("includes") or {}).get("users", [])}
    rows = []
    for t in (d or {}).get("data", []):
        m = metrics_of(t)
        if not m["impressions"]:
            continue
        u = users.get(t.get("author_id")) or {}
        rows.append({
            "surface": "Quote post", "date": t.get("created_at", "")[:10],
            "handle": "@" + u["username"] if u.get("username") else None,
            "authorName": u.get("name"),
            "text": (t.get("text") or "")[:160],
            "link": f"https://x.com/i/web/status/{t['id']}", **m,
        })
    return rows


def build_placements(registry, partner_posts):
    """One row per surface we can actually measure — nothing else.

    A surface only appears if it has a number behind it. An empty "YouTube: not
    tracked" row is noise on a placement that never had a YouTube cut, and it
    makes the total look partial when it isn't.

    `partner_posts` is the partner's own organic Mercor content. It is reach the
    sponsorship bought even though we didn't buy the post, so it is counted here
    rather than filed with unrelated 3rd-party chatter.
    """
    placements = registry.get("placements", [])
    tweets = fetch_tweets([tid for p in placements for tid in p.get("tweet_ids", [])])

    out = []
    for p in placements:
        surfaces = []

        for tid in p.get("tweet_ids", []):
            t = tweets.get(tid)
            if not t:
                print(f"  no X data for tweet {tid} ({p['id']})")
                continue
            surfaces.append({
                "surface": "Sponsored post (X)", "date": t.get("created_at", "")[:10],
                "handle": p.get("handle"),
                "text": (t.get("text") or "")[:160],
                "link": f"https://x.com/i/web/status/{tid}", **metrics_of(t),
            })
            surfaces += fetch_quote_tweets(tid) or []

        for e in partner_posts.get(p["partner"], []):
            surfaces.append({
                "surface": "Partner organic (X)", "date": e["date"],
                "handle": p.get("handle"),
                "text": e["text"][:160], "link": e["link"],
                "impressions": e["impressions"], "engagements": e["engagements"],
            })

        # Manual surfaces: included only when a number or a link exists.
        for key, label, metric in (("youtube", "YouTube", "views"),
                                   ("spotify", "Podcast", "plays")):
            block = p.get(key) or {}
            if block.get(metric) is None and not block.get("url"):
                continue
            surfaces.append({
                "surface": label, "date": p.get("date", ""), "text": "",
                "link": block.get("url"), "impressions": block.get(metric),
                "engagements": None, "manual": True,
            })

        counted = [s for s in surfaces if s.get("impressions")]
        total = sum(s["impressions"] for s in counted)
        pending = [s["surface"] for s in surfaces if not s.get("impressions")]

        # A flight of clips spans weeks; showing only the registry's start date
        # makes a running placement look like a one-off.
        dates = sorted(s["date"] for s in surfaces if s.get("date"))
        surfaces.sort(key=lambda s: -(s.get("impressions") or 0))
        out.append({
            **{k: v for k, v in p.items() if k != "tweet_ids"},
            "firstSeen": dates[0] if dates else p.get("date"),
            "lastSeen": dates[-1] if dates else p.get("date"),
            "postCount": sum(1 for s in surfaces if s["surface"].startswith("Sponsored")),
            "surfaces": surfaces,
            "totalReach": total,
            "totalEngagements": sum(s.get("engagements") or 0 for s in surfaces),
            "pendingSurfaces": pending,
            "costPerThousand": (round(p["cost"] / total * 1000, 2)
                                if p.get("cost") and total else None),
        })
    out.sort(key=lambda r: r.get("date", ""), reverse=True)
    return out


def fetch_partner_posts(registry):
    """Organic Mercor posts from accounts we have a paid relationship with,
    keyed by partner. These roll up under that partner's placement, not into
    the general 3rd-party pile — they are amplification the deal produced."""
    keywords = [k.lower() for k in registry.get("partner_keywords", ["mercor"])]
    # Don't double-count a post that is already reported as the placement itself.
    claimed = {tid for pl in registry.get("placements", [])
               for tid in pl.get("tweet_ids", [])}

    by_partner = defaultdict(list)
    for acct in registry.get("partner_accounts", []):
        d = x_get(f"users/{acct['user_id']}/tweets", {
            "max_results": 100, "exclude": "replies,retweets",
            "tweet.fields": "created_at,public_metrics,text",
        })
        for t in (d or {}).get("data", []):
            text = t.get("text") or ""
            if t["id"] in claimed or not any(k in text.lower() for k in keywords):
                continue
            m = metrics_of(t)
            by_partner[acct["partner"]].append({
                "date": t.get("created_at", "")[:10], "text": text,
                "link": f"https://x.com/i/web/status/{t['id']}",
                "impressions": m["impressions"], "engagements": m["engagements"],
            })
    return by_partner


def build_earned(apex_posts):
    """3rd-party APEX chatter — anyone outside Mercor posting about APEX."""
    rows = [dict(p) for p in apex_posts if p.get("account") == "3rd Party"]
    rows.sort(key=lambda r: (r.get("date", ""), r.get("impressions", 0)), reverse=True)
    return resolve_handles(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--no-refresh", action="store_true",
                    help="build from the existing tweet cache, no API discovery")
    args = ap.parse_args()

    registry = json.load(open(PLACEMENTS_PATH))

    print("APEX social report...")
    apex = build_apex(refresh=not args.no_refresh)
    print(f"  {apex['ytdImpressions']:,} YTD impressions · {len(apex['posts'])} posts")

    print("Sponsorships...")
    partner_posts = fetch_partner_posts(registry)
    placements = build_placements(registry, partner_posts)
    for p in placements:
        pend = f" ({len(p['pendingSurfaces'])} surface(s) awaiting a number)" \
            if p["pendingSurfaces"] else ""
        print(f"  {p['partner']}: {p['totalReach']:,} reach across "
              f"{len(p['surfaces'])} surface(s){pend}")

    print("3rd-party APEX chatter...")
    earned = build_earned(apex["posts"])
    print(f"  {len(earned)} posts")

    if args.no_refresh:
        print("\n  WARNING: --no-refresh skips Sprout, so the Mercor owned-profile\n"
              "  impressions and the LinkedIn columns are missing. Flagged as partial.")

    payload = {
        "partial": bool(args.no_refresh),
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "apex": apex,
        "placements": placements,
        "earned": earned,
    }
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(payload, open(out, "w"), indent=1)
    print(f"\nwrote {out} ({out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
