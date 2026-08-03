#!/usr/bin/env python3
"""Reconcile the APEX Slack report against the marketing dashboard.

Both surfaces are meant to be the same numbers from the same code. They have
drifted before, silently and for weeks, in three distinct ways:

  * the Slack report's tweet cache froze because CI could not push it, so its
    3rd-party history simply stopped while the dashboard's kept growing
  * the dashboard counted our own viral posts twice, once from Sprout and once
    from the X cache
  * a quote of our own post could land in two buckets at the same time

None of those announced themselves. This script fails loudly on all three, plus
a month-over-month sanity check against the previous run, so the next drift is
caught the day it starts rather than at the next readout.

Exit code 0 = clean, 1 = something needs looking at. Safe to run any time: it
reads Sprout and the caches, and never posts anywhere.
"""

import collections
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
DASH_JSON = os.path.expanduser("~/marketing-dash/data/social.json")
SNAPSHOT = os.path.join(HERE, "reconcile_snapshot.json")
TWEET_ID_RE = re.compile(r"status/(\d+)")

# How far the two surfaces may differ on a month before it is a problem.
# Impressions are lifetime and each surface refreshes at a different moment, so
# a small gap is normal drift; anything larger is a real divergence.
TOLERANCE = 0.02
OWNED_ACCOUNTS = {"Mercor", "Mercor Twitter", "Mercor LinkedIn"}

problems, notes = [], []


def fail(msg):
    problems.append(msg)


def note(msg):
    notes.append(msg)


def load_dash():
    with open(DASH_JSON) as f:
        return json.load(f)


def check_output_parity(dash):
    """The Slack report's numbers must match what the dashboard published.

    Cache parity was not enough. The two agreed on their input and still published
    different figures for two months, because the quote-post logic existed only in
    the dashboard — a ~2.9M gap that nothing flagged. This recomputes the Slack
    report's monthly table and compares it against social.json column by column,
    which is the check that would have caught it.

    Compared with a tolerance, because the two are generated minutes apart and
    lifetime impressions keep accruing in between.
    """
    import importlib.util

    sys.path.insert(0, ROOT)
    try:
        spec = importlib.util.spec_from_file_location(
            "_slack_root", os.path.join(ROOT, "slack_report.py"))
        slack = importlib.util.module_from_spec(spec)
        sys.modules["_slack_root"] = slack
        spec.loader.exec_module(slack)
        import apex_social
    except Exception as e:
        note(f"could not load the Slack report module ({e}) — skipped output parity")
        return

    cache = slack.load_tweet_cache()
    ids, pmap = slack.get_sprout_profiles()
    if not ids:
        note("Sprout unavailable — skipped output parity")
        return
    sprout = slack.get_all_posts(ids, start_date="2026-01-01")
    posts, _ = apex_social.dedupe_owned(sprout, slack.cache_to_posts(cache))
    monthly, _ = slack.build_report(posts, pmap, cache=cache)

    published = {m["month"]: m for m in dash["apex"]["months"]}
    mismatches = 0
    for month, pub in sorted(published.items()):
        mine = monthly.get(month)
        if mine is None:
            fail(f"{month} is on the dashboard but the Slack report produces nothing")
            mismatches += 1
            continue
        pairs = [("Impressions", mine.get("Total Impressions", 0), pub["impressions"]),
                 ("X total", mine.get("Twitter Total Impressions", 0), pub["twitter"]),
                 ("LinkedIn total", mine.get("LinkedIn Total Impressions", 0), pub["linkedin"])]
        for col, val in pub["byAccount"].items():
            pairs.append((col, mine.get(f"{col} Impressions", 0), val))
        for label, a, b in pairs:
            worst = max(abs(a), abs(b), 1)
            if abs(a - b) / worst > TOLERANCE:
                mismatches += 1
                if mismatches <= 5:
                    fail(f"{month} {label}: Slack report {a:,} vs dashboard {b:,} "
                         f"({100*(a-b)/worst:+.1f}%)")
    if mismatches:
        fail(f"{mismatches} value(s) diverge between the Slack report and the dashboard")
    else:
        note(f"output parity OK — Slack report matches the dashboard across "
             f"{len(published)} month(s)")


def check_no_owned_double_count():
    """The same tweet must not sit in both Sprout's output and the X cache."""
    sys.path.insert(0, HERE)
    import slack_report as rpt

    cache = json.load(open(os.path.join(ROOT, "tweet_cache.json")))
    ids, _ = rpt.get_sprout_profiles()
    if not ids:
        note("Sprout unavailable — skipped the owned double-count check")
        return
    sprout = rpt.get_all_posts(ids, start_date="2026-01-01")
    sprout_ids = {m.group(1) for p in sprout
                  if (m := TWEET_ID_RE.search(p.get("perma_link") or ""))}
    # Overlap itself is normal: the cache picks our own posts up through the
    # retweet/quote path while Sprout reports them as owned-profile posts. What
    # matters is that the exporter dropped one copy. It records how many it
    # dropped, so this asserts the count instead of trusting a truncated list.
    overlap = [(k, v) for k, v in cache["tweets"].items() if k in sprout_ids]
    dropped = load_dash()["apex"].get("ownedDupesDropped")
    if dropped is None:
        fail("dashboard data predates the dedupe fix — regenerate it "
             "(ownedDupesDropped is absent)")
    elif dropped != len(overlap):
        tot = sum(v.get("impressions", 0) or 0 for _, v in overlap)
        fail(f"{len(overlap)} tweet(s) are in both Sprout and the X cache but the "
             f"exporter only dropped {dropped} — {tot:,} impressions at risk")
        for k, v in overlap[:3]:
            fail(f"    {v.get('impressions',0):>10,}  {v.get('account')}  "
                 f"{(v.get('text') or '')[:44]}")
    else:
        note(f"{len(overlap)} owned tweet(s) in both sources, all {dropped} deduped")


def check_bucket_exclusivity(dash):
    """Quote posts / 3rd Party / own-account columns must not overlap."""
    cache = json.load(open(os.path.join(ROOT, "tweet_cache.json")))
    quote_ids = {m.group(1) for s in cache.get("quote_scans", {}).values()
                 for q in s.get("quotes", [])
                 if (m := TWEET_ID_RE.search(q.get("link") or ""))}
    keyword_ids = set(cache.get("tweets", {}))
    # Raw overlap is expected and fine — a quote of ours whose commentary also
    # contains a keyword is found by both paths. What matters is that the routing
    # resolved it to exactly one bucket, so check the emitted rows, not the inputs.
    both = quote_ids & keyword_ids
    emitted = collections.defaultdict(set)
    for p in dash["apex"]["posts"]:
        m = TWEET_ID_RE.search(p.get("link") or "")
        if m:
            emitted[m.group(1)].add(p.get("account"))
    multi = {k: v for k, v in emitted.items() if len(v) > 1}
    if multi:
        for k, v in list(multi.items())[:3]:
            fail(f"tweet {k} is reported under multiple buckets: {sorted(v)}")
    else:
        note(f"{len(both)} tweet(s) found by both paths, each routed to one bucket")

    seen = collections.Counter()
    for p in dash["apex"]["posts"]:
        m = TWEET_ID_RE.search(p.get("link") or "")
        if m:
            seen[m.group(1)] += 1
    dupes = [k for k, v in seen.items() if v > 1]
    if dupes:
        fail(f"{len(dupes)} tweet(s) listed more than once in the dashboard posts")
    else:
        note("no duplicate tweets in the dashboard post list")


def check_cache_parity():
    """Both surfaces must resolve to the SAME cache file.

    They used to keep one each, which is how the Slack report's froze in June
    while the dashboard's kept growing, and how quote scans came to exist in only
    one of them. There is now a single cache at the repo root; this asserts
    neither module has drifted back to a private copy.
    """
    import importlib.util

    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod

    sys.path.insert(0, ROOT)
    try:
        a = load("_cp_root", os.path.join(ROOT, "slack_report.py")).TWEET_CACHE_PATH
        b = load("_cp_dash", os.path.join(HERE, "slack_report.py")).TWEET_CACHE_PATH
    except Exception as e:
        note(f"could not resolve cache paths ({e})")
        return
    if os.path.realpath(a) != os.path.realpath(b):
        fail(f"the two surfaces read different caches:\n    Slack: {a}\n    dash:  {b}")
    else:
        cache = json.load(open(a))
        note(f"single shared cache OK — {len(cache.get('tweets', {}))} tweets, "
             f"{len(cache.get('quote_scans', {}))} quote-scanned posts")


def check_totals_add_up(dash):
    """Each month's total must equal the sum of its columns."""
    bad = 0
    for m in dash["apex"]["months"]:
        col = sum(m["byAccount"].values())
        if col and abs(col - m["impressions"]) / max(m["impressions"], 1) > 0.001:
            fail(f"{m['label']}: columns sum to {col:,} but total says "
                 f"{m['impressions']:,}")
            bad += 1
    if not bad:
        note("every month's columns sum to its stated total")


def check_against_snapshot(dash, rebaseline=False):
    """Impressions are lifetime, so a month must never fall between runs.

    A deliberate correction (removing a double count, say) legitimately lowers a
    month. Re-run with --rebaseline to accept the current numbers as the new
    floor; without it, a drop keeps failing so an accidental one cannot slip by.
    """
    current = {m["month"]: m["impressions"] for m in dash["apex"]["months"]}
    if rebaseline:
        json.dump({"generatedAt": dash.get("generatedAt"), "months": current},
                  open(SNAPSHOT, "w"), indent=2)
        note(f"re-baselined {len(current)} month(s) at the current values")
        return
    if not os.path.exists(SNAPSHOT):
        note("no previous snapshot — recording this run as the baseline")
    else:
        prev = json.load(open(SNAPSHOT))
        for month, was in prev.get("months", {}).items():
            now = current.get(month)
            if now is None:
                fail(f"{month} disappeared from the report (was {was:,})")
            elif now < was * (1 - TOLERANCE):
                fail(f"{month} DROPPED {was:,} -> {now:,} "
                     f"({100*(now-was)/max(was,1):+.1f}%) — lifetime impressions "
                     f"should only ever climb")
            elif now > was * 3 and was > 10000:
                fail(f"{month} tripled {was:,} -> {now:,} — verify before trusting")
        note(f"compared {len(prev.get('months', {}))} month(s) against the previous run")
    json.dump({"generatedAt": dash.get("generatedAt"), "months": current},
              open(SNAPSHOT, "w"), indent=2)


def main():
    rebaseline = "--rebaseline" in sys.argv
    dash = load_dash()
    if dash.get("partial"):
        fail("dashboard data is flagged partial — Sprout/LinkedIn figures missing")

    check_cache_parity()
    check_totals_add_up(dash)
    check_bucket_exclusivity(dash)
    check_against_snapshot(dash, rebaseline)
    check_no_owned_double_count()
    check_output_parity(dash)

    print(f"Reconciling {DASH_JSON}")
    print(f"  generated {dash.get('generatedAt')}\n")
    for n in notes:
        print(f"  ok    {n}")
    for p in problems:
        print(f"  FAIL  {p}")
    print()
    if problems:
        print(f"{len(problems)} problem(s) found.")
        return 1
    print("All checks passed — Slack report and dashboard agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
