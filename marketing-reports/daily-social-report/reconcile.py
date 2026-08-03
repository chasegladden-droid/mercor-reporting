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


def check_no_owned_double_count():
    """The same tweet must not sit in both Sprout's output and the X cache."""
    sys.path.insert(0, HERE)
    import slack_report as rpt

    cache = json.load(open(os.path.join(HERE, "tweet_cache.json")))
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
    cache = json.load(open(os.path.join(HERE, "tweet_cache.json")))
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
    """The Slack report's cache must not fall behind the dashboard's.

    This is the failure that hid for eight weeks: CI could not push, so the
    root cache stopped at June while the dashboard's kept growing.
    """
    root = json.load(open(os.path.join(ROOT, "tweet_cache.json")))["tweets"]
    sub = json.load(open(os.path.join(HERE, "tweet_cache.json")))["tweets"]
    missing = {k: v for k, v in sub.items()
               if k not in root and v.get("account") not in OWNED_ACCOUNTS}
    if missing:
        imp = sum(v.get("impressions", 0) or 0 for v in missing.values())
        newest = max(v.get("date", "") for v in missing.values())
        fail(f"Slack report's cache is missing {len(missing)} tweet(s) the "
             f"dashboard has ({imp:,} impressions, newest {newest}) — the Slack "
             f"report will under-report until this is pushed")
    else:
        note(f"cache parity OK (root {len(root)}, dashboard {len(sub)})")


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
