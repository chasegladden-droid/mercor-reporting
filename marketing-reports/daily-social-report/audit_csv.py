#!/usr/bin/env python3
"""Dump every counted post and the bucket it lands in, with the rule that put it there.

Reuses export_dash_json's own classification so the CSV cannot drift from the
report. Read-only: no Sprout writes, no deploy, no Slack. One Sprout read.

  python3 audit_csv.py [out.csv]
"""

import csv
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import export_dash_json as ex          # noqa: E402
import slack_report as rpt             # noqa: E402

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "social_audit.csv")


def main():
    cache = rpt.load_tweet_cache()
    profile_ids, profile_map = rpt.get_sprout_profiles()

    cache_posts = rpt.cache_to_posts(cache)
    for p in cache_posts:
        m = ex.TWEET_ID_RE.search(p.get("perma_link") or "")
        if m:
            p["quoted_id"] = cache["tweets"].get(m.group(1), {}).get("quoted_id")
    sprout_posts = (rpt.get_all_posts(profile_ids, start_date="2026-01-01")
                    if profile_ids else [])

    # Same owned-dupe rule the exporter applies: Sprout wins.
    posts, seen, dropped_owned = [], set(), []
    for p in sprout_posts + cache_posts:
        m = ex.TWEET_ID_RE.search(p.get("perma_link") or "")
        if m:
            if m.group(1) in seen:
                dropped_owned.append(p)
                continue
            seen.add(m.group(1))
        posts.append(p)

    quotes, _, _ = ex.apex_quote_posts(posts, False, cache)
    bucketed, kept, _ = ex.classify_quotes(quotes, posts)

    rows = []

    def tid_of(link):
        m = ex.TWEET_ID_RE.search(link or "")
        return m.group(1) if m else ""

    for bucket, q in bucketed:
        why = ("rule 1: quote written by one of our own accounts"
               if bucket != "Quote posts"
               else "rule 2: outside account quoting one of our APEX posts")
        rows.append({
            "date": q.get("date", ""), "counted_in": bucket,
            "author": q.get("handle") or "", "impressions": q.get("impressions", 0),
            "engagements": q.get("engagements", 0), "why": why,
            "tweet_id": tid_of(q.get("link")), "link": q.get("link", ""),
            "text": (q.get("text") or "").replace("\n", " ")[:160],
        })

    for p in kept:
        account = rpt.get_account(p, profile_map)
        src = p.get("source")
        if src is None:
            why = "rule 1: our own profile, via Sprout"
        elif src in ex.OWN_POST_SOURCES:
            why = "rule 1: our own account, via X API"
        else:
            why = "rule 3: outside post matching an APEX keyword"
        rows.append({
            "date": (p.get("created_time") or "")[:10], "counted_in": account,
            "author": src or account,
            "impressions": p.get("metrics", {}).get("lifetime.impressions", 0),
            "engagements": p.get("metrics", {}).get("lifetime.engagements", 0),
            "why": why, "tweet_id": tid_of(p.get("perma_link")),
            "link": p.get("perma_link", ""),
            "text": (p.get("text") or "").replace("\n", " ")[:160],
        })

    for p in dropped_owned:
        rows.append({
            "date": (p.get("created_time") or "")[:10], "counted_in": "NOT COUNTED",
            "author": p.get("source") or "",
            "impressions": p.get("metrics", {}).get("lifetime.impressions", 0),
            "engagements": p.get("metrics", {}).get("lifetime.engagements", 0),
            "why": "duplicate: same tweet already counted from Sprout",
            "tweet_id": tid_of(p.get("perma_link")), "link": p.get("perma_link", ""),
            "text": (p.get("text") or "").replace("\n", " ")[:160],
        })

    rows.sort(key=lambda r: (r["date"], -int(r["impressions"] or 0)), reverse=True)
    cols = ["date", "counted_in", "author", "impressions", "engagements",
            "why", "tweet_id", "link", "text"]
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    counted = [r for r in rows if r["counted_in"] != "NOT COUNTED"]
    print(f"wrote {OUT}: {len(rows)} rows ({len(counted)} counted)")
    tally = {}
    for r in counted:
        tally[r["counted_in"]] = tally.get(r["counted_in"], 0) + int(r["impressions"] or 0)
    for k in sorted(tally, key=lambda x: -tally[x]):
        print(f"   {k:<26}{tally[k]:>12,}")


if __name__ == "__main__":
    main()
