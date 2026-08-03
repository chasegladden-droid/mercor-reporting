#!/usr/bin/env python3
"""Scan 2nd-degree amplification: quotes of third-party posts that quote us.

@elonmusk quoted our APEX-SWE post and did 2.6M. People then quoted HIM. That
downstream reach is real APEX conversation, but it is not our post being seen and
it is not someone amplifying our post directly — it is one step further out. So
it gets its own store and its own column, never folded into Quote posts, where it
would read as our own reach.

Sources are third-party cached posts that carry a quoted_id. Retweets of those
posts are deliberately not chased: X credits a retweet's reach to the original,
so they log ~0 and would add nothing but calls.

Writes cache["second_degree_scans"] only. Run AFTER full_quote_sweep.py — both
write the same cache file.

  python3 second_degree_sweep.py
"""

import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import export_dash_json as ex          # noqa: E402
import slack_report as rpt             # noqa: E402
from full_quote_sweep import sweep_post  # noqa: E402


def main():
    token = rpt.TWITTER_BEARER_TOKEN
    if not token:
        sys.exit("no TWITTER_BEARER_TOKEN")

    cache = rpt.load_tweet_cache()
    tw = cache.get("tweets", {})
    store = cache.setdefault("second_degree_scans", {})

    # Third-party posts that quote something. Their own quotes are 2nd degree.
    sources = [tid for tid, t in tw.items()
               if t.get("quoted_id") and t.get("account") not in ex.OWN_POST_SOURCES]
    sources.sort(key=lambda t: -(tw[t].get("impressions") or 0))
    print(f"scanning {len(sources)} third-party quote posts for their own quotes\n",
          flush=True)

    total = 0
    for i, tid in enumerate(sources, 1):
        res = sweep_post(tid, token)
        if res is None:
            print(f"  [{i}/{len(sources)}] {tid} FAILED (kept previous)", flush=True)
            continue
        rows, seen = res
        store[tid] = {
            "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "parent_impressions": tw[tid].get("impressions", 0),
            "parent_text": (tw[tid].get("text") or "")[:120],
            "quotes": rows,
        }
        rpt.save_tweet_cache(cache)
        reach = sum(q["impressions"] for q in rows)
        total += reach
        if rows:
            print(f"  [{i}/{len(sources)}] {reach:>9,} from {len(rows)} quote(s) of: "
                  f"{(tw[tid].get('text') or '')[:44]}", flush=True)

    print(f"\n2nd-degree reach found: {total:,} across {len(sources)} source post(s)")


if __name__ == "__main__":
    main()
