#!/usr/bin/env python3
"""Exhaustively scan every one of our APEX posts for quote tweets.

The daily exporter is budget-limited: ~65 calls per run, page 1 only. That is
fine for keeping up but cannot give a complete picture, and page-1-only is how
@elonmusk's 2.6M quote hid for weeks behind 98 lower-reach quotes.

This walks EVERY one of our posts and follows every page of quotes, sleeping
until the rate-limit window resets rather than giving up. Progress is written to
the cache after each post, so an interruption loses at most one post.

Read-only against the report: it only fills cache["quote_scans"]. Run the
exporter afterwards to fold the results into the numbers.

  python3 full_quote_sweep.py
"""

import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import export_dash_json as ex          # noqa: E402
import slack_report as rpt             # noqa: E402

import requests                        # noqa: E402

RESET_PAD = 5          # seconds of slack past the reported reset
MAX_PAGES = 40         # a single post with >4000 quotes is not a real case


def quote_page(tid, token, pagination_token=None):
    """One page of quotes. Returns (payload|None, remaining|None, reset_epoch|None)."""
    params = {
        "max_results": 100,
        "tweet.fields": "created_at,public_metrics,text,author_id",
        "expansions": "author_id",
        "user.fields": "username,name",
    }
    if pagination_token:
        params["pagination_token"] = pagination_token
    r = requests.get(f"https://api.twitter.com/2/tweets/{tid}/quote_tweets",
                     headers={"Authorization": f"Bearer {token}"},
                     params=params, timeout=30)
    rem = r.headers.get("x-rate-limit-remaining")
    rst = r.headers.get("x-rate-limit-reset")
    rem = int(rem) if rem is not None and rem.isdigit() else None
    rst = int(rst) if rst is not None and rst.isdigit() else None
    if r.status_code == 429:
        return "RATE_LIMITED", rem, rst
    if not r.ok:
        print(f"    {tid} returned {r.status_code}: {r.text[:120]}", flush=True)
        return None, rem, rst
    return r.json(), rem, rst


def wait_for_reset(reset_epoch):
    delay = max(15, (reset_epoch - int(time.time()) + RESET_PAD)
                if reset_epoch else 900)
    print(f"  rate limited — sleeping {delay}s for the window to reset", flush=True)
    time.sleep(delay)


def sweep_post(tid, token):
    """Every page of quotes for one post. Returns (rows, total_seen) or None on give-up."""
    rows, seen, page_token, pages = [], 0, None, 0
    while pages < MAX_PAGES:
        payload, rem, rst = quote_page(tid, token, page_token)
        if payload == "RATE_LIMITED":
            wait_for_reset(rst)
            continue                      # retry the same page
        if payload is None:
            return None
        users = {u["id"]: u for u in (payload.get("includes") or {}).get("users", [])}
        for t in payload.get("data", []):
            seen += 1
            m = ex.metrics_of(t)
            if not m["impressions"]:
                continue                  # zero-reach quotes add nothing to totals
            u = users.get(t.get("author_id")) or {}
            rows.append({
                "surface": "Quote post", "date": (t.get("created_at") or "")[:10],
                "handle": "@" + u["username"] if u.get("username") else None,
                "authorName": u.get("name"),
                "text": (t.get("text") or "")[:160],
                "link": f"https://x.com/i/web/status/{t['id']}", **m,
            })
        page_token = (payload.get("meta") or {}).get("next_token")
        pages += 1
        if not page_token:
            break
        if rem is not None and rem <= 1:
            wait_for_reset(rst)
    return rows, seen


def main():
    token = rpt.TWITTER_BEARER_TOKEN
    if not token:
        sys.exit("no TWITTER_BEARER_TOKEN")

    cache = rpt.load_tweet_cache()
    profile_ids, _ = rpt.get_sprout_profiles()
    cache_posts = rpt.cache_to_posts(cache)
    sprout_posts = (rpt.get_all_posts(profile_ids, start_date="2026-01-01")
                    if profile_ids else [])

    own = {}
    for p in sprout_posts + cache_posts:
        link = p.get("perma_link") or ""
        if "x.com" not in link and "twitter.com" not in link:
            continue
        src = p.get("source")
        if src is not None and src not in ex.OWN_POST_SOURCES:
            continue
        m = ex.TWEET_ID_RE.search(link)
        if m:
            own[m.group(1)] = (p.get("created_time") or "")[:10]

    scans = cache.setdefault("quote_scans", {})
    order = sorted(own, key=lambda t: own[t], reverse=True)
    print(f"sweeping {len(order)} of our posts, every page of quotes each\n", flush=True)

    done = failed = total_quotes = total_seen = 0
    for i, tid in enumerate(order, 1):
        res = sweep_post(tid, token)
        if res is None:
            failed += 1
            print(f"  [{i}/{len(order)}] {tid} FAILED (kept previous)", flush=True)
            continue
        rows, seen = res
        scans[tid] = {"scanned_at": f"sweep:{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
                      "quotes": rows}
        rpt.save_tweet_cache(cache)       # persist per post
        done += 1
        total_quotes += len(rows)
        total_seen += seen
        if rows or i % 25 == 0:
            print(f"  [{i}/{len(order)}] {own[tid]}  {len(rows)} quote(s) with reach "
                  f"of {seen} seen", flush=True)

    print(f"\nswept {done}/{len(order)} posts ({failed} failed)")
    print(f"quotes seen: {total_seen}   with non-zero reach: {total_quotes}")
    reach = sum(q["impressions"] for s in scans.values() for q in s.get("quotes", []))
    print(f"total quote reach now in cache: {reach:,}")


if __name__ == "__main__":
    main()
