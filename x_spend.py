#!/usr/bin/env python3
"""X (Twitter) Ads spend, live from the Ads API. Mirrors linkedin_spend.py.

    python3 x_spend.py <start YYYY-MM-DD> <end YYYY-MM-DD> <MONTHLY|DAILY|ALL>

Prints JSON to stdout: {"campaigns": [{campaign, spend, period_start, ...}]}
so build_paid_media.py can consume it the same way it consumes LinkedIn, and X
stops depending on a hand-exported xlsx.

Auth is OAuth 1.0a. The app-only bearer token does NOT work on the Ads API even
though the app is approved and the bearer works fine on api.x.com/2 — it returns
401 UNAUTHORIZED_ACCESS forever. Four keys are required, all in .env:
TWITTER_CONSUMER_KEY / _SECRET and TWITTER_ACCESS_TOKEN / _SECRET.

Two API constraints shape the code:
  * stats accepts at most 20 entity_ids per call
  * the time window is capped at 7 days per call for EVERY granularity, not just
    DAY (TOTAL is capped too — an easy wrong assumption)
  * timestamps must be midnight in the ACCOUNT's timezone (America/Los_Angeles),
    not UTC midnight, so the offset shifts with DST
so everything is fetched daily in 7-day chunks and aggregated locally.

A full year is ~62 stats calls, which exceeds the endpoint's rate limit in one go.
Closed days never change, so results are cached in x_spend_cache.json and only
the last CACHE_REFRESH_DAYS are re-fetched. First run backfills; later runs make
a handful of calls.
"""

import datetime
import time
from zoneinfo import ZoneInfo
import json
import os
import sys

from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, ".env"))

BASE = "https://ads-api.x.com/12"
ACCOUNT_ID = "18ce55lygd9"          # "Mercor" — NOT 18ce55lygdh, which is Brendan's
# The API rejects any timestamp that is not midnight in the ACCOUNT's timezone for
# day granularity, so the offset has to follow US Pacific DST, not a fixed -07:00.
ACCOUNT_TZ = ZoneInfo("America/Los_Angeles")
MAX_IDS = 20
MAX_DAYS_DAILY = 7
CACHE_PATH = os.path.join(HERE, "x_spend_cache.json")
CACHE_REFRESH_DAYS = 10          # re-fetch this trailing window; older days are settled


def session():
    missing = [k for k in ("TWITTER_CONSUMER_KEY", "TWITTER_CONSUMER_SECRET",
                           "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_TOKEN_SECRET")
               if not os.getenv(k)]
    if missing:
        sys.exit(f"x_spend: missing {', '.join(missing)} in .env — the Ads API needs "
                 f"OAuth 1.0a; the bearer token will not work.")
    return OAuth1Session(os.getenv("TWITTER_CONSUMER_KEY"),
                         os.getenv("TWITTER_CONSUMER_SECRET"),
                         os.getenv("TWITTER_ACCESS_TOKEN"),
                         os.getenv("TWITTER_ACCESS_TOKEN_SECRET"))


def get(s, path, **params):
    """GET with backoff. The stats endpoint rate-limits readily once a full year is
    being walked a week at a time, and a hard failure there loses the whole run."""
    for attempt in range(6):
        r = s.get(f"{BASE}{path}", params=params, timeout=60)
        if r.status_code == 429:
            wait = int(r.headers.get("x-rate-limit-reset", 0)) - int(time.time())
            wait = min(max(wait, 5), 120) if wait > 0 else min(5 * 2 ** attempt, 120)
            print(f"x_spend: rate limited, sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if not r.ok:
            sys.exit(f"x_spend: {path} -> HTTP {r.status_code}: {r.text[:300]}")
        return r.json()
    sys.exit("x_spend: still rate limited after 6 attempts")


def campaigns(s):
    """Every campaign on the account, including deleted ones — they still hold spend."""
    out, cursor = {}, None
    while True:
        d = get(s, f"/accounts/{ACCOUNT_ID}/campaigns", count=200, with_deleted="true",
                **({"cursor": cursor} if cursor else {}))
        for c in d.get("data", []):
            out[c["id"]] = c.get("name") or c["id"]
        cursor = d.get("next_cursor")
        if not cursor:
            break
    return out


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def windows(start, end, days):
    """Split [start, end] into <= `days`-long spans. days=None means one span."""
    s = datetime.date.fromisoformat(start)
    e = datetime.date.fromisoformat(end)
    if not days:
        yield s, e
        return
    while s <= e:
        stop = min(s + datetime.timedelta(days=days - 1), e)
        yield s, stop
        s = stop + datetime.timedelta(days=1)


def local_midnight(d):
    """Midnight on date `d` in the account's timezone, as a UTC instant."""
    local = datetime.datetime(d.year, d.month, d.day, tzinfo=ACCOUNT_TZ)
    return local.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_cache():
    try:
        with open(CACHE_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_cache(c):
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(c, f)
    os.replace(tmp, CACHE_PATH)      # atomic: a crash must not leave a truncated cache


def stats(s, ids, start, end):
    """Billed spend per entity, per day. Returns {campaign_id: [(YYYY-MM-DD, micros)]}.

    Always DAY granularity in 7-day windows — the API caps the window for every
    granularity, so there is nothing to gain from asking for TOTAL.
    """
    cache = load_cache()
    settled = (datetime.date.today() - datetime.timedelta(days=CACHE_REFRESH_DAYS)).isoformat()
    out = {}
    fetched = 0
    for w_start, w_end in windows(start, end, MAX_DAYS_DAILY):
        key = f"{w_start}:{w_end}"
        if w_end.isoformat() < settled and key in cache:
            for cid, pts in cache[key].items():
                out.setdefault(cid, []).extend(tuple(p) for p in pts)
            continue
        window_out = {}
        for group in chunks(ids, MAX_IDS):
            d = get(s, f"/stats/accounts/{ACCOUNT_ID}",
                    entity="CAMPAIGN", entity_ids=",".join(group),
                    granularity="DAY", metric_groups="BILLING",
                    placement="ALL_ON_TWITTER",
                    start_time=local_midnight(w_start),
                    end_time=local_midnight(w_end + datetime.timedelta(days=1)))
            for row in d.get("data", []):
                series = (row.get("id_data") or [{}])[0].get("metrics") or {}
                spend = series.get("billed_charge_local_micro") or []
                for i, micros in enumerate(spend):
                    if micros:
                        day = w_start + datetime.timedelta(days=i)
                        window_out.setdefault(row["id"], []).append((day.isoformat(), micros))
            fetched += 1
        for cid, pts in window_out.items():
            out.setdefault(cid, []).extend(pts)
        cache[key] = window_out
        save_cache(cache)            # persist per window so a rate-limit stop keeps progress
    if fetched:
        print(f"x_spend: fetched {fetched} window(s), rest from cache", file=sys.stderr)
    return out


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    start, end, gran = sys.argv[1], sys.argv[2], sys.argv[3].upper()
    s = session()
    names = campaigns(s)
    ids = list(names)
    if not ids:
        print(json.dumps({"campaigns": []}))
        return

    data = stats(s, ids, start, end)

    if gran == "ALL":
        rows = [{"campaign": names[cid],
                 "spend": round(sum(m for _, m in pts) / 1_000_000, 2),
                 "period_start": None, "landing_page_clicks": 0, "reach": 0,
                 "type": "X", "status": ""}
                for cid, pts in data.items()]
    else:
        bucket = {}
        for cid, pts in data.items():
            for day, micros in pts:
                # MONTHLY rolls days up to the first of the month; the builder keys
                # months off period_start[:7] and weeks off the exact day.
                key = f"{day[:7]}-01" if gran == "MONTHLY" else day
                bucket.setdefault((cid, key), 0)
                bucket[(cid, key)] += micros
        rows = [{"campaign": names[cid], "period_start": key,
                 "spend": round(micros / 1_000_000, 2),
                 "landing_page_clicks": 0, "reach": 0, "type": "X", "status": ""}
                for (cid, key), micros in sorted(bucket.items())]

    print(json.dumps({"campaigns": [r for r in rows if r["spend"]]}))


if __name__ == "__main__":
    main()
