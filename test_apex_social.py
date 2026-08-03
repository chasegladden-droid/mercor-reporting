#!/usr/bin/env python3
"""Guard the APEX counting rules. No network, no credentials, runs in ~50ms.

Every assertion here is a bug that actually shipped and went unnoticed for weeks.
CI runs this before the daily report, so a rule change that breaks one of them
fails the build instead of quietly publishing wrong numbers.

  python3 test_apex_social.py
"""

import sys

import apex_social as a

failures = []


def check(label, got, want):
    if got != want:
        failures.append(f"{label}: got {got!r}, want {want!r}")


def sprout(tid, imp, text="APEX-SWE leaderboard"):
    return {"created_time": "2026-07-11T00:00:00Z", "text": text,
            "perma_link": f"https://x.com/mercor_ai/status/{tid}",
            "metrics": {"lifetime.impressions": imp, "lifetime.engagements": 0}}


def cached(tid, imp, source="3rd Party", text="apex-swe", quoted=None):
    return {"created_time": "2026-07-11T00:00:00Z", "text": text,
            "perma_link": f"https://x.com/someone/status/{tid}", "source": source,
            "quoted_id": quoted,
            "metrics": {"lifetime.impressions": imp, "lifetime.engagements": 0}}


# --- keyword matching -------------------------------------------------------
# apex-accounting was missing, so the whole APEX-Accounting launch was dropped.
check("apex-accounting matches", a.matches_apex("We launched APEX-Accounting with Ramp"), True)
# The search query asks for (apex mercor) but the filter used to discard it.
check("apex+mercor matches", a.matches_apex("Mercor APEX tops out at ~24%"), True)
check("space-separated matches", a.matches_apex("scored well on apex agents"), True)
# Must not sweep in unrelated uses of the word.
check("apex legends rejected", a.matches_apex("apex legends is a fun game"), False)
check("bare apex rejected", a.matches_apex("she is the apex of her field"), False)
check("empty rejected", a.matches_apex(""), False)

# --- owned-post dedupe ------------------------------------------------------
# The @mercor_ai Grok post existed in both Sprout and the X cache and was counted
# twice, overstating July by 1.85M.
posts, dropped = a.dedupe_owned([sprout("111", 1_846_754)],
                                [cached("111", 1_846_822, source="Mercor")])
check("dedupe keeps one copy", len(posts), 1)
check("dedupe drops the other", len(dropped), 1)
check("Sprout's copy is the one kept",
      posts[0]["metrics"]["lifetime.impressions"], 1_846_754)

# A genuinely different tweet must survive.
posts, dropped = a.dedupe_owned([sprout("111", 10)], [cached("222", 20)])
check("distinct tweets both kept", len(posts), 2)
check("nothing dropped", len(dropped), 0)

# --- bucket resolution ------------------------------------------------------
ours = sprout("500", 1000)

# An outside quote of our post is a Quote post, even with no keyword in its text.
# This is @elonmusk: "Grok places second after Fable…" — no APEX anywhere.
quote = {"date": "2026-07-11", "handle": "@elonmusk", "link":
         "https://x.com/i/web/status/600", "impressions": 2_632_568, "engagements": 0,
         "text": "Grok places second after Fable on real-world software engineering"}
bucketed, kept, _ = a.classify_quotes([quote], [ours])
check("outside quote -> Quote posts", [b for b, _ in bucketed], ["Quote posts"])

# A quote written by us is our own reach, not third-party amplification.
mine = dict(quote, handle="@BrendanFoody")
bucketed, _, _ = a.classify_quotes([mine], [ours])
check("our own quote -> our column", [b for b, _ in bucketed], ["Brendan"])

# quoted_id is the second route in, for when the quote_tweets endpoint paginates
# past the biggest quote. Elon sat in 3rd Party for weeks because of that.
elon_cached = cached("600", 2_632_568, text="Grok places second", quoted="500")
bucketed, kept, reclassified = a.classify_quotes([], [ours, elon_cached])
check("quoted_id reclassifies to Quote posts", [b for b, _ in bucketed], ["Quote posts"])
check("reclassified count", reclassified, 1)
check("reclassified row leaves the keyword bucket", len(kept), 1)

# Found by both paths -> counted once, as a quote.
bucketed, kept, _ = a.classify_quotes([quote], [ours, elon_cached])
check("no double count across paths", len(bucketed), 1)
check("keyword copy dropped", [a.tweet_id(p["perma_link"]) for p in kept], ["500"])

# A quote of a THIRD PARTY's post is not ours — it must not become a Quote post.
other = cached("700", 50, quoted="999")            # 999 is not one of our posts
bucketed, kept, _ = a.classify_quotes([], [ours, other])
check("quote of a third party stays out", [b for b, _ in bucketed], [])
check("it remains in the post list", len(kept), 2)

# --- totals validation ------------------------------------------------------
good = {"2026-07": {"Mercor Twitter Impressions": 100, "Quote posts Impressions": 50,
                    "Mercor LinkedIn Impressions": 25,
                    "Twitter Total Impressions": 150,
                    "LinkedIn Total Impressions": 25,
                    "Total Impressions": 175}}
check("valid month passes", a.validate_monthly(good), [])

bad = {"2026-07": dict(good["2026-07"], **{"Total Impressions": 999})}
check("wrong total is caught", len(a.validate_monthly(bad)) > 0, True)

neg = {"2026-07": dict(good["2026-07"], **{"Mercor Twitter Impressions": -5,
                                           "Twitter Total Impressions": 45,
                                           "Total Impressions": 70})}
check("negative impressions caught",
      any("negative" in p for p in a.validate_monthly(neg)), True)

# 2nd degree must never be inside the X total.
check("2nd degree is not an X bucket", "2nd degree" in a.X_BUCKETS, False)


if failures:
    print(f"FAILED ({len(failures)}):")
    for f in failures:
        print(f"  {f}")
    sys.exit(1)
print("all APEX counting-rule tests passed")
