import os
import json
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

SPROUT_API_TOKEN = os.getenv("SPROUT_API_TOKEN")
SPROUT_CUSTOMER_ID = os.getenv("SPROUT_CUSTOMER_ID")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

TWITTER_PERSONAL_ACCOUNTS = {
    "1237401069921001472": "Brendan Foody",
    "1029157733528829952": "Adarsh",
}

APEX_KEYWORDS = ["apex-agents", "apex-swe", "apex-agents-aa"]


def get_sprout_profiles():
    """Dynamically fetch all Sprout profiles and build profile map + ID list."""
    url = f"https://api.sproutsocial.com/v1/{SPROUT_CUSTOMER_ID}/metadata/customer"
    headers = {"Authorization": f"Bearer {SPROUT_API_TOKEN}"}
    profiles = requests.get(url, headers=headers).json().get("data", [])

    profile_ids = []
    profile_map = {}
    for p in profiles:
        pid = str(p["customer_profile_id"])
        network = p.get("network_type", "")
        name = p.get("name", "")
        native_name = p.get("native_name", "")
        profile_ids.append(int(pid))

        if network == "twitter":
            profile_map[pid] = "Mercor Twitter"
        elif network == "linkedin_company":
            profile_map[pid] = "Mercor LinkedIn"
        elif network == "linkedin":
            # Personal LinkedIn — use the person's name
            profile_map[pid] = f"{name} LinkedIn"

    return profile_ids, profile_map


def get_all_posts(profile_ids, start_date="2026-01-01", end_date=None):
    if end_date is None:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    url = f"https://api.sproutsocial.com/v1/{SPROUT_CUSTOMER_ID}/analytics/posts"
    headers = {
        "Authorization": f"Bearer {SPROUT_API_TOKEN}",
        "Content-Type": "application/json",
    }

    all_posts = []
    page = 1

    while True:
        payload = {
            "fields": ["created_time", "text", "perma_link", "customer_profile_id"],
            "metrics": ["lifetime.impressions", "lifetime.engagements"],
            "filters": [
                f"customer_profile_id.eq({', '.join(str(p) for p in profile_ids)})",
                f"created_time.in({start_date}T00:00:00..{end_date}T23:59:59)",
            ],
            "timezone": "America/Chicago",
            "limit": 100,
            "page": page,
        }

        resp = requests.post(url, headers=headers, json=payload)
        data = resp.json()

        posts = data.get("data", [])
        if not posts:
            break

        all_posts.extend(posts)

        if len(posts) < 100:
            break
        page += 1

    return all_posts


def get_personal_tweets(start_date="2026-01-01", end_date=None):
    if end_date is None:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    start_time = f"{start_date}T00:00:00Z"
    end_time = f"{end_date}T23:59:59Z"

    all_posts = []
    for user_id, name in TWITTER_PERSONAL_ACCOUNTS.items():
        params = {
            "start_time": start_time,
            "end_time": end_time,
            "max_results": 100,
            "tweet.fields": "created_at,text,public_metrics",
            "expansions": "author_id",
        }
        url = f"https://api.twitter.com/2/users/{user_id}/tweets"

        while True:
            resp = requests.get(url, headers=headers, params=params)
            data = resp.json()
            tweets = data.get("data", [])
            for t in tweets:
                metrics = t.get("public_metrics", {})
                all_posts.append({
                    "created_time": t["created_at"],
                    "text": t["text"],
                    "perma_link": f"https://twitter.com/{name}/status/{t['id']}",
                    "metrics": {
                        "lifetime.impressions": metrics.get("impression_count", 0),
                        "lifetime.engagements": (
                            metrics.get("like_count", 0) +
                            metrics.get("retweet_count", 0) +
                            metrics.get("reply_count", 0)
                        ),
                    },
                    "source": name,
                })
            next_token = data.get("meta", {}).get("next_token")
            if not next_token:
                break
            params["pagination_token"] = next_token

    return all_posts


def get_third_party_mentions(start_date="2026-01-01", max_results=500):
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    query = (
        '(apex-agents OR "apex-swe" OR "apex-agents-aa" OR (apex mercor)) '
        '-from:mercor_ai -from:BrendanFoody -from:adarsh_exe '
        '-"apex folders" -is:retweet lang:en'
    )
    params = {
        "query": query,
        "start_time": f"{start_date}T00:00:00Z",
        "max_results": 100,
        "tweet.fields": "created_at,text,public_metrics",
    }

    all_posts = []
    tweets_fetched = 0

    while tweets_fetched < max_results:
        resp = requests.get(
            "https://api.twitter.com/2/tweets/search/all",
            headers=headers,
            params=params,
        )
        data = resp.json()
        tweets = data.get("data", [])
        for t in tweets:
            metrics = t.get("public_metrics", {})
            all_posts.append({
                "created_time": t["created_at"],
                "text": t["text"],
                "perma_link": f"https://twitter.com/i/web/status/{t['id']}",
                "metrics": {
                    "lifetime.impressions": metrics.get("impression_count", 0),
                    "lifetime.engagements": (
                        metrics.get("like_count", 0) +
                        metrics.get("retweet_count", 0) +
                        metrics.get("reply_count", 0)
                    ),
                },
                "source": "3rd Party",
            })
        tweets_fetched += len(tweets)
        next_token = data.get("meta", {}).get("next_token")
        if not next_token or not tweets:
            break
        params["next_token"] = next_token

    return all_posts, tweets_fetched


def is_apex_post(post):
    text = (post.get("text") or "").lower()
    return any(kw in text for kw in APEX_KEYWORDS)


def get_account(post, profile_map):
    source = post.get("source", "")
    if source == "Brendan Foody":
        return "Brendan"
    if source == "Adarsh":
        return "Adarsh"
    if source == "3rd Party":
        return "3rd Party"
    profile_id = str(post.get("customer_profile_id", ""))
    if profile_id in profile_map:
        return profile_map[profile_id]
    link = post.get("perma_link", "")
    if "twitter.com" in link or "x.com" in link:
        return "Mercor Twitter"
    if "linkedin.com" in link:
        return "Mercor LinkedIn"
    return "Other"


TWITTER_ACCOUNTS = ["Mercor Twitter", "Brendan", "Adarsh", "3rd Party"]


def build_report(posts, profile_map):
    apex_posts = [p for p in posts if p.get("source") == "3rd Party" or is_apex_post(p)]

    daily = defaultdict(lambda: defaultdict(int))
    monthly = defaultdict(lambda: defaultdict(int))
    post_log = []

    for post in apex_posts:
        dt = datetime.fromisoformat(post["created_time"].replace("Z", "+00:00"))
        day = dt.strftime("%Y-%m-%d")
        month = dt.strftime("%Y-%m")
        account = get_account(post, profile_map)
        impressions = post.get("metrics", {}).get("lifetime.impressions", 0)
        engagements = post.get("metrics", {}).get("lifetime.engagements", 0)

        is_twitter = account in TWITTER_ACCOUNTS
        is_linkedin = "LinkedIn" in account


        daily[day]["Total Impressions"] += impressions
        daily[day]["Total Engagements"] += engagements
        daily[day][f"{account} Impressions"] += impressions
        if is_twitter:
            daily[day]["Twitter Total Impressions"] += impressions
        if is_linkedin:
            daily[day]["LinkedIn Total Impressions"] += impressions

        monthly[month]["Total Impressions"] += impressions
        monthly[month]["Total Engagements"] += engagements
        monthly[month][f"{account} Impressions"] += impressions
        if is_twitter:
            monthly[month]["Twitter Total Impressions"] += impressions
        if is_linkedin:
            monthly[month]["LinkedIn Total Impressions"] += impressions

        post_log.append({
            "date": day,
            "account": account,
            "impressions": impressions,
            "engagements": engagements,
            "link": post.get("perma_link", ""),
            "text": (post.get("text") or "")[:120],
        })

    return daily, monthly, post_log


def print_report(daily, monthly, post_log, profile_map):
    col_w = 11

    # Dynamically determine LinkedIn personal accounts from profile_map
    li_personal = sorted([v for v in profile_map.values() if "LinkedIn" in v and v != "Mercor LinkedIn"])

    def row(label, d, w=col_w):
        tw_mercor = d.get("Mercor Twitter Impressions", 0)
        tw_brendan = d.get("Brendan Impressions", 0)
        tw_adarsh  = d.get("Adarsh Impressions", 0)
        tw_third   = d.get("3rd Party Impressions", 0)
        tw_total   = d.get("Twitter Total Impressions", 0)
        li_mercor  = d.get("Mercor LinkedIn Impressions", 0)
        li_personal_vals = [d.get(f"{a} Impressions", 0) for a in li_personal]
        li_total   = d.get("LinkedIn Total Impressions", 0)
        grand      = d.get("Total Impressions", 0)
        li_str = "".join(f" {v:{w},}" for v in li_personal_vals)
        return (f"{label:<14} {tw_mercor:{w},} {tw_brendan:{w},} {tw_adarsh:{w},} {tw_third:{w},} {tw_total:{w},} "
                f"{li_mercor:{w},}{li_str} {li_total:{w},} {grand:{w},}")

    li_personal_headers = "".join(f" {'LI:' + a.split()[0]:>{col_w}}" for a in li_personal)
    header = (f"{'':14} "
              f"{'TW:Mercor':>{col_w}} {'TW:Brendan':>{col_w}} {'TW:Adarsh':>{col_w}} {'TW:3rdPty':>{col_w}} {'TW:Total':>{col_w}} "
              f"{'LI:Mercor':>{col_w}}{li_personal_headers} {'LI:Total':>{col_w}} {'Grand':>{col_w}}")
    divider = "-" * len(header)

    print("=" * len(header))
    print("APEX IMPRESSIONS BY DAY")
    print("=" * len(header))
    print(header)
    print(divider)
    for day in sorted(daily.keys()):
        print(row(day, daily[day]))

    print()
    print("=" * len(header))
    print("APEX IMPRESSIONS BY MONTH")
    print("=" * len(header))
    print(header)
    print(divider)
    for month in sorted(monthly.keys()):
        print(row(month, monthly[month]))

    print()
    print("=" * 70)
    print("TOP APEX POSTS BY IMPRESSIONS")
    print("=" * 70)
    top = sorted(post_log, key=lambda p: p["impressions"], reverse=True)[:10]
    for p in top:
        print(f"{p['date']} | {p['account']:<16} | {p['impressions']:>10,} impressions")
        print(f"  {p['text'][:100]}...")
        print(f"  {p['link']}")
        print()


if __name__ == "__main__":
    print("Fetching Sprout profiles...")
    profile_ids, profile_map = get_sprout_profiles()
    print(f"Found {len(profile_ids)} profiles: {list(profile_map.values())}")

    print("Fetching posts from Sprout...")
    posts = get_all_posts(profile_ids, start_date="2026-01-01")
    print(f"Found {len(posts)} total posts from Sprout, filtering for APEX...")

    print("Fetching personal tweets (Brendan, Adarsh)...")
    personal_tweets = get_personal_tweets(start_date="2026-01-01")
    apex_personal = [t for t in personal_tweets if is_apex_post(t)]
    print(f"Found {len(apex_personal)} APEX tweets from personal accounts")

    print("Fetching 3rd party mentions...")
    third_party, tweets_fetched = get_third_party_mentions(start_date="2026-01-01")
    cost_estimate = tweets_fetched * 0.0001
    print(f"Found {len(third_party)} 3rd party APEX mentions ({tweets_fetched} tweets fetched)")
    print(f"Estimated API cost for this run: ${cost_estimate:.4f}")

    all_posts = posts + personal_tweets + third_party
    daily, monthly, post_log = build_report(all_posts, profile_map)
    print(f"Found {len(post_log)} total APEX posts\n")
    print_report(daily, monthly, post_log, profile_map)
