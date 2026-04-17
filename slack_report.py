import csv
import io
import os
import requests
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

SPROUT_API_TOKEN = os.getenv("SPROUT_API_TOKEN")
SPROUT_CUSTOMER_ID = os.getenv("SPROUT_CUSTOMER_ID")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
CLAY_API_KEY = os.getenv("CLAY_API_KEY")

CLAY_WORKSPACE_ID = "532985"
CLAY_WEB_INTENT_TABLE_ID = "t_0tdfylwXHPXVGjXUbPe"

HIGH_REV_PREFIXES = (
    "$10M", "$25M", "$50M", "$100M", "$250M", "$500M",
    "$1B", "$5B", "$10B",
)

TWITTER_PERSONAL_ACCOUNTS = {
    "1237401069921001472": "Brendan Foody",
    "1029157733528829952": "Adarsh",
}

APEX_KEYWORDS = ["apex-agents", "apex-swe", "apex-agents-aa"]


def get_sprout_profiles():
    url = f"https://api.sproutsocial.com/v1/{SPROUT_CUSTOMER_ID}/metadata/customer"
    headers = {"Authorization": f"Bearer {SPROUT_API_TOKEN}"}
    profiles = requests.get(url, headers=headers).json().get("data", [])

    profile_ids = []
    profile_map = {}
    for p in profiles:
        pid = str(p["customer_profile_id"])
        network = p.get("network_type", "")
        name = p.get("name", "")
        profile_ids.append(int(pid))

        if network == "twitter":
            profile_map[pid] = "Mercor Twitter"
        elif network == "linkedin_company":
            profile_map[pid] = "Mercor LinkedIn"
        elif network == "linkedin":
            profile_map[pid] = f"{name} LinkedIn"

    return profile_ids, profile_map


def get_all_posts(profile_ids, start_date="2026-01-01", end_date=None):
    if end_date is None:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    url = f"https://api.sproutsocial.com/v1/{SPROUT_CUSTOMER_ID}/analytics/posts"
    headers = {"Authorization": f"Bearer {SPROUT_API_TOKEN}", "Content-Type": "application/json"}
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
            "timezone": "America/Los_Angeles",
            "limit": 100,
            "page": page,
        }
        posts = requests.post(url, headers=headers, json=payload).json().get("data", [])
        if not posts:
            break
        all_posts.extend(posts)
        if len(posts) < 100:
            break
        page += 1

    return all_posts


def get_personal_tweets(start_date="2026-01-01"):
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    all_posts = []

    for user_id, name in TWITTER_PERSONAL_ACCOUNTS.items():
        params = {
            "start_time": f"{start_date}T00:00:00Z",
            "max_results": 100,
            "tweet.fields": "created_at,text,public_metrics",
        }
        url = f"https://api.twitter.com/2/users/{user_id}/tweets"
        while True:
            data = requests.get(url, headers=headers, params=params).json()
            for t in data.get("data", []):
                m = t.get("public_metrics", {})
                all_posts.append({
                    "created_time": t["created_at"],
                    "text": t["text"],
                    "perma_link": f"https://twitter.com/{name}/status/{t['id']}",
                    "metrics": {
                        "lifetime.impressions": m.get("impression_count", 0),
                        "lifetime.engagements": m.get("like_count", 0) + m.get("retweet_count", 0) + m.get("reply_count", 0),
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
    fetched = 0

    while fetched < max_results:
        data = requests.get("https://api.twitter.com/2/tweets/search/all", headers=headers, params=params).json()
        tweets = data.get("data", [])
        for t in tweets:
            m = t.get("public_metrics", {})
            all_posts.append({
                "created_time": t["created_at"],
                "text": t["text"],
                "perma_link": f"https://twitter.com/i/web/status/{t['id']}",
                "metrics": {
                    "lifetime.impressions": m.get("impression_count", 0),
                    "lifetime.engagements": m.get("like_count", 0) + m.get("retweet_count", 0) + m.get("reply_count", 0),
                },
                "source": "3rd Party",
            })
        fetched += len(tweets)
        next_token = data.get("meta", {}).get("next_token")
        if not next_token or not tweets:
            break
        params["next_token"] = next_token

    return all_posts


def is_apex_post(post):
    return any(kw in (post.get("text") or "").lower() for kw in APEX_KEYWORDS)


def get_account(post, profile_map):
    source = post.get("source", "")
    if source == "Brendan Foody": return "Brendan"
    if source == "Adarsh": return "Adarsh"
    if source == "3rd Party": return "3rd Party"
    profile_id = str(post.get("customer_profile_id", ""))
    if profile_id in profile_map: return profile_map[profile_id]
    link = post.get("perma_link", "")
    if "twitter.com" in link or "x.com" in link: return "Mercor Twitter"
    if "linkedin.com" in link: return "Mercor LinkedIn"
    return "Other"


def build_report(posts, profile_map):
    apex_posts = [p for p in posts if p.get("source") == "3rd Party" or is_apex_post(p)]
    monthly = defaultdict(lambda: defaultdict(int))
    post_log = []

    for post in apex_posts:
        dt = datetime.fromisoformat(post["created_time"].replace("Z", "+00:00"))
        month = dt.strftime("%Y-%m")
        account = get_account(post, profile_map)
        impressions = post.get("metrics", {}).get("lifetime.impressions", 0)
        engagements = post.get("metrics", {}).get("lifetime.engagements", 0)

        monthly[month]["Total Impressions"] += impressions
        monthly[month]["Total Engagements"] += engagements
        monthly[month][f"{account} Impressions"] += impressions
        if account in ["Mercor Twitter", "Brendan", "Adarsh", "3rd Party"]:
            monthly[month]["Twitter Total Impressions"] += impressions
        if "LinkedIn" in account:
            monthly[month]["LinkedIn Total Impressions"] += impressions

        post_log.append({
            "date": dt.strftime("%Y-%m-%d"),
            "account": account,
            "impressions": impressions,
            "engagements": engagements,
            "link": post.get("perma_link", ""),
            "text": (post.get("text") or "")[:120],
        })

    return monthly, post_log


def fetch_clay_web_intent():
    """Fetch APEX web intent stats from Clay: visits to APEX pages by $10M+ companies."""
    headers = {"Authorization": CLAY_API_KEY}

    r = requests.post(
        f"https://api.clay.com/v3/tables/{CLAY_WEB_INTENT_TABLE_ID}/export",
        headers=headers,
    )
    r.raise_for_status()
    export_id = r.json()["id"]

    import time
    for _ in range(30):
        r = requests.get(f"https://api.clay.com/v3/exports/{export_id}", headers=headers)
        data = r.json()
        if data.get("status") == "FINISHED":
            break
        time.sleep(2)

    rows = list(csv.DictReader(io.StringIO(requests.get(data["downloadUrl"]).text)))

    daily = defaultdict(lambda: {"total": 0, "high_rev": 0})
    for row in rows:
        if "apex" not in row.get("Unique Visited Pages", "").lower():
            continue
        date = row.get("Created At", "")[:10]
        sessions = int(row.get("Total Session Count in Window") or 1)
        rev = row.get("Company Revenue", "").strip()
        daily[date]["total"] += sessions
        if any(rev.startswith(p) for p in HIGH_REV_PREFIXES):
            daily[date]["high_rev"] += sessions

    return daily


def format_clay_section(daily):
    if not daily:
        return None

    lines = []
    total_all = total_high = 0
    for date in sorted(daily.keys()):
        d = daily[date]
        pct = (d["high_rev"] / d["total"] * 100) if d["total"] else 0
        lines.append(f"{date}  {d['total']:>5,} visits  {d['high_rev']:>5,} from $10M+  ({pct:.0f}%)")
        total_all += d["total"]
        total_high += d["high_rev"]

    overall_pct = (total_high / total_all * 100) if total_all else 0
    lines.append(f"{'─' * 50}")
    lines.append(f"Total     {total_all:>5,} visits  {total_high:>5,} from $10M+  ({overall_pct:.0f}%)")

    return "\n".join(lines)


def format_slack_message(monthly, post_log, profile_map, clay_daily=None):
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")

    ytd_impressions = sum(v["Total Impressions"] for v in monthly.values())
    ytd_engagements = sum(v["Total Engagements"] for v in monthly.values())

    li_personal = sorted([v for v in profile_map.values() if "LinkedIn" in v and v != "Mercor LinkedIn"])

    header = (
        f"{'Month':<10} {'TW:Mercor':>9} {'TW:Bren':>8} {'TW:Adar':>8} {'TW:3Pty':>8} {'TW:Tot':>8} "
        f"{'LI:Mercor':>9}"
        + "".join(f" {'LI:' + a.split()[0]:>8}" for a in li_personal)
        + f" {'LI:Tot':>8} {'Total':>9}"
    )
    divider = "-" * len(header)
    rows = []
    for month in sorted(monthly.keys()):
        label = datetime.strptime(month, "%Y-%m").strftime("%b %Y")
        m = monthly[month]
        marker = " ◀" if month == current_month else ""
        li_vals = "".join(f" {m.get(f'{a} Impressions', 0):>8,}" for a in li_personal)
        rows.append(
            f"{label:<10} "
            f"{m.get('Mercor Twitter Impressions', 0):>9,} "
            f"{m.get('Brendan Impressions', 0):>8,} "
            f"{m.get('Adarsh Impressions', 0):>8,} "
            f"{m.get('3rd Party Impressions', 0):>8,} "
            f"{m.get('Twitter Total Impressions', 0):>8,} "
            f"{m.get('Mercor LinkedIn Impressions', 0):>9,}"
            f"{li_vals} "
            f"{m.get('LinkedIn Total Impressions', 0):>8,} "
            f"{m.get('Total Impressions', 0):>9,}{marker}"
        )
    table = "\n".join([header, divider] + rows)

    top = sorted(post_log, key=lambda p: p["impressions"], reverse=True)[:3]
    top_text = "\n".join(
        f">*{i+1}.* {p['date']} · {p['account']} · *{p['impressions']:,} impressions*\n"
        f">{p['text'][:90]}...\n"
        f"><{p['link']}|View post>"
        for i, p in enumerate(top)
    )

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"APEX Social Report — {today}"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*YTD Impressions: {ytd_impressions:,}*  |  YTD Engagements: {ytd_engagements:,}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Impressions by Month*\n```{table}```"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Top APEX Posts (All-Time)*\n{top_text}"}},
    ]

    if clay_daily:
        clay_text = format_clay_section(clay_daily)
        if clay_text:
            blocks += [
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*APEX Web Intent — Visits from $10M+ Companies*\n```{clay_text}```"}},
            ]

    return {"blocks": blocks}


def send_to_slack(message):
    resp = requests.post(SLACK_WEBHOOK_URL, json=message)
    if resp.status_code == 200:
        print("Report sent to Slack successfully.")
    else:
        print(f"Slack error {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    print("Fetching Sprout profiles...")
    profile_ids, profile_map = get_sprout_profiles()

    print("Fetching Sprout posts...")
    posts = get_all_posts(profile_ids, start_date="2026-01-01")

    print("Fetching personal tweets...")
    personal = get_personal_tweets(start_date="2026-01-01")

    print("Fetching 3rd party mentions...")
    third_party = get_third_party_mentions(start_date="2026-01-01")

    print("Fetching Clay web intent data...")
    clay_daily = fetch_clay_web_intent()

    all_posts = posts + personal + third_party
    monthly, post_log = build_report(all_posts, profile_map)
    message = format_slack_message(monthly, post_log, profile_map, clay_daily)
    send_to_slack(message)
