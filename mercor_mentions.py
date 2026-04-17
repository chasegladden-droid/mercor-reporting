import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

MIN_FOLLOWERS = 5000
FIRST_RUN_HOUR = 9  # 9am PT — first cron run of the day
FIRST_RUN_LOOKBACK_MINUTES = 725  # 9pm previous day to 9am = 12 hours + 5 min buffer
LOOKBACK_MINUTES = 65  # all other runs


def get_lookback_minutes():
    now_pt = datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo("America/Los_Angeles"))
    if now_pt.hour == FIRST_RUN_HOUR:
        return FIRST_RUN_LOOKBACK_MINUTES
    return LOOKBACK_MINUTES


def check_mercor_mentions():
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    lookback = get_lookback_minutes()
    start_time = (datetime.now(timezone.utc) - timedelta(minutes=lookback)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "query": "mercor is:verified -from:mercor_ai -from:BrendanFoody -from:adarsh_exe -is:retweet lang:en",
        "start_time": start_time,
        "max_results": 100,
        "tweet.fields": "created_at,text,public_metrics,author_id",
        "expansions": "author_id",
        "user.fields": "name,username,public_metrics",
    }

    qualifying = []

    while True:
        resp = requests.get(
            "https://api.twitter.com/2/tweets/search/recent",
            headers=headers,
            params=params,
        ).json()

        tweets = resp.get("data", [])
        users = {u["id"]: u for u in resp.get("includes", {}).get("users", [])}

        for tweet in tweets:
            author = users.get(tweet.get("author_id"), {})
            followers = author.get("public_metrics", {}).get("followers_count", 0)
            if followers >= MIN_FOLLOWERS:
                qualifying.append({
                    "text": tweet["text"],
                    "created_at": tweet["created_at"],
                    "url": f"https://twitter.com/i/web/status/{tweet['id']}",
                    "author_name": author.get("name", "Unknown"),
                    "author_username": author.get("username", "unknown"),
                    "followers": followers,
                    "impressions": tweet.get("public_metrics", {}).get("impression_count", 0),
                })

        next_token = resp.get("meta", {}).get("next_token")
        if not next_token or not tweets:
            break
        params["next_token"] = next_token

    return qualifying


def send_slack_alert(mentions):
    if not mentions:
        print("No qualifying mentions found.")
        return

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Mercor Mention Alert — {len(mentions)} verified mention(s)"},
        },
        {"type": "divider"},
    ]

    for m in sorted(mentions, key=lambda x: x["followers"], reverse=True):
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*@{m['author_username']}* ({m['author_name']})  |  *{m['followers']:,} followers*  |  {m['impressions']:,} impressions\n"
                    f"{m['text'][:280]}\n"
                    f"<{m['url']}|View tweet>"
                ),
            },
        })
        blocks.append({"type": "divider"})

    resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
    if resp.status_code == 200:
        print(f"Sent {len(mentions)} alert(s) to Slack.")
    else:
        print(f"Slack error {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}] Starting run")
    lookback = get_lookback_minutes()
    print(f"Checking for verified Mercor mentions in the last {lookback} minutes...")
    mentions = check_mercor_mentions()
    print(f"Found {len(mentions)} mention(s) with {MIN_FOLLOWERS:,}+ followers.")
    send_slack_alert(mentions)
