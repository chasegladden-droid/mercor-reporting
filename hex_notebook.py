# ── CELL 1: Setup & Data Pull ──────────────────────────────────────────
# Paste this into your first Python cell in Hex
# Add SPROUT_API_TOKEN and SPROUT_CUSTOMER_ID as Hex secrets

import requests
import pandas as pd
from datetime import datetime, timezone

SPROUT_API_TOKEN = get_secret("SPROUT_API_TOKEN")  # set in Hex > Secrets
SPROUT_CUSTOMER_ID = "2813309"
PROFILE_IDS = [7497747, 7499374]
APEX_KEYWORDS = ["apex", "apex-agents", "apex-swe", "apex-1"]

def fetch_posts(start="2026-01-01"):
    url = f"https://api.sproutsocial.com/v1/{SPROUT_CUSTOMER_ID}/analytics/posts"
    headers = {"Authorization": f"Bearer {SPROUT_API_TOKEN}", "Content-Type": "application/json"}
    all_posts, page = [], 1
    while True:
        payload = {
            "fields": ["created_time", "text", "perma_link"],
            "metrics": ["lifetime.impressions", "lifetime.engagements"],
            "filters": [
                f"customer_profile_id.eq({', '.join(str(p) for p in PROFILE_IDS)})",
                f"created_time.in({start}T00:00:00..{datetime.now(timezone.utc).strftime('%Y-%m-%d')}T23:59:59)",
            ],
            "timezone": "America/Chicago",
            "limit": 100,
            "page": page,
        }
        posts = requests.post(url, headers=headers, json=payload).json().get("data", [])
        if not posts: break
        all_posts.extend(posts)
        if len(posts) < 100: break
        page += 1
    return all_posts

def build_df(posts):
    rows = []
    for p in posts:
        text = (p.get("text") or "").lower()
        if not any(k in text for k in APEX_KEYWORDS):
            continue
        link = p.get("perma_link", "")
        network = "X/Twitter" if "twitter.com" in link or "x.com" in link else "LinkedIn" if "linkedin.com" in link else "Other"
        dt = datetime.fromisoformat(p["created_time"].replace("Z", "+00:00"))
        rows.append({
            "date": dt.strftime("%Y-%m-%d"),
            "month": dt.strftime("%Y-%m"),
            "month_label": dt.strftime("%b %Y"),
            "network": network,
            "impressions": p.get("metrics", {}).get("lifetime.impressions", 0),
            "engagements": p.get("metrics", {}).get("lifetime.engagements", 0),
            "text": (p.get("text") or "")[:120],
            "link": link,
        })
    return pd.DataFrame(rows)

posts = fetch_posts()
df = build_df(posts)
print(f"{len(df)} APEX posts loaded")


# ── CELL 2: Monthly Impressions Table ──────────────────────────────────
# Paste this into a second Python cell

monthly = df.groupby(["month", "month_label", "network"])["impressions"].sum().reset_index()
monthly_pivot = monthly.pivot_table(index=["month", "month_label"], columns="network", values="impressions", fill_value=0).reset_index()
monthly_pivot["Total"] = monthly_pivot.drop(columns=["month", "month_label"]).sum(axis=1)
monthly_pivot = monthly_pivot.sort_values("month")
monthly_pivot  # Hex renders this as a table automatically


# ── CELL 3: Monthly Impressions Chart ──────────────────────────────────
# Paste this into a third Python cell

import plotly.express as px

chart_df = monthly.copy()
chart_df["month_label"] = pd.Categorical(chart_df["month_label"], categories=sorted(chart_df["month"].unique()), ordered=True)
chart_df = chart_df.sort_values("month")

fig = px.bar(
    chart_df,
    x="month_label",
    y="impressions",
    color="network",
    barmode="group",
    title="APEX Impressions by Month",
    labels={"month_label": "Month", "impressions": "Impressions", "network": "Platform"},
    color_discrete_map={"X/Twitter": "#1DA1F2", "LinkedIn": "#0A66C2"},
)
fig.update_layout(plot_bgcolor="white", font_size=13)
fig


# ── CELL 4: Top Posts Table ─────────────────────────────────────────────
# Paste this into a fourth Python cell

top_posts = df.sort_values("impressions", ascending=False).head(10)[
    ["date", "network", "impressions", "engagements", "text", "link"]
].reset_index(drop=True)
top_posts
