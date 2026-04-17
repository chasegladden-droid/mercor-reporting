import os
import csv
import io
import requests
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

CLAY_API_KEY = os.getenv("CLAY_API_KEY")
CLAY_WORKSPACE_ID = "532985"
CLAY_TABLE_ID = "t_0tdfylwXHPXVGjXUbPe"  # Event: Web intent Table

# Revenue ranges considered >= $10M
HIGH_REV_PREFIXES = (
    "$10M", "$25M", "$50M", "$100M", "$250M", "$500M",
    "$1B", "$5B", "$10B",
)


def is_high_revenue(rev_str):
    """Return True if revenue is $10M or more."""
    if not rev_str:
        return False
    rev = rev_str.strip()
    return any(rev.startswith(p) for p in HIGH_REV_PREFIXES)


def fetch_export():
    headers = {"Authorization": CLAY_API_KEY}

    # Kick off export
    r = requests.post(
        f"https://api.clay.com/v3/tables/{CLAY_TABLE_ID}/export",
        headers=headers,
    )
    r.raise_for_status()
    export_id = r.json()["id"]

    # Poll until finished
    for _ in range(30):
        r = requests.get(
            f"https://api.clay.com/v3/exports/{export_id}",
            headers=headers,
        )
        data = r.json()
        if data.get("status") == "FINISHED":
            return data["downloadUrl"]
        import time; time.sleep(2)

    raise RuntimeError("Export timed out")


def load_rows(download_url):
    r = requests.get(download_url)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    return list(reader)


def build_report(rows):
    # daily[date] = {"total_apex": N, "high_rev_apex": N}
    daily = defaultdict(lambda: {"total_apex": 0, "high_rev_apex": 0})

    for row in rows:
        pages = row.get("Unique Visited Pages", "").lower()
        if "apex" not in pages:
            continue

        date = row.get("Created At", "")[:10]
        sessions = int(row.get("Total Session Count in Window") or 1)
        revenue = row.get("Company Revenue", "")

        daily[date]["total_apex"] += sessions
        if is_high_revenue(revenue):
            daily[date]["high_rev_apex"] += sessions

    return daily


def print_report(daily):
    col = 14
    header = f"{'Date':<12}  {'APEX Visits':>{col}}  {'$10M+ Visits':>{col}}  {'% $10M+':>8}"
    divider = "-" * len(header)

    print("=" * len(header))
    print("APEX WEB INTENT: $10M+ REVENUE VISITS BY DAY")
    print("=" * len(header))
    print(header)
    print(divider)

    total_apex = 0
    total_high = 0

    for date in sorted(daily.keys()):
        d = daily[date]
        apex = d["total_apex"]
        high = d["high_rev_apex"]
        pct = (high / apex * 100) if apex else 0
        total_apex += apex
        total_high += high
        print(f"{date:<12}  {apex:>{col},}  {high:>{col},}  {pct:>7.1f}%")

    print(divider)
    overall_pct = (total_high / total_apex * 100) if total_apex else 0
    print(f"{'TOTAL':<12}  {total_apex:>{col},}  {total_high:>{col},}  {overall_pct:>7.1f}%")
    print()


if __name__ == "__main__":
    print("Fetching Web intent table from Clay...")
    download_url = fetch_export()
    rows = load_rows(download_url)
    print(f"Loaded {len(rows)} rows\n")

    daily = build_report(rows)
    print_report(daily)
