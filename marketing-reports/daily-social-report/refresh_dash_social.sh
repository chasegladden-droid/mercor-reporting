#!/bin/bash
# Refresh the marketing dashboard's Social & Sponsorships tab and redeploy.
#
# The APEX social Slack report is untouched — it still runs on GitHub Actions.
# This regenerates ~/marketing-dash/data/social.json from the same code and
# pushes a new Vercel build, because the dashboard is a local-only Vercel
# project with no git remote, so there is nothing for CI to commit to.
#
# Cron (local time):
#   15 9 * * 1-5  /Users/chasegladden/mercor-reporting/marketing-reports/daily-social-report/refresh_dash_social.sh
#
# Caveat: this needs the Mac awake. If the dashboard data goes stale, that is
# why — the durable fix is a git remote on ~/marketing-dash plus a Vercel deploy
# hook so the existing GitHub Action can do it.
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$HERE/refresh_dash_social.log"

{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
  python3 "$HERE/export_dash_json.py"

  # Gate the deploy on reconciliation. Every past failure here was silent — a
  # frozen cache, a double count, a post in two buckets — and shipped for weeks
  # looking plausible. Publishing stale-but-correct numbers beats publishing
  # fresh wrong ones, so a failure stops the deploy and leaves a reason in the log.
  if ! python3 "$HERE/reconcile.py"; then
    echo "RECONCILIATION FAILED — deploy skipped, dashboard left as-is."
    echo "Fix the problems above, or accept an intentional correction with:"
    echo "  python3 $HERE/reconcile.py --rebaseline"
    exit 1
  fi

  cd "$HOME/marketing-dash"
  vercel --prod --yes
  echo "done"
} >> "$LOG" 2>&1
