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

REPO="$(cd "$HERE/../.." && pwd)"

{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

  # The tweet cache is shared with the Slack report, which runs in CI and commits
  # it. Only this local run discovers quote tweets, so without a pull/push the two
  # copies drift: CI freezes at the last commit while this one keeps growing. That
  # is the same divergence that hid a 2.9M gap for weeks, one layer up.
  # Non-fatal: a git problem should not stop the dashboard refreshing.
  git -C "$REPO" pull --rebase --autostash -q origin main \
    || echo "WARNING: could not pull — the shared cache may be behind CI"

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

  # Push the quote scans this run discovered so CI's copy of the Slack report sees
  # them too. Scoped to the cache alone — never sweeps up unrelated working changes.
  if ! git -C "$REPO" diff --quiet -- tweet_cache.json; then
    git -C "$REPO" add tweet_cache.json
    git -C "$REPO" -c user.email=refresh@local -c user.name="dash refresh" \
      commit -q -m "chore: quote scans from the dashboard refresh" \
      && git -C "$REPO" push -q origin main \
      && echo "shared cache pushed for CI" \
      || echo "WARNING: could not push the shared cache — CI will lag until it lands"
  else
    echo "shared cache unchanged."
  fi

  cd "$HOME/marketing-dash"
  vercel --prod --yes
  echo "done"
} >> "$LOG" 2>&1
