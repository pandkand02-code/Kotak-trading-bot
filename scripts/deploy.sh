#!/usr/bin/env bash
# Deploy script run on the DigitalOcean droplet by GitHub Actions on every
# push. Idempotent — safe to re-run by hand from the droplet console too:
#   bash /root/Kotak-trading-bot/scripts/deploy.sh
#
# Picks up the branch from $GITHUB_REF_NAME if set by the Actions runner,
# otherwise stays on the currently-checked-out branch.

set -euo pipefail

cd /root/Kotak-trading-bot

BRANCH="${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD)}"
echo "─── deploying branch: ${BRANCH} ──────────────────────────────"

# 1. Pull the latest code for that branch. `git fetch` + `git reset --hard`
#    is safer than `git pull` — it guarantees the droplet ends in the exact
#    state of origin, even if a merge would have conflicted.
git fetch --all --prune
git checkout "${BRANCH}"
git reset --hard "origin/${BRANCH}"
echo "HEAD now at $(git rev-parse --short HEAD): $(git log -1 --pretty=%s)"

# 2. Re-sync Python deps. --quiet keeps the log readable; pip is a no-op
#    when nothing in requirements.txt has changed.
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# 3. Restart the bot service. systemctl returns 0 on a clean restart even
#    if the unit was already running.
systemctl restart nexus-bot

# 4. Wait for /health to come back up. Up to 15 s with 1 s gap.
echo "Waiting for /health ..."
for i in $(seq 1 15); do
  if curl -sf --max-time 2 http://localhost:8000/health > /dev/null; then
    echo "✓ Bot healthy after ${i}s"
    curl -s http://localhost:8000/health | head -c 200
    echo
    exit 0
  fi
  sleep 1
done

echo "✗ /health did not respond within 15s — dumping last 20 log lines:"
journalctl -u nexus-bot --no-pager -n 20 || true
exit 1
