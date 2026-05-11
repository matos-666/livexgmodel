#!/usr/bin/env bash
#
# Sofascore region auto-failover.
#
# Triggered by .github/workflows/sofascore-watchdog.yml when 3 consecutive
# hourly healthchecks have failed, indicating Sofascore likely blocked the
# current Fly.io region.
#
# Algorithm:
#   1. Read current primary_region from fly.toml.
#   2. For each candidate region in CANDIDATE_POOL (skip current), spawn a
#      throwaway test machine in parallel.
#   3. Wait up to 4 minutes for any of them to log "BG cycle done" with
#      games processed > 0 (= Sofascore reachable from that region).
#   4. Pick the first working region.
#   5. Fork the production volume from the current region to the winner.
#   6. Clone the production machine into the winner with the forked volume.
#   7. Update fly.toml -> primary_region.
#   8. Commit + push the fly.toml change so future deploys use the new region.
#   9. Destroy the old production machine + all test machines + their (empty)
#      test volumes + the now-orphaned old production volume.
#  10. Send a Telegram notification to the admin (if TELEGRAM_BOT_TOKEN +
#      TELEGRAM_ADMIN_CHAT_ID are set).
#
# Required env:
#   FLY_API_TOKEN              — fly.io token with full access to the app
#   APP_NAME                   — fly app name (default: livexgmodel-pt)
#   TELEGRAM_BOT_TOKEN         — optional, for notifications
#   TELEGRAM_ADMIN_CHAT_ID     — optional, target chat for notifications
#
# Exit codes:
#   0 = migrated successfully (or already healthy — no-op)
#   1 = no candidate region worked. Manual intervention needed.
#   2 = setup error (fly.toml missing, flyctl not installed, etc.)

set -euo pipefail

APP_NAME="${APP_NAME:-livexgmodel-pt}"
FLY_TOML="$(dirname "$0")/../fly.toml"
CANDIDATE_POOL=(yyz lhr ewr ams fra arn bom jnb cdg iad lax sjc nrt sin ord)
TEST_WAIT_SECONDS=240
LOG_PREFIX="[failover $(date -u '+%Y-%m-%dT%H:%M:%SZ')]"

log()  { echo "$LOG_PREFIX $*"; }
fail() { log "ERROR: $*"; exit "${2:-1}"; }

command -v flyctl >/dev/null || fail "flyctl not installed" 2
[[ -f "$FLY_TOML" ]] || fail "fly.toml not found at $FLY_TOML" 2
[[ -n "${FLY_API_TOKEN:-}" ]] || fail "FLY_API_TOKEN env var is required" 2

# ─── 1. Read current primary_region ──────────────────────────────────────────
CURRENT_REGION="$(grep -E '^\s*primary_region' "$FLY_TOML" | sed -E 's/.*"([^"]+)".*/\1/')"
[[ -n "$CURRENT_REGION" ]] || fail "could not parse primary_region from fly.toml"
log "current primary_region: $CURRENT_REGION"

# ─── 2. Get current production machine + volume IDs ──────────────────────────
PROD_MACHINE_ID="$(flyctl machines list -a "$APP_NAME" --json 2>/dev/null \
  | python3 -c "import sys,json; m=[x for x in json.load(sys.stdin) if x.get('region')=='$CURRENT_REGION' and x.get('state')=='started']; print(m[0]['id'] if m else '')")"
[[ -n "$PROD_MACHINE_ID" ]] || fail "no started machine in region $CURRENT_REGION"

PROD_VOLUME_ID="$(flyctl volumes list -a "$APP_NAME" --json 2>/dev/null \
  | python3 -c "import sys,json; v=[x for x in json.load(sys.stdin) if x.get('region')=='$CURRENT_REGION' and x.get('attached_machine_id')=='$PROD_MACHINE_ID']; print(v[0]['id'] if v else '')")"
[[ -n "$PROD_VOLUME_ID" ]] || fail "no volume attached to machine $PROD_MACHINE_ID"

log "production machine: $PROD_MACHINE_ID  volume: $PROD_VOLUME_ID"

# ─── 3. Spawn throwaway test clones in candidate regions ─────────────────────
declare -A TEST_MACHINES=()
declare -A TEST_VOLUMES=()
for region in "${CANDIDATE_POOL[@]}"; do
    [[ "$region" == "$CURRENT_REGION" ]] && continue
    output="$(flyctl machine clone "$PROD_MACHINE_ID" --region "$region" -a "$APP_NAME" 2>&1 || true)"
    mid="$(echo "$output" | grep -oE 'Machine [a-f0-9]+ has been created' | awk '{print $2}' | head -1)"
    if [[ -n "$mid" ]]; then
        TEST_MACHINES[$region]=$mid
        log "spawned test machine in $region: $mid"
    else
        log "skip $region (clone failed: $(echo "$output" | grep -oE 'Error:.*' | head -1))"
    fi
done

[[ ${#TEST_MACHINES[@]} -gt 0 ]] || fail "could not spawn any test machine"

# Track empty volumes that the clones automatically created (for cleanup)
sleep 5
for region in "${!TEST_MACHINES[@]}"; do
    mid="${TEST_MACHINES[$region]}"
    vid="$(flyctl volumes list -a "$APP_NAME" --json 2>/dev/null \
        | python3 -c "import sys,json; v=[x for x in json.load(sys.stdin) if x.get('attached_machine_id')=='$mid']; print(v[0]['id'] if v else '')")"
    [[ -n "$vid" ]] && TEST_VOLUMES[$region]=$vid
done

# ─── 4. Poll for the first region that successfully runs a BG cycle ──────────
log "waiting up to ${TEST_WAIT_SECONDS}s for first working region..."
WINNER_REGION=""
deadline=$(( $(date +%s) + TEST_WAIT_SECONDS ))
while [[ $(date +%s) -lt $deadline ]]; do
    for region in "${!TEST_MACHINES[@]}"; do
        mid="${TEST_MACHINES[$region]}"
        # Sofascore reachable from this region if EITHER:
        #   1. "BG cycle done ... N games processed" with N >= 1 (full cycle ran), OR
        #   2. "BG cycle: N live total" with N >= 1 (Sofascore returned events,
        #      even if none are in our monitored leagues at this exact moment)
        # Both signals prove Sofascore isn't 403'ing this region. Originally we
        # only checked #1, but the full cycle requires monitored leagues to be
        # live — false negative when only un-monitored leagues are in play.
        if flyctl logs -a "$APP_NAME" -i "$mid" --no-tail 2>/dev/null \
             | grep -qE 'BG cycle done in [0-9.]+s — [1-9][0-9]* games processed|BG cycle: [1-9][0-9]* live total'; then
            WINNER_REGION=$region
            break 2
        fi
    done
    sleep 8
done

if [[ -z "$WINNER_REGION" ]]; then
    log "no candidate region succeeded within ${TEST_WAIT_SECONDS}s — cleaning up"
    for region in "${!TEST_MACHINES[@]}"; do
        flyctl machine destroy "${TEST_MACHINES[$region]}" --force -a "$APP_NAME" 2>&1 | tail -1 || true
    done
    for region in "${!TEST_VOLUMES[@]}"; do
        flyctl volumes destroy "${TEST_VOLUMES[$region]}" -y -a "$APP_NAME" 2>&1 | tail -1 || true
    done
    fail "no working region found in pool: ${!TEST_MACHINES[*]}"
fi

log "WINNER: $WINNER_REGION"

# ─── 5. Fork the production volume to the winner region ──────────────────────
NEW_VOLUME_ID="$(flyctl volumes fork "$PROD_VOLUME_ID" -a "$APP_NAME" \
    --region "$WINNER_REGION" --name tips_data 2>&1 \
    | grep -oE 'vol_[a-z0-9]+' | head -1)"
[[ -n "$NEW_VOLUME_ID" ]] || fail "fork volume to $WINNER_REGION failed"
log "forked production volume to $WINNER_REGION: $NEW_VOLUME_ID"

# ─── 6. Clone production machine into winner region with forked volume ───────
NEW_MACHINE_ID="$(flyctl machine clone "$PROD_MACHINE_ID" --region "$WINNER_REGION" \
    --attach-volume "${NEW_VOLUME_ID}:/data" -a "$APP_NAME" 2>&1 \
    | grep -oE 'Machine [a-f0-9]+ has been created' | awk '{print $2}' | head -1)"
[[ -n "$NEW_MACHINE_ID" ]] || fail "clone machine to $WINNER_REGION failed"
log "new production machine: $NEW_MACHINE_ID"

# ─── 7. Update fly.toml ──────────────────────────────────────────────────────
sed -i.bak -E "s/^(primary_region = ).*/\1\"$WINNER_REGION\"/" "$FLY_TOML"
rm -f "${FLY_TOML}.bak"
log "fly.toml updated: primary_region = $WINNER_REGION"

# ─── 8. Commit + push the fly.toml change ────────────────────────────────────
cd "$(dirname "$0")/.."
git config user.email "auto-failover@webpronos.com"
git config user.name  "Sofascore Watchdog"
git add fly.toml
git commit -m "infra(auto-failover): migrate $CURRENT_REGION → $WINNER_REGION

Sofascore blocked region $CURRENT_REGION. Auto-failover script picked
$WINNER_REGION after parallel testing all candidate regions." || log "nothing to commit"
git push origin HEAD || log "git push failed (non-fatal)"

# ─── 9. Cleanup: destroy old production + all test machines/volumes ──────────
log "cleanup: destroy old production machine + test machines"
flyctl machine destroy "$PROD_MACHINE_ID" --force -a "$APP_NAME" 2>&1 | tail -1 || true
for region in "${!TEST_MACHINES[@]}"; do
    flyctl machine destroy "${TEST_MACHINES[$region]}" --force -a "$APP_NAME" 2>&1 | tail -1 || true
done
sleep 3
flyctl volumes destroy "$PROD_VOLUME_ID" -y -a "$APP_NAME" 2>&1 | tail -1 || true
for region in "${!TEST_VOLUMES[@]}"; do
    flyctl volumes destroy "${TEST_VOLUMES[$region]}" -y -a "$APP_NAME" 2>&1 | tail -1 || true
done

# ─── 10. Telegram notification ───────────────────────────────────────────────
if [[ -n "${TELEGRAM_BOT_TOKEN:-}" && -n "${TELEGRAM_ADMIN_CHAT_ID:-}" ]]; then
    msg="🟢 *Sofascore auto-failover OK*%0A%0AMigrated \`$CURRENT_REGION\` → \`$WINNER_REGION\`%0AScraping resumed."
    curl -s "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_ADMIN_CHAT_ID}" \
        -d "text=$msg" \
        -d "parse_mode=Markdown" >/dev/null || log "telegram notify failed"
fi

log "done — primary_region is now $WINNER_REGION"
