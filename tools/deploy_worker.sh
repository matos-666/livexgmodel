#!/usr/bin/env bash
#
# One-command deploy of cloudflare-worker.js to webpronos.com.
#
# Prerequisites (one-time, see WRANGLER_SETUP.md):
#   - wrangler.toml populated with account_id + worker name
#   - ~/.webpronos_cf_token contains:  export CLOUDFLARE_API_TOKEN=...
#
# Usage:
#   tools/deploy_worker.sh             # deploy
#   tools/deploy_worker.sh --dry-run   # validate config without deploying

set -euo pipefail

cd "$(dirname "$0")/.."

TOKEN_FILE="${HOME}/.webpronos_cf_token"

if [[ ! -f "$TOKEN_FILE" ]]; then
    echo "❌ Token file not found at $TOKEN_FILE"
    echo "   Follow WRANGLER_SETUP.md to create it (one-time)."
    exit 1
fi

# shellcheck source=/dev/null
source "$TOKEN_FILE"

if [[ -z "${CLOUDFLARE_API_TOKEN:-}" ]]; then
    echo "❌ CLOUDFLARE_API_TOKEN is empty in $TOKEN_FILE"
    exit 1
fi

# Sanity-check the token is valid + see which account we're authenticating as
echo "── auth check ──"
npx --yes wrangler@latest whoami 2>&1 | head -10

# Sanity-check the worker file parses (helps catch dumb syntax errors before
# wrangler does — wrangler errors are less readable).
if command -v node >/dev/null 2>&1; then
    node --check cloudflare-worker.js && echo "✅ cloudflare-worker.js syntax OK"
fi

# Detect --dry-run flag
if [[ "${1:-}" == "--dry-run" ]]; then
    echo ""
    echo "── DRY RUN — wrangler deploy --dry-run ──"
    npx --yes wrangler@latest deploy --dry-run
    exit 0
fi

echo ""
echo "── DEPLOYING ──"
npx --yes wrangler@latest deploy

echo ""
echo "── post-deploy smoke test ──"
sleep 3
for path in /robots.txt /sitemap.xml /sitemap_index.xml /sitemap-leagues.xml; do
    status="$(curl -sI -o /dev/null -w '%{http_code}' "https://webpronos.com${path}" || echo "ERR")"
    printf '  %-30s → HTTP %s\n' "$path" "$status"
done

echo ""
echo "✅ done."
