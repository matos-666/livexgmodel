# Cloudflare Worker — CLI deploy setup (one-time)

After this is done **once**, every future change to `cloudflare-worker.js`
deploys with a single command — no more dashboard visits, no more bothering
the boss.

---

## What the boss needs to do (1 time, ~3 min)

Send us **three values** and we are done with them forever.

### 1. Account ID

- Login to https://dash.cloudflare.com
- Pick any domain on the left → the **right sidebar** shows `Account ID` (it
  is a hex string like `a1b2c3d4e5f6...`)
- Copy that string and send it.

### 2. The existing worker's name

- Sidebar: **Workers & Pages** → look at the list
- Find the one with the route `webpronos.com/*` (open each one's
  **Settings → Triggers → Routes** if unclear)
- Send us the worker's **name** (top of the page, e.g. `webpronos-seo-router`)

### 3. An API token

- Top-right user menu → **My Profile** → **API Tokens** → **Create Token**
- Pick the template **"Edit Cloudflare Workers"**
- Account Resources: **Include → All accounts** (or the specific one)
- Zone Resources: **Include → All zones** (or `webpronos.com`)
- Click **Continue to summary** → **Create Token**
- Copy the token (shown only once) and send it.

That is it. The token can be revoked from the same screen anytime.

---

## What we do when those three arrive

```bash
# 1. Put values in wrangler.toml
#    - replace REPLACE_WITH_ACCOUNT_ID with the account ID
#    - confirm/replace the `name = "..."` line with the worker name

# 2. Store the API token in a local untracked file (NOT in git)
echo 'export CLOUDFLARE_API_TOKEN=THE_TOKEN_HERE' > ~/.webpronos_cf_token
chmod 600 ~/.webpronos_cf_token

# 3. Verify everything works
source ~/.webpronos_cf_token
npx wrangler whoami     # should print the account info
```

---

## Future deploys (forever, from this point on)

```bash
tools/deploy_worker.sh
```

That script sources the token, runs `npx wrangler deploy`, and prints the
new version's live URL. No browser involved.

Test it after deploying:

```bash
curl -sI https://webpronos.com/sitemap_index.xml | head -3
# HTTP/2 200 ← success
```

---

## Security notes

- `~/.webpronos_cf_token` is outside the repo, never committed.
- `wrangler.toml` contains the account ID — that ID is **not** a secret
  (it is visible in any dashboard URL), only the API token is.
- The token only grants Workers edit + (optionally) Zone read; it cannot
  touch DNS, billing, or other resources unless we extend its scope.
- Revoke from `My Profile → API Tokens` at any time; just regenerate and
  the script keeps working.
