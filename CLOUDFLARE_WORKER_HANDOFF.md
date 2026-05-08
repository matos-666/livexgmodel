# Cloudflare Worker — webpronos.com SEO routing

## What this does

Adds SEO server-side rendering for search engine bots. Real users keep getting the existing Lovable SPA — zero impact for them. Only Googlebot, Bingbot, etc. get HTML pre-rendered by our Flask backend (already deployed at `livexgmodel-pt.fly.dev`), so they can index team pages, league pages, match pages, etc.

Currently the site has ~2 indexable URLs (homepage + history). After this Worker is live, Google will see ~720+ URLs (374 teams, 44 leagues, 285 matches, plus market/static pages).

---

## What to do

### 1. Open the existing Worker

- Login to https://dash.cloudflare.com
- Sidebar: **Workers & Pages**
- Find the Worker that's currently bound to `webpronos.com` (check **Settings → Triggers → Routes** on each worker — the one with route `webpronos.com/*` is the active one)

### 2. Replace the code

Click **Edit code** on that Worker. Select all existing code (`Cmd+A` / `Ctrl+A`) and replace with the block below:

```js
/**
 * webpronos.com SEO Worker
 * For known bots hitting SEO routes, fetches pre-rendered HTML from the
 * Flask /prerender dispatcher. All other traffic passes through to Lovable.
 */

const PRERENDER_BASE = "https://livexgmodel-pt.fly.dev/prerender";

const BOT_PATTERNS = [
  "googlebot", "bingbot", "slurp", "duckduckbot",
  "baiduspider", "yandexbot", "sogou", "exabot",
  "facebookexternalhit", "twitterbot", "linkedinbot",
  "whatsapp", "telegrambot", "slackbot", "discordbot",
  "applebot", "ia_archiver", "semrushbot", "ahrefsbot",
];

const SEO_ROUTES = [
  /^\/$/,
  /^\/match\/\d+/,
  /^\/team\/[^/]+$/,
  /^\/league\/[^/]+$/,
  /^\/tips\/[^/]+$/,
  /^\/blog(\/.*)?$/,
  /^\/today$/,
  /^\/tomorrow$/,
  /^\/history$/,
  /^\/about$/,
  /^\/terms$/,
  /^\/privacy$/,
  /^\/responsible-gambling$/,
];

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ua  = (request.headers.get("user-agent") || "").toLowerCase();
    const isBot = BOT_PATTERNS.some(p => ua.includes(p));
    const isSeoRoute = SEO_ROUTES.some(re => re.test(url.pathname));

    if (isBot && isSeoRoute) {
      const target = new URL(PRERENDER_BASE);
      target.searchParams.set("path", url.pathname + url.search);
      try {
        const upstream = await fetch(target.toString(), {
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname },
          cf: { cacheTtl: 300, cacheEverything: true },
        });
        return new Response(upstream.body, {
          status: upstream.status,
          headers: {
            "Content-Type":   "text/html; charset=utf-8",
            "Cache-Control":  "public, max-age=300",
            "X-Prerender-By": "webpronos-cf-worker",
          },
        });
      } catch (e) {
        // Fall through to Lovable on prerender failure
      }
    }

    return fetch(request);
  },
};
```

Click **Deploy** (top-right).

### 3. Verify the route binding is correct

Same Worker → tab **Settings** → **Triggers** → **Routes**. Make sure there is at least one route covering the production hostname:

- `webpronos.com/*` (and optionally `www.webpronos.com/*`)

If the route is missing, click **Add route**, set:

- **Route**: `webpronos.com/*`
- **Zone**: `webpronos.com`

### 4. Verify it works (run from any terminal)

```bash
# Should return HTML with "Flamengo" in the title
curl -A "Googlebot" https://webpronos.com/team/flamengo | grep -i "<title>"

# Should return HTML with "Premier League" in the title
curl -A "Googlebot" https://webpronos.com/league/premier-league | grep -i "<title>"

# Should NOT contain x-prerender (real users still get Lovable)
curl -I https://webpronos.com/team/flamengo | grep -i "x-prerender"
```

Expected:

- First two commands → output a `<title>...</title>` tag with the team/league name and "WebPronos"
- Third command → no output (header absent for non-bots) ✓

If the first two commands return an empty HTML shell or a Lovable SPA, the Worker isn't routing correctly — share the output and we'll debug.

---

## Safety notes

- **Real users are unaffected.** The check `isBot && isSeoRoute` is strict. Any user-agent without a bot keyword falls straight through to Lovable.
- **No auth, secrets, or user data** flow through this Worker.
- **Rollback** is one click: in the Worker dashboard → tab **Deployments** → previous version → **Rollback**.
- The Worker only adds latency to bot requests (Google etc.), never to real users.
- If `livexgmodel-pt.fly.dev` is down, bots fall through to Lovable too (the `try/catch` handles it).

---

## What changes for users / search engines

| Who | Before | After |
|---|---|---|
| Real visitor | Lovable SPA | Lovable SPA (no change) |
| Googlebot on `/team/flamengo` | Empty SPA shell | Full HTML with team data |
| Googlebot on `/league/premier-league` | Empty SPA shell | Full HTML with fixtures |
| Googlebot on `/match/12345` | Empty SPA shell | Full HTML with match preview |
| Googlebot on homepage | Empty SPA shell | Full HTML with hero copy |

After the Worker is live, the next step (separate, not for you) is submitting `https://webpronos.com/sitemap.xml` to Google Search Console so the crawler discovers the new URLs faster.
