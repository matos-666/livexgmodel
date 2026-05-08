/**
 * webpronos.com SEO Worker
 * ─────────────────────────────────────────────────────────────
 * Runs on Cloudflare in front of Lovable. For known bots hitting SEO routes,
 * fetches pre-rendered HTML from the Flask /prerender dispatcher and returns
 * it directly. All other traffic (real users, non-SEO routes) passes through
 * to Lovable untouched.
 *
 * Sitemaps and robots.txt are ALWAYS proxied directly to Flask — for everyone,
 * not just bots — because Lovable (the SPA host) doesn't serve these files.
 */

const FLASK_BASE    = "https://livexgmodel-pt.fly.dev";
const PRERENDER_BASE = FLASK_BASE + "/prerender";

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

// These routes are served by Flask for EVERYONE (bots and real users alike).
// Lovable SPA has no knowledge of these files.
const FLASK_ALWAYS_ROUTES = [
  /^\/sitemap.*\.xml$/,
  /^\/robots\.txt$/,
];

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ua  = (request.headers.get("user-agent") || "").toLowerCase();

    // 1. Sitemaps + robots.txt → always proxy to Flask directly
    if (FLASK_ALWAYS_ROUTES.some(re => re.test(url.pathname))) {
      try {
        const target = FLASK_BASE + url.pathname + url.search;
        const upstream = await fetch(target, {
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname },
          cf: { cacheTtl: 3600, cacheEverything: true },
        });
        const contentType = upstream.headers.get("Content-Type") || "text/xml; charset=utf-8";
        return new Response(upstream.body, {
          status: upstream.status,
          headers: {
            "Content-Type":  contentType,
            "Cache-Control": "public, max-age=3600",
          },
        });
      } catch (e) {
        // Fall through to Lovable on Flask failure
      }
    }

    // 2. Bot on SEO route → proxy to /prerender for full HTML
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

    // 3. Everyone else → Lovable SPA
    return fetch(request);
  },
};
