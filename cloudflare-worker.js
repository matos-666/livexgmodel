/**
 * webpronos.com SEO Worker
 * ─────────────────────────────────────────────────────────────
 * Runs on Cloudflare in front of Lovable.
 *
 *   1. Sitemaps / robots.txt           → always proxied to Flask
 *   2. Known bot on SEO route          → proxied to Flask /prerender
 *   3. Static assets (/assets/*, etc.) → forced edge cache (1 year)
 *      WHY: Lovable serves the JS bundle (473 KB compressed / 1.7 MB raw)
 *      with `cache-control: immutable, max-age=31536000`, but by default
 *      a worker subrequest with `fetch(request)` does NOT share the CDN
 *      cache for that URL — every visitor was round-tripping to Lovable
 *      origin, adding ~200-500ms TTFB to the JS bundle on top of the
 *      already-large parse/eval cost. `cacheEverything: true` puts the
 *      asset in CF's standard tiered cache so subsequent visitors hit
 *      the closest edge POP. Cuts LCP measurably on cold-cache users.
 *   4. HTML index page                 → edge cache 60s + stale-while-
 *      revalidate 300s. Lovable returns `cache-control: no-cache` (which
 *      makes sense for them since the React app fetches live data after
 *      hydration), but the HTML SHELL itself rarely changes — caching
 *      it at edge for 60s avoids hammering Lovable for the same bytes
 *      and gives users a near-zero TTFB.
 *
 * NOTE: We tried injecting an inline LCP shell here (see git history for
 * commit "perf: inject LCP shell"). It produced no measurable improvement
 * because Lovable already server-renders the hero. The shell was pure
 * cost (extra bytes, hidden via `:has()`). Removed.
 */

const FLASK_BASE    = "https://livexgmodel-pt.fly.dev";
const PRERENDER_BASE = FLASK_BASE + "/prerender";

// Paths matched by this regex go through forced edge caching (1y, immutable).
// All Vite/Lovable-built assets land under /assets/, plus a few favicon-
// like static files at the root. Anything dynamic (HTML, /api, /prerender)
// is excluded.
const STATIC_ASSET_RE = /^\/assets\/|^\/favicon\.(?:ico|png)$|^\/apple-touch-icon\.png$|^\/robots\.txt$|^\/manifest\.(?:json|webmanifest)$/;

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
  // Affiliate redirect interstitial — Flask renders the smart loader and
  // bounces to the Betlabel affiliate URL. Must NOT go through Lovable
  // (the SPA would render a 404 for /go/*).
  /^\/go\/bet$/,
];

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ua  = (request.headers.get("user-agent") || "").toLowerCase();

    // 1a. /go/* (affiliate interstitial) → ALWAYS pass through to Flask
    //     with NO caching. Each render writes a tracking row and shows
    //     fresh competitor odds — caching would break analytics + dedupe.
    if (/^\/go\//.test(url.pathname)) {
      try {
        const target = FLASK_BASE + url.pathname + url.search;
        const upstream = await fetch(target, {
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname,
                     "CF-IPCountry": request.cf?.country || "" },
          cf: { cacheTtl: 0, cacheEverything: false },
        });
        return new Response(upstream.body, {
          status: upstream.status,
          headers: {
            "Content-Type":  upstream.headers.get("Content-Type") || "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
          },
        });
      } catch (e) {
        // Fall through to Lovable on Flask failure (will likely 404 SPA-side
        // but better than a hard worker error).
      }
    }

    // 1b. Sitemaps + robots.txt → always proxy to Flask, edge-cache 1h.
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

    // 3. Static assets → force into CF edge cache (1 year, immutable).
    //    Lovable's bundle was bypassing CF cache (cf-cache-status: BYPASS)
    //    because worker subrequests don't auto-cache. Setting cacheEverything
    //    + cacheTtl puts the response in the tiered cache for all subsequent
    //    visitors. The stripped Set-Cookie keeps cache compatibility.
    if (STATIC_ASSET_RE.test(url.pathname)) {
      try {
        const upstream = await fetch(request, {
          cf: { cacheEverything: true, cacheTtl: 31536000 },
        });
        const headers = new Headers(upstream.headers);
        headers.delete("set-cookie");
        headers.set("cache-control", "public, max-age=31536000, immutable");
        return new Response(upstream.body, {
          status: upstream.status,
          headers,
        });
      } catch (e) {
        // Fall through to a plain pass-through if the cache call errors
      }
    }

    // 4. HTML index for real users → 60s edge cache + 300s stale-while-
    //    revalidate. The Vite shell HTML rarely changes; live data is
    //    fetched client-side by React after hydration so caching is safe.
    //    Bots are excluded (they already got /prerender in step 2).
    const isHome = url.pathname === "/" || url.pathname === "";
    if (isHome && !isBot) {
      try {
        const upstream = await fetch(request, {
          cf: { cacheEverything: true, cacheTtl: 60 },
        });
        const ct = upstream.headers.get("content-type") || "";
        if (!ct.toLowerCase().includes("text/html")) {
          return upstream;
        }
        const headers = new Headers(upstream.headers);
        headers.delete("set-cookie");
        headers.set("cache-control", "public, max-age=60, stale-while-revalidate=300");
        return new Response(upstream.body, {
          status: upstream.status,
          headers,
        });
      } catch (e) {
        // Fall through to unmodified pass-through
      }
    }

    // 5. Everyone else → Lovable SPA untouched
    return fetch(request);
  },
};
