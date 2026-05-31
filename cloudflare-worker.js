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
 * i18n (added 2026-05):
 *   The site supports four locales — EN (default, at root), PT-PT (/pt/),
 *   PT-BR (/br/) and ES (/es/). The worker handles the FIRST-VISIT routing
 *   so users land on their language without needing to click the switcher:
 *
 *     - Cookie wp_lang set?         → user already chose, respect their choice
 *     - URL has explicit prefix?    → respect the URL (NEVER override an
 *                                      explicit link, even if cookie disagrees)
 *     - First visit to ROOT only?   → detect via CF-IPCountry and 302-redirect
 *                                      to the matching prefix (no cookie set —
 *                                      the SPA sets it after the redirect via
 *                                      the language switcher logic)
 *     - Bots?                       → NEVER redirected (so each crawled URL
 *                                      maps 1:1 to indexable content)
 *
 *   SEO route regexes accept an optional /pt|/br|/es prefix so prerender
 *   matching works for translated bot crawls. The Flask /prerender route
 *   reads the prefix from the forwarded ?path= and renders in that locale.
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
// Note: /manifest.json is intentionally EXCLUDED — Flask serves it (per-locale
// PWA manifest). Same for /sw.js, /push-bootstrap.js and /pwa-icon/* — all
// PWA-related assets live on Flask, not Lovable.
const STATIC_ASSET_RE = /^\/assets\/|^\/favicon\.(?:ico|png)$|^\/apple-touch-icon\.png$|^\/robots\.txt$/;

const BOT_PATTERNS = [
  "googlebot", "bingbot", "slurp", "duckduckbot",
  "baiduspider", "yandexbot", "sogou", "exabot",
  "facebookexternalhit", "twitterbot", "linkedinbot",
  "whatsapp", "telegrambot", "slackbot", "discordbot",
  "applebot", "ia_archiver", "semrushbot", "ahrefsbot",
];

// Recognised language prefixes. EN is the default and has NO prefix.
const LANG_CODES = ["pt", "br", "es"];
const LANG_PREFIX_RE = new RegExp(`^/(?:${LANG_CODES.join("|")})(?=/|$)`);

// ─── Slug-level redirects for the BR locale ─────────────────────────────
//
// Blog: when the BR blog posts were re-keyed to Portuguese slugs we kept
//   the EN URLs working with a 301 → BR slug. Add new entries here as
//   posts are renamed; keep old entries forever to preserve link equity.
//
// Static routes: /br/about, /br/terms, etc. → /br/sobre, /br/termos, etc.
//   These 301s ensure ONE canonical URL per locale and let any existing
//   inbound link to the EN-slug-on-/br/ continue to resolve.
//
// Both maps are flat { fullPath: fullPath } — easy to scan and extend.
const BR_BLOG_SLUG_REDIRECTS = {
  "/br/blog/live-betting-vs-pre-match-why-timing-matters":
    "/br/blog/apostas-ao-vivo-vs-pre-jogo-por-que-o-timing-muda-tudo",
  "/br/blog/what-is-betting-edge-how-to-calculate":
    "/br/blog/o-que-e-edge-em-apostas-como-calcular",
  "/br/blog/over-under-betting-strategy-xg":
    "/br/blog/apostas-over-under-por-que-xg-torna-previsivel",
  "/br/blog/how-real-time-xg-powers-our-betting-tips":
    "/br/blog/como-xg-tempo-real-move-tips-apostas",
  "/br/blog/what-is-xg-expected-goals-football":
    "/br/blog/o-que-e-xg-gols-esperados-futebol",
};

const PT_BLOG_SLUG_REDIRECTS = {
  "/pt/blog/live-betting-vs-pre-match-why-timing-matters":
    "/pt/blog/apostas-em-direto-vs-pre-jogo-porque-o-timing-importa",
  "/pt/blog/what-is-betting-edge-how-to-calculate":
    "/pt/blog/o-que-e-o-edge-nas-apostas-como-calcular",
  "/pt/blog/over-under-betting-strategy-xg":
    "/pt/blog/apostas-over-under-porque-o-xg-as-torna-previsiveis",
  "/pt/blog/how-real-time-xg-powers-our-betting-tips":
    "/pt/blog/como-o-xg-em-tempo-real-impulsiona-as-nossas-tips",
  "/pt/blog/what-is-xg-expected-goals-football":
    "/pt/blog/o-que-e-xg-golos-esperados-futebol",
};

const ES_BLOG_SLUG_REDIRECTS = {
  "/es/blog/live-betting-vs-pre-match-why-timing-matters":
    "/es/blog/apuestas-en-vivo-vs-pre-partido-por-que-importa-el-timing",
  "/es/blog/what-is-betting-edge-how-to-calculate":
    "/es/blog/que-es-el-edge-en-apuestas-como-calcularlo",
  "/es/blog/over-under-betting-strategy-xg":
    "/es/blog/apuestas-over-under-por-que-xg-las-hace-predecibles",
  "/es/blog/how-real-time-xg-powers-our-betting-tips":
    "/es/blog/como-el-xg-en-tiempo-real-impulsa-nuestras-tips",
  "/es/blog/what-is-xg-expected-goals-football":
    "/es/blog/que-es-xg-goles-esperados-futbol",
};

const BR_STATIC_SLUG_REDIRECTS = {
  "/br/about":                "/br/sobre",
  "/br/terms":                "/br/termos",
  "/br/privacy":              "/br/privacidade",
  "/br/responsible-gambling": "/br/jogo-responsavel",
  "/br/today":                "/br/hoje",
  "/br/tomorrow":             "/br/amanha",
  "/br/history":              "/br/historico",
};

const PT_STATIC_SLUG_REDIRECTS = {
  // PT-PT static slugs happen to coincide with BR (same Portuguese words).
  // Kept under a separate map so they live under /pt/ and the worker
  // 301-redirects the EN-slug-on-/pt/ form to the Portuguese counterpart.
  "/pt/about":                "/pt/sobre",
  "/pt/terms":                "/pt/termos",
  "/pt/privacy":              "/pt/privacidade",
  "/pt/responsible-gambling": "/pt/jogo-responsavel",
  "/pt/today":                "/pt/hoje",
  "/pt/tomorrow":             "/pt/amanha",
  "/pt/history":              "/pt/historico",
};

const ES_STATIC_SLUG_REDIRECTS = {
  "/es/about":                "/es/sobre",
  "/es/terms":                "/es/terminos",
  "/es/privacy":              "/es/privacidad",
  "/es/responsible-gambling": "/es/juego-responsable",
  "/es/today":                "/es/hoy",
  "/es/tomorrow":             "/es/manana",
  "/es/history":              "/es/historial",
};

// Merge maps for the redirect step. Object spread keeps each entry's
// distinct full-path key, so a single lookup serves all three locales.
const BLOG_SLUG_REDIRECTS = {
  ...BR_BLOG_SLUG_REDIRECTS,
  ...PT_BLOG_SLUG_REDIRECTS,
  ...ES_BLOG_SLUG_REDIRECTS,
};

const STATIC_SLUG_REDIRECTS = {
  ...BR_STATIC_SLUG_REDIRECTS,
  ...PT_STATIC_SLUG_REDIRECTS,
  ...ES_STATIC_SLUG_REDIRECTS,
};

// Reverse map: localized-slug → EN-canonical, used to widen SEO_ROUTES
// matching (so bots crawling /br/sobre, /pt/sobre, /es/sobre all find
// the correct prerender mapping). Kept per-locale for completeness; not
// directly consumed today but mirrors the BR pattern for symmetry.
const BR_TO_EN_STATIC = Object.fromEntries(
  Object.entries(BR_STATIC_SLUG_REDIRECTS).map(([en, br]) => [br, en])
);
const PT_TO_EN_STATIC = Object.fromEntries(
  Object.entries(PT_STATIC_SLUG_REDIRECTS).map(([en, pt]) => [pt, en])
);
const ES_TO_EN_STATIC = Object.fromEntries(
  Object.entries(ES_STATIC_SLUG_REDIRECTS).map(([en, es]) => [es, en])
);

// CF-IPCountry → preferred lang. Mapping picks the realistic native lang
// for each country; non-matched countries fall through to EN.
const COUNTRY_TO_LANG = {
  // Portuguese (Portugal + PALOP)
  PT: "pt", AO: "pt", MZ: "pt", CV: "pt", GW: "pt", ST: "pt", TL: "pt",
  // Portuguese (Brazil)
  BR: "br",
  // Spanish — Spain + Latin America (Mexico, Argentina, etc.)
  ES: "es", MX: "es", AR: "es", CO: "es", CL: "es", PE: "es",
  UY: "es", VE: "es", EC: "es", BO: "es", PY: "es", DO: "es",
  GT: "es", HN: "es", SV: "es", NI: "es", CR: "es", PA: "es", CU: "es",
  PR: "es", GQ: "es",
};

// Build SEO route list ONCE: each route also matches its /pt|/br|/es-prefixed
// variant so bot prerender works for translated URLs. The BR-specific
// localized slugs (/br/sobre, /br/hoje, etc.) are also matched so bots
// crawling Portuguese URLs hit the prerender layer.
const LANG_PREFIX_OPT = `(?:/(?:${LANG_CODES.join("|")}))?`;
const SEO_ROUTES = [
  new RegExp(`^${LANG_PREFIX_OPT}/?$`),
  new RegExp(`^${LANG_PREFIX_OPT}/match/\\d+`),
  new RegExp(`^${LANG_PREFIX_OPT}/team/[^/]+$`),
  new RegExp(`^${LANG_PREFIX_OPT}/league/[^/]+$`),
  new RegExp(`^${LANG_PREFIX_OPT}/tips/[^/]+$`),
  new RegExp(`^${LANG_PREFIX_OPT}/blog(/.*)?$`),
  new RegExp(`^${LANG_PREFIX_OPT}/today$`),
  new RegExp(`^${LANG_PREFIX_OPT}/tomorrow$`),
  new RegExp(`^${LANG_PREFIX_OPT}/history$`),
  new RegExp(`^${LANG_PREFIX_OPT}/about$`),
  new RegExp(`^${LANG_PREFIX_OPT}/terms$`),
  new RegExp(`^${LANG_PREFIX_OPT}/privacy$`),
  new RegExp(`^${LANG_PREFIX_OPT}/responsible-gambling$`),
  // BR-localized static slugs — same content, BR URL.
  /^\/br\/sobre$/,
  /^\/br\/termos$/,
  /^\/br\/privacidade$/,
  /^\/br\/jogo-responsavel$/,
  /^\/br\/hoje$/,
  /^\/br\/amanha$/,
  /^\/br\/historico$/,
  // PT-PT localized slugs (identical to BR — both Portuguese — but under /pt/).
  /^\/pt\/sobre$/,
  /^\/pt\/termos$/,
  /^\/pt\/privacidade$/,
  /^\/pt\/jogo-responsavel$/,
  /^\/pt\/hoje$/,
  /^\/pt\/amanha$/,
  /^\/pt\/historico$/,
  // ES localized slugs.
  /^\/es\/sobre$/,
  /^\/es\/terminos$/,
  /^\/es\/privacidad$/,
  /^\/es\/juego-responsable$/,
  /^\/es\/hoy$/,
  /^\/es\/manana$/,
  /^\/es\/historial$/,
  // ── DYNAMIC PATH LOCALIZATION ──
  // PT-BR and PT-PT use /jogo and /liga for /match and /league respectively.
  // ES uses /partido and /liga. Without these, bots crawling localized
  // dynamic URLs get the Lovable SPA shell (with the homepage hero as H1)
  // instead of our match/league prerender — a massive SEO regression
  // because Google sees identical generic content for every match URL.
  /^\/br\/jogo\/\d+/,
  /^\/pt\/jogo\/\d+/,
  /^\/es\/partido\/\d+/,
  /^\/br\/liga\/[^/]+$/,
  /^\/pt\/liga\/[^/]+$/,
  /^\/es\/liga\/[^/]+$/,
  /^\/br\/equipa\/[^/]+$/,
  /^\/pt\/equipa\/[^/]+$/,
  /^\/es\/equipo\/[^/]+$/,
  /^\/br\/palpites\/[^/]+$/,
  /^\/pt\/palpites\/[^/]+$/,
  /^\/es\/pronosticos\/[^/]+$/,
];

// Same lang-aware regex for the home-page cache step.
const HOME_RE = new RegExp(`^${LANG_PREFIX_OPT}/?$`);

// These routes are served by Flask for EVERYONE (bots and real users alike).
// Lovable SPA has no knowledge of these files.
const FLASK_ALWAYS_ROUTES = [
  /^\/sitemap.*\.xml$/,   // sitemap.xml + sitemap-br.xml + sitemap-index.xml
  /^\/robots\.txt$/,
  // Affiliate redirect interstitial — Flask renders the smart loader and
  // bounces to the Betlabel affiliate URL. Must NOT go through Lovable
  // (the SPA would render a 404 for /go/*).
  /^\/go\/bet$/,
  // PWA + Web Push assets — all served by Flask. Lovable has no such
  // routes so without this they would 404 against the SPA shell.
  //   /manifest.json     — install metadata (Flask renders per-locale)
  //   /sw.js             — Service Worker (must be at root scope)
  //   /pwa-icon/*.png    — Pillow-generated 192/512/maskable icons
  //   /push-bootstrap.js — tiny JS that registers the SW + exposes
  //                        window.wpEnablePush() for opt-in
  /^\/manifest\.json$/,
  /^\/sw\.js$/,
  /^\/pwa-icon\//,
  /^\/push-bootstrap\.js$/,
];

// Pull a cookie value out of the raw Cookie header. Returns undefined if
// the cookie isn't present or doesn't match the expected lang codes.
function readLangCookie(cookieHeader) {
  if (!cookieHeader) return undefined;
  const m = cookieHeader.match(/(?:^|;\s*)wp_lang=([a-z]{2})(?:;|$)/);
  if (!m) return undefined;
  const v = m[1];
  return (v === "en" || LANG_CODES.includes(v)) ? v : undefined;
}

// ─── PWA injector ───────────────────────────────────────────────────────────
// Adds the PWA manifest link + push bootstrap script to every HTML response.
// The injection happens at the CF edge so we don't have to touch Lovable.
//
//  • <link rel="manifest" href="/manifest.json?lang=...">  → PWA install
//  • <link rel="apple-touch-icon" href="/pwa-icon/192.png"> → iOS home icon
//  • <meta name="theme-color" content="#10b981">           → standalone bar
//  • <script src="/push-bootstrap.js" defer>               → SW + opt-in API
//
// HTMLRewriter is CF-native (zero-copy streaming) so this adds <1ms overhead.
class HeadInjector {
  constructor(lang) { this.lang = lang || "en"; }
  element(el) {
    // Only inject inside <head>; this handler is wired to "head" selector.
    el.append(
      `<link rel="manifest" href="/manifest.json?lang=${this.lang}">` +
      `<link rel="apple-touch-icon" href="/pwa-icon/192.png">` +
      `<meta name="theme-color" content="#10b981">` +
      `<meta name="apple-mobile-web-app-capable" content="yes">` +
      `<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">` +
      `<meta name="apple-mobile-web-app-title" content="WebPronos">` +
      `<script src="/push-bootstrap.js" defer></script>`,
      { html: true }
    );
  }
}

function injectPwa(response, lang) {
  try {
    const ct = (response.headers.get("Content-Type") || "").toLowerCase();
    if (!ct.includes("text/html")) return response;
    return new HTMLRewriter()
      .on("head", new HeadInjector(lang))
      .transform(response);
  } catch (e) {
    return response;
  }
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ua  = (request.headers.get("user-agent") || "").toLowerCase();
    const cookieLang = readLangCookie(request.headers.get("Cookie") || "");
    const pathLangMatch = url.pathname.match(LANG_PREFIX_RE);
    const pathLang = pathLangMatch ? pathLangMatch[0].slice(1) : null;
    const isBot = BOT_PATTERNS.some(p => ua.includes(p));

    // ── EMBED SUBDOMAIN ─────────────────────────────────────────────
    // embed.webpronos.com is dedicated to the iframe widgets we serve
    // to partners (InBet WC2026 widgets etc). It MUST bypass the SPA
    // routing — i18n redirects, prerender, sitemap routing, Lovable
    // fallback are all irrelevant here. Just forward straight to Flask
    // with no caching (widgets manage their own poll cadence) and let
    // the Flask routes do their thing.
    //
    // This split keeps the main site fully cacheable while widgets stay
    // dynamic, AND ensures /widget/wc2026/* paths never fall through to
    // Lovable (which would return 404 since Lovable has no such routes).
    if (url.hostname.startsWith("embed.")) {
      try {
        const target = FLASK_BASE + url.pathname + url.search;
        const upstream = await fetch(target, {
          method:  request.method,
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname,
                     "CF-IPCountry": request.cf?.country || "",
                     "Referer":      request.headers.get("Referer") || "" },
          body:    (request.method !== "GET" && request.method !== "HEAD")
                     ? request.body : undefined,
          cf:      { cacheTtl: 0, cacheEverything: false },
          redirect: "manual",
        });
        // Iframes load cross-origin from the partner site — set permissive
        // CORS + frame-ancestors so any inbet.io page can embed without
        // X-Frame-Options blocking. The widget HTML itself comes from Flask.
        const headers = new Headers(upstream.headers);
        headers.set("Access-Control-Allow-Origin", "*");
        // X-Frame-Options DENY/SAMEORIGIN from origin would block iframe.
        // Replace with permissive frame-ancestors so iframe embedding works.
        headers.delete("X-Frame-Options");
        headers.delete("x-frame-options");
        headers.set("Content-Security-Policy", "frame-ancestors *");
        return new Response(upstream.body, {
          status:  upstream.status,
          headers: headers,
        });
      } catch (e) {
        return new Response("Origin unreachable", { status: 502 });
      }
    }

    // 0. Slug-level 301 redirects — must run BEFORE every other branch so
    //    the canonical URL is fixed at the edge. Blog and static-page
    //    EN-slug URLs on /br/, /pt/ and /es/ all permanently redirect to
    //    their localized counterparts. 301 (not 302) so browsers/CDNs/
    //    Google cache the redirect.
    {
      const blogTarget = BLOG_SLUG_REDIRECTS[url.pathname];
      if (blogTarget) {
        return Response.redirect(`${url.origin}${blogTarget}${url.search}`, 301);
      }
      const staticTarget = STATIC_SLUG_REDIRECTS[url.pathname];
      if (staticTarget) {
        return Response.redirect(`${url.origin}${staticTarget}${url.search}`, 301);
      }
      // Dynamic-path 301s: /br/match/X → /br/jogo/X, /br/league/X → /br/liga/X
      // (and pt/es equivalents). Keeps inbound links from the SPA's older
      // routing or external sites pointing at the EN slug on a locale URL.
      // Uses prefix matching so the trailing /id/slug or /slug-segment is
      // preserved verbatim.
      const DYNAMIC_PREFIX_301 = {
        "/br/match/":   "/br/jogo/",
        "/br/league/":  "/br/liga/",
        "/br/team/":    "/br/equipa/",
        "/br/tips/":    "/br/palpites/",
        "/pt/match/":   "/pt/jogo/",
        "/pt/league/":  "/pt/liga/",
        "/pt/team/":    "/pt/equipa/",
        "/pt/tips/":    "/pt/palpites/",
        "/es/match/":   "/es/partido/",
        "/es/league/":  "/es/liga/",
        "/es/team/":    "/es/equipo/",
        "/es/tips/":    "/es/pronosticos/",
      };
      for (const [src, dst] of Object.entries(DYNAMIC_PREFIX_301)) {
        if (url.pathname.startsWith(src)) {
          const rest = url.pathname.slice(src.length);
          return Response.redirect(
            `${url.origin}${dst}${rest}${url.search}`, 301
          );
        }
      }
    }

    // 1a. /go/*, /r/* and /betradar/* → ALWAYS pass through to Flask
    //     with NO caching.
    //
    //     /go/*       = affiliate interstitial (each render writes tracking)
    //     /r/*        = generic short-link redirector
    //     /betradar/* = branded short-link path for Telegram BetRadar AI
    //                   CTAs (same lookup as /r/ but the path itself shows
    //                   "betradar" in the Telegram "Open link?" modal)
    //
    // redirect:"manual" is CRITICAL — Cloudflare's fetch() auto-follows
    // 3xx by default, which silently turned our 302-to-affiliate into
    // a 200 + Lovable HTML. We want the 302 + Location header surfaced
    // to the browser intact so the browser does the redirect.
    if (/^\/go\//.test(url.pathname)
        || /^\/r\//.test(url.pathname)
        || /^\/betradar\//.test(url.pathname)) {
      try {
        const target = FLASK_BASE + url.pathname + url.search;
        // Forward Referer so Flask can infer locale from the page that
        // opened the popup (eg Referer: /br/jogo/X → render in pt-br).
        // Without this fallback, if the SPA forgets to append &lang=...
        // the popup defaults to EN even for non-EN users.
        const upstream = await fetch(target, {
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname,
                     "CF-IPCountry": request.cf?.country || "",
                     "Referer":      request.headers.get("Referer") || "" },
          cf: { cacheTtl: 0, cacheEverything: false },
          redirect: "manual",
        });
        // Copy the full set of upstream headers so Location (and any
        // others Flask set) reach the browser. Just override the
        // caching directive — everything else is upstream's choice.
        const headers = new Headers(upstream.headers);
        headers.set("Cache-Control", "no-store, no-cache, must-revalidate");
        return new Response(upstream.body, {
          status:  upstream.status,
          headers: headers,
        });
      } catch (e) {
        // Fall through to Lovable on Flask failure (will likely 404 SPA-side
        // but better than a hard worker error).
      }
    }

    // 1b. Sitemaps + robots.txt → always proxy to Flask.
    //     Cache only 2xx responses. Non-2xx must NOT be cached (a brief
    //     origin glitch returning 4xx/5xx would otherwise get pinned at
    //     every POP for the full TTL — Google Search Console then sees
    //     stale 403/1006 errors while the origin is healthy).
    //     `cacheTtlByStatus` instructs CF to: cache 200-299 for 1h, do not
    //     cache 4xx (TTL 0) and bypass cache on 5xx (TTL -1).
    if (FLASK_ALWAYS_ROUTES.some(re => re.test(url.pathname))) {
      try {
        const target = FLASK_BASE + url.pathname + url.search;
        const upstream = await fetch(target, {
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname },
          cf: {
            cacheEverything: true,
            cacheTtlByStatus: {
              "200-299": 3600,
              "300-399": 0,
              "400-499": 0,
              "500-599": -1,
            },
          },
        });
        const contentType = upstream.headers.get("Content-Type") || "text/xml; charset=utf-8";
        const ok = upstream.status >= 200 && upstream.status < 300;
        return new Response(upstream.body, {
          status: upstream.status,
          headers: {
            "Content-Type":  contentType,
            // Browser/Google cache only on success; on errors force no-store
            // so a transient glitch is not pinned downstream either.
            "Cache-Control": ok ? "public, max-age=3600" : "no-store",
          },
        });
      } catch (e) {
        // Fall through to Lovable on Flask failure
      }
    }

    // 1c. i18n REDIRECT — first-visit only, at the root path.
    //
    //     If the user (a) has no language cookie and (b) the URL has no
    //     explicit /pt|/br|/es prefix and (c) is hitting the root `/`, we
    //     detect their country and 302 to the matching localised root.
    //
    //     Why ONLY the root? Because URLs like /match/123 may have been
    //     shared via link/Google/Twitter and the user explicitly clicked
    //     that EN URL — silently redirecting them to /br/match/123 would
    //     be hostile UX (and confuses Google about which URL is canonical
    //     for the EN audience).
    //
    //     Bots are NEVER redirected — each crawled URL must map 1:1 to
    //     stable content so the search index doesn't churn.
    //
    //     No Set-Cookie here: the SPA's language switcher manages the
    //     cookie after the user lands on the prefix. This avoids cache
    //     fragmentation by cookie at the edge.
    if (!isBot && !cookieLang && !pathLang && (url.pathname === "/" || url.pathname === "")) {
      const country = (request.cf?.country || "").toUpperCase();
      const preferred = COUNTRY_TO_LANG[country];
      if (preferred) {
        const target = `${url.origin}/${preferred}/${url.search}`;
        return new Response(null, {
          status: 302,
          headers: {
            "Location": target,
            "Cache-Control": "no-store",
            "Vary": "CF-IPCountry, Cookie",
          },
        });
      }
      // No country detection / EN-speaking country → fall through to EN.
    }

    // 2. Bot on SEO route → proxy to /prerender for full HTML
    //
    //    The forwarded ?path= preserves the lang prefix (if any), so
    //    Flask /prerender can strip it and render the matching locale.
    //    Example: /br/match/123 → ?path=/br/match/123 → Flask sets locale
    //    pt-br before rendering match 123.
    const isSeoRoute = SEO_ROUTES.some(re => re.test(url.pathname));

    if (isBot && isSeoRoute) {
      const target = new URL(PRERENDER_BASE);
      target.searchParams.set("path", url.pathname + url.search);
      try {
        const upstream = await fetch(target.toString(), {
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname },
          // Cache 2xx prerenders for 5 min; never cache errors (a flaky
          // origin should not produce 1h+ stale 4xx/5xx at the edge).
          cf: {
            cacheEverything: true,
            cacheTtlByStatus: {
              "200-299": 300,
              "300-399": 0,
              "400-499": 0,
              "500-599": -1,
            },
          },
        });
        const ok = upstream.status >= 200 && upstream.status < 300;
        return new Response(upstream.body, {
          status: upstream.status,
          headers: {
            "Content-Type":   "text/html; charset=utf-8",
            "Cache-Control":  ok ? "public, max-age=300" : "no-store",
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
    //
    //    HOME_RE matches `/`, `/pt`, `/pt/`, `/br`, `/br/`, `/es`, `/es/`,
    //    each cached separately by the CF cache key (URL-based) so the
    //    SPA shell can differ per locale (eg. <html lang> attribute) without
    //    interfering across locales.
    if (HOME_RE.test(url.pathname) && !isBot) {
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
        const baseResp = new Response(upstream.body, {
          status: upstream.status,
          headers,
        });
        // Inject PWA <link> + bootstrap <script> into the SPA shell so the
        // service worker registers and users can opt-in via DevTools console
        // (window.wpEnablePush()) until Lovable ships a UI for it.
        return injectPwa(baseResp, pathLang || cookieLang || "en");
      } catch (e) {
        // Fall through to unmodified pass-through
      }
    }

    // 4b. /api/* → forward to Flask with per-endpoint edge caching.
    //
    //     Before this rule the SPA's relative /api/* fetches were falling
    //     through to Lovable (which returns the SPA shell HTML — 30KB of
    //     nothing useful) and the SPA had to retry against a hardcoded
    //     livexgmodel-pt.fly.dev fallback. That detour adds 200-700ms per
    //     call on first paint and bypasses CF edge caching entirely.
    //
    //     With this rule:
    //       - Same-origin /api/* works (no CORS preflight needed)
    //       - Frequently-hit endpoints serve from the CF edge in 20-50ms
    //         for repeat visitors (vs 300-700ms round-trip to sjc)
    //       - The SSE picks stream + admin endpoints stay uncached
    if (/^\/api\//.test(url.pathname)) {
      try {
        // SSE stream and admin / mutating endpoints must NEVER cache.
        // Live picks stream is event-driven; admin/health/diag are
        // interactive; POST/PUT/DELETE are mutations.
        const isSse        = /^\/api\/live\/picks-stream/.test(url.pathname);
        const isAdmin      = /^\/api\/(admin|telegram)\//.test(url.pathname);
        const isMutating   = request.method !== "GET" && request.method !== "HEAD";
        const noCache      = isSse || isAdmin || isMutating;

        // Per-endpoint TTLs (in seconds) for safe-to-cache reads. Tight
        // TTLs because live data changes constantly; longer TTLs for
        // SEO/aggregate data that updates per hour.
        let ttl;
        if      (noCache)                                        ttl = 0;
        else if (/^\/api\/state\/tips/.test(url.pathname))       ttl = 15;
        else if (/^\/api\/state\/upcoming/.test(url.pathname))   ttl = 60;
        else if (/^\/api\/state(\b|\?|$)/.test(url.pathname))    ttl = 15;
        else if (/^\/api\/health/.test(url.pathname))            ttl = 30;
        else if (/^\/api\/footer\/dynamic/.test(url.pathname))   ttl = 300;
        else if (/^\/api\/seo\//.test(url.pathname))             ttl = 300;
        else if (/^\/api\/match\/\d+/.test(url.pathname))        ttl = 30;
        else                                                     ttl = 10;

        const target = FLASK_BASE + url.pathname + url.search;
        const fetchInit = {
          method:  request.method,
          headers: { "User-Agent": ua, "X-Forwarded-Host": url.hostname,
                     "CF-IPCountry": request.cf?.country || "" },
          body:    isMutating ? request.body : undefined,
          redirect: "manual",
        };
        if (ttl > 0) {
          fetchInit.cf = {
            cacheEverything: true,
            cacheTtlByStatus: {
              "200-299": ttl,
              "300-399": 0,
              "400-499": 0,
              "500-599": -1,
            },
          };
        } else {
          // Explicit zero so CF doesn't auto-cache GETs by file-extension
          fetchInit.cf = { cacheTtl: 0, cacheEverything: false };
        }

        const upstream = await fetch(target, fetchInit);
        const ok = upstream.status >= 200 && upstream.status < 300;
        const headers = new Headers(upstream.headers);
        // CORS-friendly: same-origin so no preflight, but allow credentialed
        // calls from any webpronos.com page (incl. embed.webpronos.com).
        headers.set("Access-Control-Allow-Origin", "*");
        headers.set("Vary", "Accept-Encoding");
        // Browser cache mirrors edge TTL on success; force no-store on error.
        if (noCache) {
          headers.set("Cache-Control", "no-store");
        } else if (ok) {
          headers.set("Cache-Control", `public, max-age=${ttl}, s-maxage=${ttl}`);
        } else {
          headers.set("Cache-Control", "no-store");
        }
        return new Response(upstream.body, {
          status:  upstream.status,
          headers: headers,
        });
      } catch (e) {
        // Origin unreachable — fall through to Lovable (will return SPA
        // shell, useless for JSON but at least visible to the user).
      }
    }

    // 5. Everyone else → Lovable SPA, with PWA injection on HTML responses.
    //    Non-HTML responses (JS bundles, images, etc.) pass through unchanged
    //    because injectPwa is a no-op on non-text/html content-types.
    const upstreamSpa = await fetch(request);
    return injectPwa(upstreamSpa, pathLang || cookieLang || "en");
  },
};
