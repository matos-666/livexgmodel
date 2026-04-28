/**
 * Supabase Edge Function — SEO Router
 * ─────────────────────────────────────────────────────────────
 * Intercepts requests for /match/:id pages.
 * • Bots  → calls the Fly.io prerender endpoint (returns full HTML
 *            with dynamic meta tags: teams, tournament, score, odds)
 * • Humans → proxies transparently to the Lovable SPA
 *
 * Deploy:
 *   supabase functions deploy seo-router --project-ref lcugjwhcmtpdoernjgei
 *
 * Then in Lovable / middleware.ts (or _redirects), route:
 *   /match/* → https://lcugjwhcmtpdoernjgei.supabase.co/functions/v1/seo-router
 */

const PRERENDER_BASE = "https://livexgmodel-pt.fly.dev";
const LOVABLE_HOST   = "https://live-edge-picks.lovable.app";

const BOT_PATTERNS = [
  "googlebot", "bingbot", "slurp", "duckduckbot",
  "baiduspider", "yandexbot", "sogou", "exabot",
  "facebookexternalhit", "twitterbot", "linkedinbot",
  "whatsapp", "telegrambot", "slackbot", "discordbot",
  "applebot", "ia_archiver", "semrushbot", "ahrefsbot",
  "mj12bot", "dotbot", "rogerbot",
];

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const ua  = (req.headers.get("user-agent") ?? "").toLowerCase();
  const isBot = BOT_PATTERNS.some(p => ua.includes(p));

  // Only intercept /match/:id paths
  const matchResult = url.pathname.match(/^\/match\/(\d+)/);

  if (isBot && matchResult) {
    const matchId = matchResult[1];

    try {
      const prerenderUrl = `${PRERENDER_BASE}/prerender/match/${matchId}`;
      const res = await fetch(prerenderUrl, {
        headers: { "x-forwarded-host": url.hostname },
      });

      const html = await res.text();
      return new Response(html, {
        status: 200,
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          // Cache 60s at the edge — fresh enough for live scores
          "Cache-Control": "public, max-age=60, s-maxage=60",
          "X-Prerendered-By": "seo-router",
        },
      });
    } catch (err) {
      console.error("[seo-router] prerender fetch failed:", err);
      // Fall through to Lovable on error
    }
  }

  // Human (or bot fallback) → proxy to Lovable SPA
  const lovableUrl = new URL(req.url);
  lovableUrl.hostname  = new URL(LOVABLE_HOST).hostname;
  lovableUrl.protocol  = "https:";
  lovableUrl.port      = "";

  const proxyReq = new Request(lovableUrl.toString(), {
    method:  req.method,
    headers: req.headers,
    body:    req.method !== "GET" && req.method !== "HEAD" ? req.body : undefined,
  });

  return fetch(proxyReq);
});
