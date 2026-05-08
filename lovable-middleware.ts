/**
 * Lovable / Vercel-style Edge Middleware
 * ─────────────────────────────────────────────────────────────
 * Place this at the root of the Lovable project as `middleware.ts`.
 * It runs at the edge BEFORE the SPA is served.
 *
 * For known bots, it forwards SEO routes to the Flask /prerender dispatcher,
 * which returns fully-rendered HTML with title/meta/JSON-LD. All other
 * traffic passes through normally to the Lovable SPA.
 */

import { NextResponse } from "next/server"; // Lovable may use a different import
import type { NextRequest } from "next/server";

const PRERENDER_BASE = "https://livexgmodel-pt.fly.dev/prerender";

const BOT_PATTERNS = [
  "googlebot", "bingbot", "slurp", "duckduckbot",
  "baiduspider", "yandexbot", "sogou", "exabot",
  "facebookexternalhit", "twitterbot", "linkedinbot",
  "whatsapp", "telegrambot", "slackbot", "discordbot",
  "applebot", "ia_archiver", "semrushbot", "ahrefsbot",
];

// Routes the dispatcher knows how to render
const SEO_ROUTE_PATTERNS = [
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
  /^\/$/,
];

export function middleware(request: NextRequest) {
  const { pathname, search } = request.nextUrl;
  const ua = (request.headers.get("user-agent") ?? "").toLowerCase();
  const isBot = BOT_PATTERNS.some(p => ua.includes(p));
  const isSeoRoute = SEO_ROUTE_PATTERNS.some(re => re.test(pathname));

  if (isBot && isSeoRoute) {
    // Forward to /prerender?path=<original_path> — keeps the URL clean
    const target = new URL(PRERENDER_BASE);
    target.searchParams.set("path", pathname + (search || ""));
    return NextResponse.rewrite(target);
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    "/",
    "/match/:id*",
    "/team/:slug*",
    "/league/:slug*",
    "/tips/:market*",
    "/blog",
    "/blog/:slug*",
    "/today",
    "/tomorrow",
    "/history",
    "/about",
    "/terms",
    "/privacy",
    "/responsible-gambling",
  ],
};
