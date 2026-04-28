/**
 * Lovable / Vercel-style Edge Middleware
 * ─────────────────────────────────────────────────────────────
 * Place this at the root of the Lovable project as `middleware.ts`.
 * It runs at the edge BEFORE the SPA is served.
 *
 * For /match/:id requests from known bots, it redirects to the
 * Supabase Edge Function (seo-router) which returns prerendered HTML.
 * All other requests pass through normally to the Lovable SPA.
 */

import { NextResponse } from "next/server"; // Lovable may use a different import
import type { NextRequest } from "next/server";

const SEO_ROUTER =
  "https://lcugjwhcmtpdoernjgei.supabase.co/functions/v1/seo-router";

const BOT_PATTERNS = [
  "googlebot", "bingbot", "slurp", "duckduckbot",
  "baiduspider", "yandexbot", "sogou", "exabot",
  "facebookexternalhit", "twitterbot", "linkedinbot",
  "whatsapp", "telegrambot", "slackbot", "discordbot",
  "applebot", "ia_archiver", "semrushbot", "ahrefsbot",
];

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const ua = (request.headers.get("user-agent") ?? "").toLowerCase();
  const isBot = BOT_PATTERNS.some(p => ua.includes(p));

  if (isBot && /^\/match\/\d+/.test(pathname)) {
    // Rewrite internally to Supabase Edge Function — keeps the URL clean
    const target = new URL(SEO_ROUTER);
    target.pathname = pathname;
    return NextResponse.rewrite(target);
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/match/:id*"],
};
