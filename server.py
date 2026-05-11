#!/usr/bin/env python3
"""
Sofascore Live xG Scraper v4 — with Live Odds & Benter Value
=============================================================
Install:
    pip install flask curl_cffi requests   ← RECOMMENDED

Run:
    python3 server.py          → API server on :5050
    python3 server.py test     → CLI test
"""

import json
import time
import logging
import sys
import math
import os
import threading
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher

from flask import Flask, jsonify, request as flask_request, Response
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sofascore")

# ── CORS — permite pedidos do dashboard no Netlify (ou qualquer origem) ──
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>",             methods=["OPTIONS"])
def cors_preflight(path):
    return "", 204

SOFASCORE_API = "https://api.sofascore.com/api/v1"
SOFASCORE_WEB = "https://www.sofascore.com"

_client_type = None
_session = None


# ════════════════════════════════════════════════════════════
#  THE ODDS API — Configuration
# ════════════════════════════════════════════════════════════

# ── API keys: prioritized list (first entry = highest priority) ─────────────
# Override via env var ODDS_API_KEYS (comma-separated). The list is consumed
# top-down: each key is used until its remaining requests drop below
# ODDS_API_KEY_THRESHOLD, at which point the next key takes over.
_DEFAULT_ODDS_KEYS = [
    "9405c5e34517519015dde64a625abec4",  # fresh — added 2026-05-11
    "fa168a602fa110a3ccff53c995a527f7",  # fresh — added 2026-05-11
    "97afd246d773029c90facaf6ca9974c7",  # fresh — added 2026-05-10
    "41f3dfd7bbc62e4dc6b56386f87e4bff",  # fresh — added 2026-05-10
    "dee24918d4fce672488ce5924efa9cc6",  # fresh — added 2026-05-10
    "6dcb3758adc01c36cb7980da9580b813",  # fresh — added 2026-05-09
    "92ced3263acb9582da75fcf2fdab0472",  # fresh — added 2026-05-09
    "c270c89328c3f0f8ef2900357d320c18",  # fresh — added 2026-05-09
    "dc6bb435743a8519f5523c8c698675ae",  # fresh — added 2026-05-09
    "bbd321935d529638185ff20493a529bd",  # fresh — added 2026-05-08
    "cb98e44e1017b7074f09d64c3d6e13cf",  # fresh — added 2026-05-08
    "4ea8ea2fd077d1e3a78d22a96457cfaa",  # fresh — added 2026-05-08
    "991b181701f0357ee480d5cc4a130775",  # fresh — added 2026-05-08
    "8df74b4da3761af46c46ccaf9aa66e06",  # fresh — added 2026-05-07
    "dd8897a2d2a3f132cf61ec7b60023655",  # fresh — added 2026-05-06
    "937d66f8602ffa24432758360e85a4f4",  # fresh — added 2026-05-06
    "47cba4ae66282b03fd97e132901ee90c",  # fresh — added 2026-05-06
    "f503c6dd5df67563cf516864f33bd0a7",  # fresh — added 2026-05-06
    "00cb1a6454aa5451338fc6583326bab4",  # fresh — added 2026-05-05
    "bc2a057a6832adf77e7b725b7609ca19",  # fresh — added 2026-05-05
    "1a8a5dbd0516293080b8075b3a991d2e",  # fresh — added 2026-05-04
    "86131a98d36adbc7db54d8f5130494b5",  # fresh — added 2026-05-04
    "ed8e51f030dc323c47dd19acf2bf6378",  # fresh — added 2026-05-03
    "cf867bdd1cb4af120d1c5c27b12cccf0",  # fresh — added 2026-05-03
    "fa67a9529adddf7c847cb15d4177c4c7",  # fresh — added 2026-05-03
    "ac746e9e33bbcc4436c7b1fd90d1f284",  # fresh — added 2026-05-03
    "9760251740fad32eb0517faabba5d074",  # fresh — added 2026-05-03
    "fc5ffe1ae61f015bd565fca2241a75c9",
    "d7660835556393ef648d3fd44400a105",
    "ee0c7f34fc6c582778e591ca6fa46ab7",
    "5270d56caf490ee0643c7291a1b9fc56",
    "52e6d0bb0daaa9934550b4dc72614f0e",
    "09bc0566f02e87b93872930c42de1291",
    "8b7efbd1ffa865b6c9fe536ebcb9c6b7",
    "415046f2c4c0225f6baa278c9b10bba9",
    "8b00b28cc1004ed9726fb34309bdbdb3",
    "05eb21f6d1bdb27eb5f34e72ff0cb9f5",
    "827f8bdfce132211875593112498d659",
    "a6aebdd942dafbbe3a227bbc29bf7611",
    "cce963dbb03009752097869c7852b661",
    "50a2955658b6807a541115f81d414a76",
    "bff58f986dca45ba7308dd05ea8ee539",
    "532b6697ae8a3de4e3ead16d298cf34c",
    "c9794c676f842fa06aea5aa6c000e0bf",
    "1c0ef0a7986dd583c9eb8fe1a25f7815",
    "290e27c8a2b8c6989616812292957b32",
    "4914cf81d4ad7d509c768a9cbfdcdea2",
    "bbc2a6255721b666e6ee8b1b38542dfe",
    "86431b2f6ef8a1dc431a253f018e10c3",
    "313f5d07b1e7eecd09c596ba0604c83e",
    "5a3c8142d4cf6fc67f41e0c7f1909893",
]

_env_keys = [k.strip() for k in os.environ.get("ODDS_API_KEYS", "").split(",") if k.strip()]
ODDS_API_KEYS: list[str] = _env_keys if _env_keys else _DEFAULT_ODDS_KEYS

# Back-compat: many places in the codebase still reference ODDS_API_KEY directly
# (cache keys, quota endpoint default, log prefixes). Point it at the head of the
# rotation list so behaviour stays consistent.
ODDS_API_KEY = ODDS_API_KEYS[0] if ODDS_API_KEYS else os.environ.get("ODDS_API_KEY", "")

# Legacy single-backup env var: append to the rotation list if present and not
# already there (keeps older deployments working without a config flip).
_env_backup = os.environ.get("ODDS_API_KEY_BACKUP", "").strip()
if _env_backup and _env_backup not in ODDS_API_KEYS:
    ODDS_API_KEYS.append(_env_backup)

ODDS_API_KEY_THRESHOLD = 5    # rotate when remaining drops below this
ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def _active_odds_key() -> str:
    """
    Walk ODDS_API_KEYS top-down and return the first key that still has more
    than ODDS_API_KEY_THRESHOLD requests remaining (or that hasn't been probed
    yet — unprobed keys are assumed fresh and given a chance).

    If every key in the list is exhausted, fall back to the last one so the
    request still goes through (and surfaces a quota-exceeded error from the
    upstream API rather than failing silently here).
    """
    if not ODDS_API_KEYS:
        return ""
    for key in ODDS_API_KEYS:
        rem = _api_quotas.get(key)
        # Unknown remaining → treat as fresh (probe will populate it on first use)
        if rem is None or rem >= ODDS_API_KEY_THRESHOLD:
            return key
    # All keys exhausted — return the last one so the upstream error is exposed
    return ODDS_API_KEYS[-1]

# ════════════════════════════════════════════════════════════
#  TELEGRAM BOT
# ════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Comma-separated list of Telegram chat_ids allowed to use /admin_stats.
# Discover your own with the /whoami bot command, then:
#   fly secrets set TELEGRAM_ADMIN_CHAT_IDS="123456789"
TELEGRAM_ADMIN_CHAT_IDS: set[int] = {
    int(x.strip()) for x in os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

def _is_tg_admin(chat_id: int) -> bool:
    return chat_id in TELEGRAM_ADMIN_CHAT_IDS

# ── Affiliate CTAs — rotated on every pick alert ──────────────────────────
# Each entry: (display_text, url)
# Rotating CTAs (5 variations: #4, #9, #5, #2, and custom "Entrar nesta Pick")
_TG_CTAS = [
    # #4: Ultra-energetic with dynamic potential gain (parametrized)
    (None, "https://dashboard.onetwoaffiliates.com/click?campaign_id=797&ref_id=370"),  # template
    # #9: Cheeky/playful challenge
    ("😉 Nem penses 2x, vai!", "https://track.affshares.com/visit/?bta=657658&nci=5653"),
    # #5: Direct/minimal
    ("▶️ Próximo Passo: Clicar", "https://dashboard.onetwoaffiliates.com/click?campaign_id=796&ref_id=370"),
    # #2: Cool/gaming language
    ("💎 Entrar na Play", "https://dashboard.onetwoaffiliates.com/click?campaign_id=797&ref_id=370"),
    # Custom: Trust builder
    ("🎯 Entrar nesta Pick", "https://track.affshares.com/visit/?bta=657658&nci=5653"),
]
_tg_cta_counter = 0
_tg_cta_lock = threading.Lock()

def _next_cta(odds: float = None, stake: float = 100.0) -> str:
    """
    Return the next CTA as an HTML hyperlink, cycling through _TG_CTAS.
    For CTA #4 (index 0), calculate dynamic potential gain:
      potential_gain = (odds - 1) * stake
    """
    global _tg_cta_counter
    with _tg_cta_lock:
        idx = _tg_cta_counter % len(_TG_CTAS)
        _tg_cta_counter += 1

    text, url = _TG_CTAS[idx]

    # Special handling for #4 (index 0): dynamic gain calculation
    if idx == 0 and odds and odds > 0:
        potential_gain = (odds - 1) * stake
        text = f"🔥 AGORA! Ganhar +€{potential_gain:.0f} (odds {odds:.2f})!"

    return f'<a href="{url}">{text}</a>'

_COUNTRY_FLAGS = {
    "england": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "spain": "🇪🇸", "italy": "🇮🇹", "germany": "🇩🇪",
    "france": "🇫🇷", "portugal": "🇵🇹", "netherlands": "🇳🇱", "belgium": "🇧🇪",
    "turkey": "🇹🇷", "scotland": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "austria": "🇦🇹", "switzerland": "🇨🇭",
    "russia": "🇷🇺", "ukraine": "🇺🇦", "greece": "🇬🇷", "poland": "🇵🇱",
    "czech republic": "🇨🇿", "denmark": "🇩🇰", "sweden": "🇸🇪", "norway": "🇳🇴",
    "usa": "🇺🇸", "brazil": "🇧🇷", "argentina": "🇦🇷", "mexico": "🇲🇽",
    "colombia": "🇨🇴", "chile": "🇨🇱", "japan": "🇯🇵", "south korea": "🇰🇷",
    "australia": "🇦🇺", "china": "🇨🇳",
    # Confederations / international competitions
    "europe": "🇪🇺", "international": "🌍", "world": "🌍",
    "south america": "🌎", "north america": "🌎", "americas": "🌎",
    "asia": "🌏", "africa": "🌍", "oceania": "🌏",
}

def _country_flag(country: str) -> str:
    if not country:
        return "⚽"
    return _COUNTRY_FLAGS.get(country.lower(), "⚽")

def _tg_subscribers() -> list:
    """Return all active subscriber chat_ids from DB."""
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT chat_id FROM tg_subscribers WHERE active = 1"
            ).fetchall()
            return [str(r["chat_id"]) for r in rows]
    except Exception:
        return []

def _tg_subscribe(chat_id: int, username: str = None, first_name: str = None, source: str = None):
    with _db() as conn:
        # Add source column if it doesn't exist yet (migration)
        try:
            conn.execute("ALTER TABLE tg_subscribers ADD COLUMN source TEXT")
        except Exception:
            pass  # column already exists
        conn.execute("""
            INSERT INTO tg_subscribers (chat_id, username, first_name, subscribed_at, active, source)
            VALUES (?, ?, ?, ?, 1, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                active = 1,
                subscribed_at = excluded.subscribed_at,
                source = COALESCE(tg_subscribers.source, excluded.source)
        """, (chat_id, username, first_name, int(time.time()), source))

def _tg_unsubscribe(chat_id: int):
    with _db() as conn:
        conn.execute(
            "UPDATE tg_subscribers SET active = 0 WHERE chat_id = ?", (chat_id,)
        )


def _tg_log_start(chat_id: int, username: str | None, first_name: str | None,
                  start_param: str | None):
    """
    Log every /start invocation (one row per call, even if same user starts multiple times).
    `start_param` is the deep-link payload — e.g. when someone opens
    https://t.me/YourBot?start=instagram_oct it arrives as "instagram_oct".
    """
    try:
        with _db() as conn:
            conn.execute("""
                INSERT INTO tg_starts (chat_id, username, first_name, start_param, started_at)
                VALUES (?, ?, ?, ?, ?)
            """, (chat_id, username, first_name, start_param, int(time.time())))
    except Exception as e:
        log.warning(f"_tg_log_start failed for {chat_id}: {e}")


def _tg_admin_stats() -> str:
    """Build the /admin_stats reply text (HTML)."""
    try:
        now = int(time.time())
        day_ago  = now - 86400
        week_ago = now - 86400 * 7
        with _db() as conn:
            total_starts = conn.execute("SELECT COUNT(*) c FROM tg_starts").fetchone()["c"]
            unique_users = conn.execute("SELECT COUNT(DISTINCT chat_id) c FROM tg_starts").fetchone()["c"]
            starts_24h   = conn.execute("SELECT COUNT(*) c FROM tg_starts WHERE started_at >= ?", (day_ago,)).fetchone()["c"]
            starts_7d    = conn.execute("SELECT COUNT(*) c FROM tg_starts WHERE started_at >= ?", (week_ago,)).fetchone()["c"]
            new_users_24h = conn.execute("""
                SELECT COUNT(*) c FROM (
                    SELECT chat_id, MIN(started_at) first_ts
                    FROM tg_starts GROUP BY chat_id
                ) WHERE first_ts >= ?
            """, (day_ago,)).fetchone()["c"]
            new_users_7d = conn.execute("""
                SELECT COUNT(*) c FROM (
                    SELECT chat_id, MIN(started_at) first_ts
                    FROM tg_starts GROUP BY chat_id
                ) WHERE first_ts >= ?
            """, (week_ago,)).fetchone()["c"]
            active_subs = conn.execute(
                "SELECT COUNT(*) c FROM tg_subscribers WHERE active = 1"
            ).fetchone()["c"]
            inactive_subs = conn.execute(
                "SELECT COUNT(*) c FROM tg_subscribers WHERE active = 0"
            ).fetchone()["c"]
            top_params = conn.execute("""
                SELECT COALESCE(NULLIF(start_param, ''), '(none)') src,
                       COUNT(*) c,
                       COUNT(DISTINCT chat_id) u
                FROM tg_starts
                GROUP BY src
                ORDER BY c DESC
                LIMIT 10
            """).fetchall()
            # Per-source subscriber conversion funnel
            try:
                source_funnel = conn.execute("""
                    SELECT COALESCE(NULLIF(s.source, ''), '(none)') src,
                           COUNT(*) total,
                           SUM(CASE WHEN s.active = 1 THEN 1 ELSE 0 END) active
                    FROM tg_subscribers s
                    GROUP BY src
                    ORDER BY total DESC
                    LIMIT 10
                """).fetchall()
            except Exception:
                source_funnel = []
            recent = conn.execute("""
                SELECT chat_id, username, first_name, start_param, started_at
                FROM tg_starts
                ORDER BY started_at DESC
                LIMIT 5
            """).fetchall()

        from datetime import datetime as _dt, timezone as _tz

        lines = [
            "📊 <b>Admin Stats</b>",
            "",
            f"👥 <b>Subscribers:</b> {active_subs} ativos · {inactive_subs} saíram",
            f"🆔 <b>Utilizadores únicos:</b> {unique_users}",
            f"🚀 <b>/start events:</b> {total_starts} total",
            "",
            f"📅 <b>Últimas 24h:</b>  {starts_24h} starts · {new_users_24h} novos utilizadores",
            f"📆 <b>Últimos 7d:</b>   {starts_7d} starts · {new_users_7d} novos utilizadores",
            "",
            "🔗 <b>Funil por fonte (starts → subs ativos):</b>",
        ]
        # Merge starts and subs per source
        starts_by_src = {r["src"]: {"starts": r["c"], "unique": r["u"]} for r in top_params}
        subs_by_src   = {r["src"]: {"total": r["total"], "active": r["active"]} for r in source_funnel}
        all_srcs = sorted(set(list(starts_by_src.keys()) + list(subs_by_src.keys())),
                          key=lambda s: starts_by_src.get(s, {}).get("starts", 0), reverse=True)
        if all_srcs:
            for src in all_srcs:
                s  = starts_by_src.get(src, {})
                sb = subs_by_src.get(src, {})
                n_starts = s.get("starts", 0)
                n_unique = s.get("unique", 0)
                n_active = sb.get("active", 0)
                conv = f"{100*n_active/n_unique:.0f}%" if n_unique else "—"
                lines.append(
                    f"  • <code>{src[:25]}</code>  {n_starts} starts → {n_active} subs ativos ({conv})"
                )
        else:
            lines.append("  <i>(sem dados ainda)</i>")

        lines.append("")
        lines.append("🆕 <b>Últimos 5 /start:</b>")
        if recent:
            for r in recent:
                ts = _dt.fromtimestamp(r["started_at"], tz=_tz.utc).strftime("%m-%d %H:%M")
                uname = f"@{r['username']}" if r["username"] else (r["first_name"] or f"id:{r['chat_id']}")
                param = f" [{r['start_param']}]" if r["start_param"] else ""
                lines.append(f"  • {ts} — {uname}{param}")
        else:
            lines.append("  <i>(nenhum)</i>")

        return "\n".join(lines)
    except Exception as e:
        log.exception("_tg_admin_stats failed")
        return f"❌ Erro ao gerar stats: {e}"

def _get_algorithm_results() -> dict:
    """Calculate overall algorithm statistics from tips database."""
    try:
        with _db() as conn:
            # Count picks by result
            all_tips = conn.execute("SELECT result, odd_entry FROM tips").fetchall()

            if not all_tips:
                return {
                    "total": 0,
                    "wins": 0,
                    "losses": 0,
                    "pending": 0,
                    "pnl": 0,
                    "roi": 0
                }

            total = len(all_tips)
            # Map database values: "green" (win), "red" (loss), "void" (void/cancelled)
            wins = sum(1 for r, _ in all_tips if r in ("win", "green"))
            losses = sum(1 for r, _ in all_tips if r in ("loss", "red"))
            pending = sum(1 for r, _ in all_tips if r is None)

            # Calculate P&L: sum of (odd - 1) for wins minus 1 unit lost per loss
            pnl = 0
            for result, odd_entry in all_tips:
                if result in ("win", "green") and odd_entry:
                    pnl += (odd_entry - 1)  # profit on this bet
                elif result in ("loss", "red"):
                    pnl -= 1  # loss of 1 unit

            roi = (pnl / total * 100) if total > 0 else 0

            return {
                "total": total,
                "wins": wins,
                "losses": losses,
                "pending": pending,
                "pnl": round(pnl, 2),
                "roi": round(roi, 1)
            }
    except Exception as e:
        log.error(f"Error calculating algorithm results: {e}")
        return {"total": 0, "wins": 0, "losses": 0, "pending": 0, "pnl": 0, "roi": 0}

def _send_telegram(text: str, chat_id=None):
    """Send a message via Telegram Bot API. If chat_id is None, sends to all subscribers."""
    if not TELEGRAM_BOT_TOKEN:
        return
    import urllib.request as _urllib
    ids = [str(chat_id)] if chat_id else _tg_subscribers()
    for cid in ids:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = json.dumps({
                "chat_id": cid,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }).encode()
            req = _urllib.Request(url, data=payload,
                                  headers={"Content-Type": "application/json"})
            _urllib.urlopen(req, timeout=10)
        except Exception as e:
            log.error(f"Telegram send failed to {cid}: {e}")

def _send_telegram_buttons(text: str, chat_id: int, buttons: list):
    """Send a message with inline keyboard buttons to a specific chat_id."""
    if not TELEGRAM_BOT_TOKEN:
        return
    import urllib.request as _urllib
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": str(chat_id),
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": {"inline_keyboard": buttons},
        }).encode()
        req = _urllib.Request(url, data=payload,
                              headers={"Content-Type": "application/json"})
        _urllib.urlopen(req, timeout=10)
    except Exception as e:
        log.error(f"Telegram send_buttons failed to {chat_id}: {e}")

def _tg_answer_callback(callback_id: str, text: str = ""):
    """Answer a Telegram callback query to dismiss the loading spinner."""
    if not TELEGRAM_BOT_TOKEN:
        return
    import urllib.request as _urllib
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
        payload = json.dumps({"callback_query_id": callback_id, "text": text}).encode()
        req = _urllib.Request(url, data=payload,
                              headers={"Content-Type": "application/json"})
        _urllib.urlopen(req, timeout=5)
    except Exception as e:
        log.error(f"Telegram answer_callback failed: {e}")

def _get_monthly_stats() -> dict:
    """P&L em € este mês (€100/pick), odds médias, total picks liquidados."""
    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        month_start_ts = int(datetime(now.year, now.month, 1, tzinfo=timezone.utc).timestamp())
        STAKE = get_setting("stake_per_bet", 100.0)
        with _db() as conn:
            tips = conn.execute(
                "SELECT result, odd_entry, market FROM tips "
                "WHERE wall_ts >= ? AND result IS NOT NULL",
                (month_start_ts,)
            ).fetchall()
        if not tips:
            return {"total": 0, "settled": 0, "pnl_eur": 0.0, "avg_odds": 0.0, "markets": {}}
        pnl = 0.0
        odds_sum = 0.0
        settled = 0
        markets: dict = {}
        for tip in tips:
            result, odd_entry, market = tip["result"], tip["odd_entry"], tip["market"]
            mkt = market or "—"
            markets[mkt] = markets.get(mkt, 0) + 1
            if result in ("win", "green") and odd_entry:
                pnl += (odd_entry - 1) * STAKE
                odds_sum += odd_entry
                settled += 1
            elif result in ("loss", "red"):
                pnl -= STAKE
                odds_sum += (odd_entry or 0)
                settled += 1
        avg_odds = odds_sum / settled if settled > 0 else 0.0
        return {
            "total": len(tips),
            "settled": settled,
            "pnl_eur": round(pnl, 2),
            "avg_odds": round(avg_odds, 2),
            "markets": markets,
        }
    except Exception as e:
        log.error(f"_get_monthly_stats error: {e}")
        return {"total": 0, "settled": 0, "pnl_eur": 0.0, "avg_odds": 0.0, "markets": {}}


def _period_start_ts(period: str) -> int | None:
    """Return unix-ts cutoff for the given period, or None for 'alltime'."""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    if period == "month":
        return int(datetime(now.year, now.month, 1, tzinfo=timezone.utc).timestamp())
    if period == "7d":
        return int((now - timedelta(days=7)).timestamp())
    return None  # alltime


_PERIOD_LABEL = {
    "alltime": ("♾ Geral",  "Geral"),
    "month":   ("📅 Mês",   "Este Mês"),
    "7d":      ("🗓 7 dias", "Últimos 7 dias"),
}


def _get_period_stats(period: str = "alltime") -> dict:
    """Aggregate stats over a period: alltime, month, 7d."""
    try:
        STAKE = get_setting("stake_per_bet", 100.0)
        cutoff = _period_start_ts(period)
        where  = "WHERE result IS NOT NULL"
        params: tuple = ()
        if cutoff is not None:
            where += " AND wall_ts >= ?"
            params = (cutoff,)

        with _db() as conn:
            ordered = conn.execute(
                f"SELECT result, odd_entry, market, wall_ts FROM tips {where} ORDER BY wall_ts ASC",
                params
            ).fetchall()

        if not ordered:
            return {"settled": 0, "wins": 0, "losses": 0, "winrate": 0.0,
                    "pnl_eur": 0.0, "roi": 0.0, "avg_odds": 0.0,
                    "best_streak": 0, "best_win_pnl": 0.0, "markets": {}}

        wins = losses = 0
        pnl  = 0.0
        odds_sum = 0.0
        best_win_pnl = 0.0
        streak = best_streak = 0
        markets: dict = {}

        for tip in ordered:
            result, odd, market = tip["result"], tip["odd_entry"], tip["market"]
            mkt = market or "—"
            markets[mkt] = markets.get(mkt, 0) + 1
            if result in ("win", "green") and odd:
                profit = (odd - 1) * STAKE
                wins += 1; pnl += profit; odds_sum += odd
                streak += 1
                best_streak  = max(best_streak, streak)
                best_win_pnl = max(best_win_pnl, profit)
            elif result in ("loss", "red"):
                losses += 1; pnl -= STAKE; odds_sum += (odd or 0)
                streak = 0

        settled = wins + losses
        return {
            "settled":      settled,
            "wins":         wins,
            "losses":       losses,
            "winrate":      round((wins / settled * 100) if settled else 0.0, 1),
            "pnl_eur":      round(pnl, 2),
            "roi":          round((pnl / (settled * STAKE) * 100) if settled else 0.0, 1),
            "avg_odds":     round((odds_sum / settled) if settled else 0.0, 2),
            "best_streak":  best_streak,
            "best_win_pnl": round(best_win_pnl, 2),
            "markets":      markets,
        }
    except Exception as e:
        log.error(f"_get_period_stats({period}) error: {e}")
        return {"settled": 0, "wins": 0, "losses": 0, "winrate": 0.0,
                "pnl_eur": 0.0, "roi": 0.0, "avg_odds": 0.0,
                "best_streak": 0, "best_win_pnl": 0.0, "markets": {}}


def _get_biggest_green(period: str = "alltime") -> dict | None:
    """
    Find the highest-odd winning pick in the period — extra engagement hook.
    Returns {"odd", "profit", "match", "market", "label", "ts"} or None.
    """
    try:
        STAKE = get_setting("stake_per_bet", 100.0)
        cutoff = _period_start_ts(period)
        where = "WHERE t.result IN ('win','green') AND t.odd_entry IS NOT NULL"
        params: tuple = ()
        if cutoff is not None:
            where += " AND t.wall_ts >= ?"
            params = (cutoff,)

        with _db() as conn:
            row = conn.execute(f"""
                SELECT t.odd_entry odd, t.market, t.label, t.wall_ts ts,
                       g.home_team, g.away_team
                FROM tips t
                LEFT JOIN games g ON g.id = t.match_id
                {where}
                ORDER BY t.odd_entry DESC LIMIT 1
            """, params).fetchone()

        if not row or not row["odd"]:
            return None
        return {
            "odd":    round(row["odd"], 2),
            "profit": round((row["odd"] - 1) * STAKE, 2),
            "match":  f"{row['home_team']} vs {row['away_team']}" if row["home_team"] else "—",
            "market": row["market"] or "—",
            "label":  row["label"] or "",
            "ts":     row["ts"],
        }
    except Exception as e:
        log.error(f"_get_biggest_green({period}) error: {e}")
        return None


# Back-compat alias kept for older callers
def _get_alltime_stats() -> dict:
    """All-time aggregate stats across every settled pick in the DB."""
    try:
        STAKE = get_setting("stake_per_bet", 100.0)
        with _db() as conn:
            tips = conn.execute(
                "SELECT result, odd_entry, market FROM tips WHERE result IS NOT NULL"
            ).fetchall()
            total_picks = conn.execute("SELECT COUNT(*) c FROM tips").fetchone()["c"]

        if not tips:
            return {
                "total": total_picks, "settled": 0, "wins": 0, "losses": 0,
                "winrate": 0.0, "pnl_eur": 0.0, "roi": 0.0, "avg_odds": 0.0,
                "best_streak": 0, "best_win_pnl": 0.0,
            }

        wins = losses = 0
        pnl  = 0.0
        odds_sum = 0.0
        best_win_pnl = 0.0
        # Streak tracking (chronological)
        streak = best_streak = 0

        # Re-fetch ordered for streak calculation
        with _db() as conn:
            ordered = conn.execute(
                "SELECT result, odd_entry FROM tips WHERE result IS NOT NULL ORDER BY wall_ts ASC"
            ).fetchall()

        for tip in ordered:
            result, odd = tip["result"], tip["odd_entry"]
            if result in ("win", "green") and odd:
                profit = (odd - 1) * STAKE
                wins += 1
                pnl  += profit
                odds_sum += odd
                streak += 1
                best_streak = max(best_streak, streak)
                best_win_pnl = max(best_win_pnl, profit)
            elif result in ("loss", "red"):
                losses += 1
                pnl -= STAKE
                odds_sum += (odd or 0)
                streak = 0

        settled = wins + losses
        winrate = (wins / settled * 100) if settled else 0.0
        avg_odds = (odds_sum / settled) if settled else 0.0
        roi = (pnl / (settled * STAKE) * 100) if settled else 0.0

        return {
            "total":        total_picks,
            "settled":      settled,
            "wins":         wins,
            "losses":       losses,
            "winrate":      round(winrate, 1),
            "pnl_eur":      round(pnl, 2),
            "roi":          round(roi, 1),
            "avg_odds":     round(avg_odds, 2),
            "best_streak":  best_streak,
            "best_win_pnl": round(best_win_pnl, 2),
        }
    except Exception as e:
        log.error(f"_get_alltime_stats error: {e}")
        return {
            "total": 0, "settled": 0, "wins": 0, "losses": 0,
            "winrate": 0.0, "pnl_eur": 0.0, "roi": 0.0, "avg_odds": 0.0,
            "best_streak": 0, "best_win_pnl": 0.0,
        }


def _send_daily_summary(days_back: int = 0, force_send: bool = False):
    """
    Calculate daily P&L and send summary to all Telegram subscribers.
    Only sends if daily profit > €25 (unless force_send=True).
    Includes: lucro €, odds médias, ROI, maior odd with match info, and encouragement text.
    days_back: 0 = today, 1 = yesterday, etc.
    force_send: if True, send even if lucro <= €25
    """
    try:
        from datetime import datetime, timezone, timedelta

        # Use Lisbon timezone to determine the target day
        lisbon_tz = pytz.timezone('Europe/Lisbon')
        now_lisbon = datetime.now(lisbon_tz)
        target_date = now_lisbon - timedelta(days=days_back)
        target_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=lisbon_tz)
        target_end = target_start + timedelta(days=1)

        # Convert to UTC timestamps
        target_start_ts = int(target_start.timestamp())
        target_end_ts = int(target_end.timestamp())

        STAKE = get_setting("stake_per_bet", 100.0)

        with _db() as conn:
            # Get all settled tips from the target day, with match info
            tips = conn.execute(
                "SELECT t.result, t.odd_entry, t.label, t.market, t.match_id, "
                "       g.home_team, g.away_team "
                "FROM tips t "
                "LEFT JOIN games g ON g.id = t.match_id "
                "WHERE t.wall_ts >= ? AND t.wall_ts < ? AND t.result IS NOT NULL "
                "ORDER BY t.odd_entry DESC",
                (target_start_ts, target_end_ts)
            ).fetchall()

        if not tips:
            log.info(f"_send_daily_summary: no tips settled {days_back} day(s) ago")
            return

        # Calculate stats
        lucro = 0.0
        odds_sum = 0.0
        wins = 0
        losses = 0

        for tip in tips:
            result, odd_entry, label, market = tip["result"], tip["odd_entry"], tip["label"], tip["market"]
            if result in ("win", "green") and odd_entry:
                lucro += (odd_entry - 1) * STAKE
                odds_sum += odd_entry
                wins += 1
            elif result in ("loss", "red"):
                lucro -= STAKE
                odds_sum += (odd_entry or 0)
                losses += 1

        settled = wins + losses

        # Only send if lucro > €25 (unless force_send)
        if not force_send and lucro <= 25:
            log.info(f"_send_daily_summary: lucro €{lucro:.2f} below threshold (€25) for {days_back} day(s) ago (use force_send=True to override)")
            return

        # Calculate average odds and ROI
        avg_odds = odds_sum / settled if settled > 0 else 0.0
        roi = (lucro / (settled * STAKE) * 100) if settled > 0 else 0.0

        # Find the bet with the highest odd, with match info
        # NOTE: tips are sqlite3.Row objects — no .get(), use indexing with try/except
        max_odd_tip = max(tips, key=lambda t: t["odd_entry"] or 0)
        def _row_get(row, key, default=""):
            try:
                v = row[key]
                return v if v is not None else default
            except (IndexError, KeyError):
                return default
        maior_odd  = max_odd_tip["odd_entry"]
        bet_label  = _row_get(max_odd_tip, 'label', '?')
        bet_market = _row_get(max_odd_tip, 'market', '?')
        home_team  = _row_get(max_odd_tip, 'home_team', '')
        away_team  = _row_get(max_odd_tip, 'away_team', '')

        # Build match string
        if home_team and away_team:
            match_str = f"{home_team} vs {away_team}"
        else:
            match_str = "Match desconhecido"

        bet_with_maior_odd = f"{bet_label} ({bet_market})"

        # Format the message with match context and encouragement
        date_str = target_start.strftime("%d/%m/%Y")
        day_label = "Ontem" if days_back == 1 else ("Hoje" if days_back == 0 else f"{days_back}d atrás")

        msg_lines = [
            f"<b>Resumo Diário — {day_label} ({date_str})</b>",
            "",
            f"💶 <b>Lucro:</b> €{lucro:.2f}",
            f"📊 <b>Odds Médias:</b> {avg_odds:.2f}",
            f"📈 <b>ROI:</b> {roi:.1f}%",
            "",
            f"🎯 <b>Maior Odd do Dia:</b> {maior_odd:.2f}",
            f"   <i>{bet_with_maior_odd}</i>",
            f"   <i>{match_str}</i>",
            "",
            f"<i>Mantém a vigilância nas entradas de amanhã — o edge está lá! 🚀</i>",
        ]

        msg = "\n".join(msg_lines)

        log.info(f"_send_daily_summary: sending message for {day_label} ({date_str}), lucro €{lucro:.2f}")
        _send_telegram(msg)

    except Exception as e:
        log.error(f"_send_daily_summary error: {e}", exc_info=True)


def _generate_chart(period: str = "alltime") -> bytes | None:
    """
    Generate professional P&L chart for the given period.
      - "alltime": daily aggregation since first pick
      - "month":   day-of-month aggregation
      - "7d":      day-by-day for the last 7 days
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
        import io
        from datetime import datetime, timezone, timedelta

        now    = datetime.now(timezone.utc)
        STAKE  = get_setting("stake_per_bet", 100.0)
        cutoff = _period_start_ts(period)

        where  = "WHERE result IS NOT NULL"
        params: tuple = ()
        if cutoff is not None:
            where += " AND wall_ts >= ?"
            params = (cutoff,)

        with _db() as conn:
            tips = conn.execute(
                f"SELECT result, odd_entry, wall_ts FROM tips {where} ORDER BY wall_ts ASC",
                params
            ).fetchall()

        if not tips:
            return None

        # Date-key formatting per period
        if period == "month":
            keyfmt = "%-d"            # day-of-month
        elif period == "7d":
            keyfmt = "%a %d"          # "Mon 05"
        else:
            keyfmt = "%b %-d"         # "Apr 28" — alltime can span months

        daily: dict[str, float] = {}
        order_keys: list[str] = []     # preserve chronological order
        for tip in tips:
            result, odd_entry, wall_ts = tip["result"], tip["odd_entry"], tip["wall_ts"]
            day = datetime.fromtimestamp(wall_ts, tz=timezone.utc).strftime(keyfmt)
            if day not in daily:
                order_keys.append(day)
                daily[day] = 0.0
            if result in ("win", "green") and odd_entry:
                daily[day] += (odd_entry - 1) * STAKE
            elif result in ("loss", "red"):
                daily[day] -= STAKE

        if not daily:
            return None

        days   = order_keys
        values = [daily[d] for d in days]
        cumul, total = [], 0.0
        for v in values:
            total += v
            cumul.append(round(total, 2))

        # Professional dark palette
        BG, PANEL = "#0a0e27", "#1a1f3a"
        GREEN, RED = "#10b981", "#ef5350"
        GRAY, WHITE = "#4b5563", "#e8f0f7"

        colors    = [GREEN if v >= 0 else RED for v in values]
        cum_color = GREEN if cumul[-1] >= 0 else RED
        period_label = {
            "alltime": "Geral",
            "month":   now.strftime("%B %Y"),
            "7d":      "Últimos 7 dias",
        }.get(period, "Geral")

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), facecolor=BG)
        fig.suptitle(f"🤖 BetRadar AI — P&L {period_label}",
                     color=WHITE, fontsize=16, fontweight="bold", y=0.98)

        # ── Top: daily bars ──
        ax1.set_facecolor(PANEL)
        bars = ax1.bar(days, values, color=colors, alpha=0.85, edgecolor=WHITE, linewidth=0.5, zorder=2)
        ax1.axhline(0, color=GRAY, linewidth=1, zorder=1, linestyle="-", alpha=0.6)
        ax1.grid(axis="y", color=GRAY, alpha=0.15, linestyle="--", linewidth=0.5, zorder=0)
        ax1.set_title("Daily P&L (€ per day)", color=WHITE, fontsize=11, fontweight="600", pad=10)
        ax1.tick_params(colors=WHITE, labelsize=9)
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"€{x:+.0f}"))
        ax1.yaxis.set_label_position("right")
        ax1.yaxis.tick_right()
        # Rotate x labels when there are many days (alltime / wide-period)
        if len(days) > 12:
            for lbl in ax1.get_xticklabels():
                lbl.set_rotation(45); lbl.set_ha("right")

        for spine in ax1.spines.values():
            spine.set_color(GRAY)
            spine.set_linewidth(0.8)
        ax1.spines["top"].set_visible(False)
        ax1.spines["left"].set_visible(False)
        ax1.set_xlim(-0.6, len(days) - 0.4)

        # ── Bottom: cumulative line ──
        xs = range(len(days))
        ax2.set_facecolor(PANEL)
        ax2.fill_between(xs, cumul, 0, alpha=0.15, color=cum_color, zorder=2)
        ax2.plot(xs, cumul, color=cum_color, linewidth=2.5, marker="o",
                 markersize=5, zorder=3, markerfacecolor=cum_color, markeredgecolor=WHITE, markeredgewidth=1)
        ax2.axhline(0, color=GRAY, linewidth=1, zorder=1, linestyle="-", alpha=0.6)
        ax2.grid(axis="y", color=GRAY, alpha=0.15, linestyle="--", linewidth=0.5, zorder=0)
        ax2.set_title("Cumulative P&L (€ running total)", color=WHITE, fontsize=11, fontweight="600", pad=10)
        ax2.tick_params(colors=WHITE, labelsize=9)
        ax2.set_xticks(list(xs))
        ax2.set_xticklabels(days)
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"€{x:+.0f}"))
        ax2.yaxis.set_label_position("right")
        ax2.yaxis.tick_right()
        if len(days) > 12:
            for lbl in ax2.get_xticklabels():
                lbl.set_rotation(45); lbl.set_ha("right")

        for spine in ax2.spines.values():
            spine.set_color(GRAY)
            spine.set_linewidth(0.8)
        ax2.spines["top"].set_visible(False)
        ax2.spines["left"].set_visible(False)
        ax2.set_xlim(-0.4, len(days) - 0.6)

        # Total P&L annotation with better styling
        final_value = cumul[-1]
        ax2.annotate(f"Total: €{final_value:+.2f}",
                     xy=(len(days) - 1, final_value),
                     xytext=(-15, 15), textcoords="offset points",
                     color=cum_color, fontsize=11, fontweight="bold",
                     ha="right",
                     bbox=dict(boxstyle="round,pad=0.5", facecolor=PANEL, edgecolor=cum_color,
                               linewidth=1.5, alpha=0.9),
                     arrowprops=dict(arrowstyle="->", color=cum_color, lw=1, alpha=0.7))

        # Stake info at the bottom
        fig.text(0.5, 0.01, f"Stake: €{STAKE:.0f}/pick  •  Total picks: {len(tips)}  •  Win rate: {sum(1 for v in values if v > 0)}/{len(days)} days",
                 ha="center", color=GRAY, fontsize=9, style="italic")

        plt.tight_layout(rect=[0, 0.03, 1, 0.96])
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, facecolor=BG, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        log.error(f"_generate_chart({period}) error: {e}")
        return None


def _generate_monthly_chart() -> bytes | None:
    """Back-compat shim — prefer _generate_chart('month') directly."""
    return _generate_chart("month")


def _send_telegram_photo(chat_id: int, photo_bytes: bytes,
                          caption: str = "", buttons: list | None = None):
    """Envia foto (PNG) via Telegram sendPhoto com multipart/form-data."""
    if not TELEGRAM_BOT_TOKEN:
        return
    import urllib.request as _urllib
    try:
        boundary = "TGBotBoundary7x3k"
        CRLF = b"\r\n"

        def field(name: str, value: str) -> bytes:
            return (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}"
            ).encode() + CRLF

        body = (
            field("chat_id", str(chat_id))
            + field("parse_mode", "HTML")
        )
        if caption:
            body += field("caption", caption)
        if buttons:
            body += field("reply_markup", json.dumps({"inline_keyboard": buttons}))
        # Photo part
        body += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="photo"; filename="chart.png"\r\n'
            f"Content-Type: image/png\r\n\r\n"
        ).encode() + photo_bytes + CRLF
        body += f"--{boundary}--\r\n".encode()

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        req = _urllib.Request(url, data=body,
                              headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        _urllib.urlopen(req, timeout=30)
    except Exception as e:
        log.error(f"Telegram send_photo failed to {chat_id}: {e}")


def _get_live_grouped() -> str:
    """Live games grouped by tournament, with clickable links for games with picks."""
    try:
        with _state_lock:
            by_tourn: dict[str, list] = {}
            for entry in _live_state.values():
                m = entry.get("match", {})
                if m.get("statusType") != "inprogress":
                    continue
                flag   = _country_flag(m.get("country", ""))
                tourn  = m.get("tournament", "Desconhecida")
                home   = m.get("homeTeam", "")
                away   = m.get("awayTeam", "")
                hg     = m.get("homeGoals", 0) or 0
                ag     = m.get("awayGoals", 0) or 0
                mins   = m.get("currentMinute") or m.get("statusTime") or "?"
                n_tips = len(entry.get("tips", []))
                match_id = m.get("id")

                # Format score with better readability
                score = f"<b>{hg}–{ag}</b>"
                time_str = f"{mins}'" if mins != "?" else "?"

                # Build game line with optional link
                if n_tips > 0 and match_id:
                    # Game with picks → add clickable link to webpronos
                    home_slug = _slug(home)
                    away_slug = _slug(away)
                    url = f"https://webpronos.com/match/{match_id}/{home_slug}-{away_slug}"
                    pick_icon = "🎯" if n_tips == 1 else f"🎯×{n_tips}"
                    line = f"  <a href=\"{url}\">⚽ {home} {score} {away}</a> | {time_str} | {pick_icon}"
                else:
                    # Game without picks → plain text
                    line = f"  ⚽ {home} {score} {away} | {time_str}"

                key = f"{flag} <b>{tourn}</b>"
                by_tourn.setdefault(key, []).append(line)

        if not by_tourn:
            return "Sem jogos live de momento 😴"

        parts = []
        for tourn_header, matches in by_tourn.items():
            parts.append(tourn_header)
            parts.extend(matches)
            parts.append("")  # blank line between tournaments
        return "\n".join(parts).rstrip()
    except Exception as e:
        log.error(f"Error in _get_live_grouped: {e}")
        return "Erro ao obter jogos live."

def _get_live_summary() -> str:
    """Short (max 6 lines) summary of in-progress games for the welcome message."""
    try:
        with _state_lock:
            lines = []
            for entry in _live_state.values():
                m = entry.get("match", {})
                if m.get("statusType") == "inprogress":
                    flag = _country_flag(m.get("country", ""))
                    home = m.get("homeTeam", "")
                    away = m.get("awayTeam", "")
                    hg   = m.get("homeGoals", 0) or 0
                    ag   = m.get("awayGoals", 0) or 0
                    mins = m.get("currentMinute") or m.get("statusTime") or "?"
                    n_tips = len(entry.get("tips", []))
                    tip_badge = f" 🎯" if n_tips > 0 else ""
                    lines.append(f"  {flag} {home} <b>{hg}–{ag}</b> {away} <i>({mins}')</i>{tip_badge}")
        if not lines:
            return "Sem jogos live de momento 😴"
        shown = lines[:6]
        if len(lines) > 6:
            shown.append(f"  <i>... e mais {len(lines) - 6} jogos</i>")
        return "\n".join(shown)
    except Exception:
        return "Erro ao obter jogos live."

def _build_main_menu() -> list:
    """Inline keyboard for the main menu."""
    return [
        [
            {"text": "📊 Resultados",         "callback_data": "cb_stats"},
            {"text": "🔴 Live Agora",         "callback_data": "cb_live"},
        ],
        [
            {"text": "📐 Como Funciona",      "callback_data": "cb_howto"},
            {"text": "🛑 Cancelar Picks",     "callback_data": "cb_stop"},
        ],
    ]


def _build_stats_period_menu(active: str = "alltime") -> list:
    """Inline keyboard with period toggles + main-menu shortcut."""
    def b(period: str) -> dict:
        label, _ = _PERIOD_LABEL[period]
        # Active period gets a checkmark prefix to make selection obvious
        prefix = "✅ " if period == active else ""
        return {"text": f"{prefix}{label}", "callback_data": f"cb_stats_{period}"}
    return [
        [b("alltime"), b("month"), b("7d")],
        [{"text": "↩️ Menu", "callback_data": "cb_menu"}],
    ]

def _format_pick_alert(match: dict, pick: dict, minute, shots: dict = None) -> str:
    """Build the Telegram message for a new pick."""
    flag        = _country_flag(match.get("country", ""))
    tournament  = match.get("tournament", "")
    home        = match.get("homeTeam", "Casa")
    away        = match.get("awayTeam", "Fora")
    hg          = match.get("homeGoals", 0)
    ag          = match.get("awayGoals", 0)
    home_xg     = (shots or {}).get("homeXg", 0)
    away_xg     = (shots or {}).get("awayXg", 0)
    market      = pick.get("market", "")
    label       = pick.get("label", "")
    odds        = pick.get("odds") or 0
    edge        = pick.get("edge") or 0
    model_p     = (pick.get("model") or 0) * 100
    market_p    = (1 / odds * 100) if odds > 0 else 0

    market_icons = {"1X2": "🎯", "Handicap": "⚖️"}
    mkt_icon = market_icons.get(market, "📊")

    stake = get_setting("stake_per_bet", 100.0)
    cta = _next_cta(odds=odds, stake=stake)
    return (
        f"🔔 <b>NOVA PICK</b>\n"
        f"\n"
        f"{flag} <b>{tournament}</b>\n"
        f"⚽ {home} {hg}–{ag} {away}\n"
        f"📊 xG: <b>{home_xg:.2f}</b> — <b>{away_xg:.2f}</b>\n"
        f"⏱ {minute}' em jogo\n"
        f"\n"
        f"{mkt_icon} Mercado: <b>{market} → {label}</b>\n"
        f"💰 Odds: <b>{odds:.2f}</b>\n"
        f"\n"
        f"📊 Modelo: <b>{model_p:.0f}%</b> | Mercado: <b>{market_p:.0f}%</b>\n"
        f"📈 Edge: <b>+{edge:.1f}%</b>\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{cta}"
    )

def _get_banner_stats() -> dict:
    """
    Smart stat picker for the welcome banner — NEVER shows losses.
    Priority:
      1. Today's P&L   → show if positive
      2. Best win today → show if any win exists today
      3. Week P&L       → show if positive
      4. All-time P&L   → show if positive
      5. Neutral fallback: algorithm active
    Returns dict: {label, value, sub, emoji}
    """
    try:
        STAKE   = get_setting("stake_per_bet", 100.0)
        now_ts  = int(time.time())
        from datetime import datetime, timezone, timedelta
        now_utc = datetime.now(timezone.utc)
        day_start  = int(datetime(now_utc.year, now_utc.month, now_utc.day,
                                  tzinfo=timezone.utc).timestamp())
        week_start = int((now_utc - timedelta(days=7)).timestamp())

        with _db() as conn:
            # ── Today's picks ────────────────────────────────────────────────
            today_tips = conn.execute(
                "SELECT result, odd_entry FROM tips WHERE wall_ts >= ? AND result IS NOT NULL",
                (day_start,)
            ).fetchall()

            today_pnl  = 0.0
            today_wins = 0
            best_odd_today = 0.0
            for t in today_tips:
                if t["result"] in ("win", "green") and t["odd_entry"]:
                    today_pnl += (t["odd_entry"] - 1) * STAKE
                    today_wins += 1
                    best_odd_today = max(best_odd_today, t["odd_entry"])
                elif t["result"] in ("loss", "red"):
                    today_pnl -= STAKE

            if today_pnl > 0:
                return {
                    "label": "Lucro Hoje",
                    "value": f"+€{today_pnl:.0f}",
                    "sub":   f"{today_wins} pick{'s' if today_wins != 1 else ''} verde{'s' if today_wins != 1 else ''}",
                    "emoji": "💰",
                    "color": "green",
                }

            if best_odd_today > 0:
                return {
                    "label": "Melhor Pick Hoje",
                    "value": f"@{best_odd_today:.2f}",
                    "sub":   "Odd ganha hoje ✅",
                    "emoji": "🏆",
                    "color": "green",
                }

            # ── Week ─────────────────────────────────────────────────────────
            week_tips = conn.execute(
                "SELECT result, odd_entry FROM tips WHERE wall_ts >= ? AND result IS NOT NULL",
                (week_start,)
            ).fetchall()
            week_pnl  = 0.0
            week_wins = 0
            for t in week_tips:
                if t["result"] in ("win", "green") and t["odd_entry"]:
                    week_pnl += (t["odd_entry"] - 1) * STAKE
                    week_wins += 1
                elif t["result"] in ("loss", "red"):
                    week_pnl -= STAKE
            if week_pnl > 0:
                return {
                    "label": "Lucro 7 Dias",
                    "value": f"+€{week_pnl:.0f}",
                    "sub":   f"{week_wins} pick{'s' if week_wins != 1 else ''} verde{'s' if week_wins != 1 else ''}",
                    "emoji": "📈",
                    "color": "green",
                }

            # ── All-time ─────────────────────────────────────────────────────
            all_tips = conn.execute(
                "SELECT result, odd_entry FROM tips WHERE result IS NOT NULL"
            ).fetchall()
            all_pnl  = 0.0
            all_wins = 0
            for t in all_tips:
                if t["result"] in ("win", "green") and t["odd_entry"]:
                    all_pnl += (t["odd_entry"] - 1) * STAKE
                    all_wins += 1
                elif t["result"] in ("loss", "red"):
                    all_pnl -= STAKE
            if all_pnl > 0:
                return {
                    "label": "Lucro Total",
                    "value": f"+€{all_pnl:.0f}",
                    "sub":   f"{all_wins} picks vencedoras",
                    "emoji": "📊",
                    "color": "green",
                }
    except Exception:
        pass

    return {
        "label": "Algoritmo Ativo",
        "value": "LIVE",
        "sub":   "Picks em análise • Aguarda alertas",
        "emoji": "📡",
        "color": "neutral",
    }


def _build_welcome_banner() -> bytes:
    """
    Gera banner PNG 1080x600 para a welcome message do /start.
    Design: logo esquerda | divisoria verde | stats + tagline direita.
    Requires: Pillow (pip install Pillow)
    Logo: static/logo.png (fundo transparente ou preto)
    Fonts: DejaVu (Docker/Fly.io) or Arial (macOS fallback)
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io, os

        W, H = 1080, 600

        # ── Paleta ───────────────────────────────────────────────────────────
        GREEN_NEON = (57, 255, 20)     # verde neon (#39FF14)
        GREEN_DIM  = (16, 185, 129)    # emerald (#10b981)
        WHITE      = (255, 255, 255)
        GRAY       = (190, 190, 190)
        GRAY_DIM   = (70, 70, 70)
        DARK_CARD  = (8, 14, 10)

        # ── Gradient background ───────────────────────────────────────────────
        # Diagonal gradient: top-right corner glows dark emerald, rest near-black
        # Use a small 3×2 seed image scaled with bilinear for smooth result
        seed = Image.new("RGB", (3, 2))
        seed.putdata([
            (6,  12,  8),   # top-left:    very dark
            (7,  14,  9),   # top-center:  very dark
            (14, 44, 22),   # top-right:   dark emerald glow
            (5,   8,  6),   # bottom-left: darkest
            (5,   9,  6),   # bottom-center
            (9,  24, 12),   # bottom-right: faint green
        ])
        img  = seed.resize((W, H), Image.BILINEAR)
        draw = ImageDraw.Draw(img)

        # ── Subtle diagonal line texture (tech/data feel) ─────────────────────
        for x_off in range(-H, W + H, 70):
            draw.line([(x_off, 0), (x_off + H, H)], fill=(14, 26, 17), width=1)

        # ── Dot grid ─────────────────────────────────────────────────────────
        for gy in range(0, H, 30):
            for gx in range(0, W, 30):
                draw.ellipse([(gx - 1, gy - 1), (gx + 1, gy + 1)],
                             fill=(18, 34, 22))

        # ── Neon border (top + bottom edge lines) ────────────────────────────
        draw.line([(0, 0), (W, 0)],     fill=GREEN_NEON, width=3)
        draw.line([(0, 2), (W, 2)],     fill=(24, 110, 45), width=1)
        draw.line([(0, 3), (W, 3)],     fill=(10, 50, 20),  width=1)
        draw.line([(0, H - 1), (W, H - 1)], fill=GREEN_NEON,     width=2)
        draw.line([(0, H - 3), (W, H - 3)], fill=(10, 50, 20),   width=1)

        # ── Fonts (DejaVu no Docker; Arial no macOS) ─────────────────────────
        FONT_DIRS = [
            "/usr/share/fonts/truetype/dejavu",   # Fly.io / Debian
            "/usr/share/fonts/dejavu",
            "/usr/share/fonts/truetype",
            "/System/Library/Fonts/Supplemental", # macOS
            "/Library/Fonts",
        ]
        def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
            candidates = (
                ["DejaVuSans-Bold.ttf", "Arial Bold.ttf", "Arial Black.ttf"]
                if bold else
                ["DejaVuSans.ttf", "Arial.ttf"]
            )
            for d in FONT_DIRS:
                for n in candidates:
                    p = os.path.join(d, n)
                    if os.path.exists(p):
                        return ImageFont.truetype(p, size)
            try:
                return ImageFont.load_default(size=size)
            except TypeError:
                return ImageFont.load_default()

        fnt_eyebrow = _load_font(22)
        fnt_tag     = _load_font(21)
        fnt_tiny    = _load_font(18)
        fnt_sub     = _load_font(19)
        fnt_label   = _load_font(19)
        fnt_large   = _load_font(52, bold=True)
        fnt_xlarge  = _load_font(82, bold=True)

        # ── Logo ─────────────────────────────────────────────────────────────
        logo_col_w = 390
        BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
        LOGO_PATH  = os.path.join(BASE_DIR, "static", "logo.png")
        if os.path.exists(LOGO_PATH):
            try:
                logo = Image.open(LOGO_PATH).convert("RGBA")
                logo.thumbnail((340, 400), Image.LANCZOS)
                lx = (logo_col_w - logo.width) // 2
                ly = (H - logo.height) // 2
                bg_p = Image.new("RGBA", img.size, (0, 0, 0, 0))
                bg_p.paste(logo, (lx, ly))
                img  = Image.alpha_composite(img.convert("RGBA"), bg_p).convert("RGB")
                draw = ImageDraw.Draw(img)
            except Exception:
                pass

        # ── Divisoria ────────────────────────────────────────────────────────
        div_x = logo_col_w + 8
        draw.line([(div_x - 1, 40), (div_x - 1, H - 40)], fill=(10, 80, 60),   width=1)
        draw.line([(div_x,     40), (div_x,     H - 40)], fill=GREEN_DIM,       width=1)
        draw.line([(div_x + 1, 40), (div_x + 1, H - 40)], fill=(10, 80, 60),   width=1)

        # ── Coluna direita ───────────────────────────────────────────────────
        rx = div_x + 50
        cy = 52
        rw = W - rx - 36

        # Eyebrow: "* BetRadar AI" (sem emoji — DejaVu nao suporta bem)
        dot_r = 5
        draw.ellipse([(rx, cy + 8), (rx + dot_r * 2, cy + 8 + dot_r * 2)],
                     fill=GREEN_DIM)
        draw.text((rx + dot_r * 2 + 10, cy), "BetRadar AI", font=fnt_eyebrow, fill=GREEN_DIM)
        cy += 46

        # Titulo
        draw.text((rx, cy), "LIVE AI", font=fnt_xlarge, fill=WHITE)
        cy += 88
        draw.text((rx, cy), "GENERATED PICKS", font=fnt_large, fill=GREEN_NEON)
        cy += 66

        # Subtitulo
        draw.text((rx, cy), "Real-time football value bets powered by xG model",
                  font=fnt_tag, fill=GRAY)
        cy += 40

        # Separador
        draw.line([(rx, cy), (rx + rw, cy)], fill=GRAY_DIM, width=1)
        cy += 22

        # ── Stats card ───────────────────────────────────────────────────────
        stat   = _get_banner_stats()
        card_h = 134
        card_r = [rx, cy, rx + rw, cy + card_h]
        draw.rounded_rectangle(card_r, radius=14, fill=DARK_CARD)
        draw.rounded_rectangle(card_r, radius=14, outline=GREEN_DIM, width=1)

        # Indicador verde (bola) + label
        sx, sy = rx + 24, cy + 14
        draw.ellipse([(sx, sy + 4), (sx + 8, sy + 12)], fill=GREEN_NEON)
        draw.text((sx + 16, sy), stat["label"].upper(), font=fnt_label, fill=GRAY)
        sy += 32

        # Valor principal
        val_color = GREEN_NEON if stat["color"] == "green" else GRAY
        draw.text((sx, sy), stat["value"], font=fnt_large, fill=val_color)
        sy += 58

        # Sub-texto
        draw.text((sx, sy), stat["sub"], font=fnt_sub, fill=GRAY)

        cy += card_h + 18

        # Rodape
        draw.text((rx, cy), "t.me/BetRadarAI_bot",
                  font=fnt_tiny, fill=GRAY_DIM)

        # ── Export ────────────────────────────────────────────────────────────
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()

    except Exception as e:
        log.warning(f"_build_welcome_banner failed: {e}")
        return b""  # fallback silencioso — welcome text enviado na mesma


def _build_welcome(first_name: str | None = None) -> str:
    """Personalised welcome — BetRadar AI brand with all-time engagement stats."""
    name_part = f", {first_name}" if first_name else ""
    stats = _get_alltime_stats()
    live  = _get_live_summary()

    # ── Performance block (all-time, high engagement) ─────────────────────────
    if stats["settled"] > 0:
        sign = "+" if stats["pnl_eur"] >= 0 else ""
        roi_sign = "+" if stats["roi"] >= 0 else ""
        roi_emoji = "🟢" if stats["roi"] >= 0 else "🔴"

        perf_lines = [
            f"💰 <b>Lucro total:</b> {sign}{stats['pnl_eur']:.0f}€  ({stats['settled']} picks)",
            f"{roi_emoji} <b>ROI:</b> {roi_sign}{stats['roi']:.1f}%  ·  <b>Win rate:</b> {stats['winrate']:.1f}%",
            f"📊 <b>Odds médias:</b> {stats['avg_odds']:.2f}  ·  <b>Maior streak:</b> {stats['best_streak']} 🔥",
            f"🏆 <b>Maior win:</b> +{stats['best_win_pnl']:.0f}€",
        ]
        perf_block = "\n".join(perf_lines)
    else:
        perf_block = "💰 <i>O algoritmo ainda está a aquecer — picks resolvidas em breve!</i>"

    # ── Live block ────────────────────────────────────────────────────────────
    live_block = live if live else "<i>Sem jogos live de momento 😴</i>"

    return (
        f"🤖 <b>BetRadar AI</b> — bem-vindo{name_part}! 🎯\n"
        f"\n"
        f"O bot inteligente que deteta <b>value bets</b> em tempo real durante "
        f"jogos de futebol, usando o modelo de <b>Expected Goals (xG)</b> "
        f"combinado com odds ao vivo das melhores casas. 100% gratuito. ⚡\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>Resultados Globais</b>\n"
        f"{perf_block}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔴 <b>Live agora</b>\n"
        f"{live_block}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💡 <b>Como funciona?</b>\n"
        f"A cada pick, o BetRadar AI compara a probabilidade real do modelo "
        f"com as odds da casa. Quando há <b>edge ≥ 5%</b>, recebes alerta. "
        f"Não inventamos — só te dizemos quando a matemática está do nosso lado.\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Usa o menu abaixo para começar 👇"
    )

# ════════════════════════════════════════════════════════════
#  SERVER-SENT EVENTS (SSE) — Real-time picks stream
# ════════════════════════════════════════════════════════════

from queue import Queue, Empty
import threading as _threading

_sse_clients: set[Queue] = set()
_sse_lock = _threading.Lock()

def _broadcast_pick(match: dict, pick: dict, minute: int | None):
    """Broadcast a new pick to all SSE clients."""
    try:
        flag = _country_flag(match.get("country", ""))
        home = match.get("homeTeam", "")
        away = match.get("awayTeam", "")
        tournament = match.get("tournament", "")
        market = pick.get("market", "")
        label = pick.get("label", "")
        odds = pick.get("odds") or 0
        edge = pick.get("edge") or 0
        model_p = (pick.get("model") or 0) * 100
        market_p = (1 / odds * 100) if odds > 0 else 0
        match_id = match.get("id") or match.get("match_id")

        msg = json.dumps({
            "type": "new_pick",
            "match_id": match_id,
            "flag": flag,
            "tournament": tournament,
            "home": home,
            "away": away,
            "minute": minute,
            "market": market,
            "label": label,
            "odds": round(odds, 2),
            "edge": round(edge, 1),
            "model_p": round(model_p, 0),
            "market_p": round(market_p, 0),
            "timestamp": int(time.time()),
        })

        with _sse_lock:
            dead_clients = []
            for client_q in _sse_clients:
                try:
                    client_q.put_nowait(msg)
                except Exception:
                    dead_clients.append(client_q)
            for client in dead_clients:
                _sse_clients.discard(client)
    except Exception as e:
        log.error(f"_broadcast_pick error: {e}")


def _handle_tg_callback(callback_id: str, chat_id: int, data_str: str,
                         username: str | None, first_name: str | None):
    """Dispatch an inline button callback."""
    _tg_answer_callback(callback_id)  # clear the spinner

    if data_str == "cb_stats" or data_str.startswith("cb_stats_"):
        # Default view = all-time. Sub-callbacks override the period.
        period = "alltime"
        if data_str.startswith("cb_stats_"):
            requested = data_str.replace("cb_stats_", "", 1)
            if requested in _PERIOD_LABEL:
                period = requested

        _, period_title = _PERIOD_LABEL[period]
        stake = get_setting("stake_per_bet", 100.0)
        ps    = _get_period_stats(period)
        big   = _get_biggest_green(period)

        if ps["settled"] == 0:
            caption = (
                f"📊 <b>Resultados — {period_title}</b>\n\n"
                f"<i>Ainda sem picks resolvidas neste período.</i>\n\n"
                f"Escolhe outro intervalo abaixo 👇"
            )
            _send_telegram_buttons(caption, chat_id, _build_stats_period_menu(period))
            return

        sign     = "+" if ps["pnl_eur"] >= 0 else ""
        roi_sign = "+" if ps["roi"]     >= 0 else ""
        roi_emoji = "🟢" if ps["roi"] >= 0 else "🔴"

        # Top markets
        top_mkts = sorted(ps["markets"].items(), key=lambda x: -x[1])[:4]
        mkt_lines = "\n".join(
            f"  • <b>{m}</b>: {c} pick{'s' if c != 1 else ''}"
            for m, c in top_mkts
        )

        # Biggest green hook
        if big:
            big_block = (
                f"🏆 <b>Maior green</b> ({period_title.lower()}):\n"
                f"  • {big['match']}\n"
                f"  • <b>{big['market']}</b> @ <b>{big['odd']:.2f}</b>  →  <b>+{big['profit']:.0f}€</b>"
            )
        else:
            big_block = "🏆 <i>Sem greens neste período… o próximo está a caminho! 🚀</i>"

        caption = (
            f"📊 <b>Resultados — {period_title}</b>\n"
            f"\n"
            f"💰 <b>Lucro:</b> {sign}{ps['pnl_eur']:.0f}€  <i>(€{stake:.0f}/pick)</i>\n"
            f"{roi_emoji} <b>ROI:</b> {roi_sign}{ps['roi']:.1f}%  ·  "
            f"<b>Win rate:</b> {ps['winrate']:.1f}%\n"
            f"🎯 <b>Picks:</b> {ps['settled']}  "
            f"(<b>{ps['wins']}</b>W / <b>{ps['losses']}</b>L)\n"
            f"📈 <b>Odds médias:</b> {ps['avg_odds']:.2f}  "
            f"·  <b>Streak:</b> {ps['best_streak']} 🔥\n"
            f"\n"
            f"{big_block}\n"
        )
        if mkt_lines:
            caption += f"\n<b>Top mercados:</b>\n{mkt_lines}"

        chart = _generate_chart(period)
        if chart:
            _send_telegram_photo(chat_id, chart, caption=caption,
                                  buttons=_build_stats_period_menu(period))
        else:
            _send_telegram_buttons(caption, chat_id, _build_stats_period_menu(period))

    elif data_str == "cb_menu":
        _send_telegram_buttons("Menu principal 👇", chat_id, _build_main_menu())

    elif data_str == "cb_live":
        live = _get_live_grouped()
        reply = f"🔴 <b>Live Agora</b>\n\n{live}"
        _send_telegram_buttons(reply, chat_id, _build_main_menu())

    elif data_str == "cb_howto":
        reply = (
            "📐 <b>Como funciona o modelo?</b>\n\n"
            "Enquanto o jogo decorre, o modelo analisa a cada minuto:\n"
            "• O xG acumulado de cada equipa\n"
            "• As odds ao vivo das principais casas\n"
            "• A probabilidade calculada pelo modelo Benter\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📌 <b>Onde está a vantagem?</b>\n"
            "As bookmakers são lentas a ajustar odds ao fluxo real do jogo. "
            "Quando a probabilidade do modelo diverge significativamente das odds, "
            "esse <b>edge</b> é a nossa vantagem.\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>Filtros de qualidade</b>\n"
            "• ⏱ Jogo com mais de 25 minutos\n"
            "• 🚫 Sem golo nos últimos 4 minutos\n"
            "• 📈 Edge positivo e significativo\n"
            "• 💰 Odds entre 1.40 e 4.00"
        )
        _send_telegram_buttons(reply, chat_id, _build_main_menu())

    elif data_str == "cb_stop":
        _tg_unsubscribe(chat_id)
        _send_telegram(
            "🔕 <b>Notificações canceladas.</b>\n\nJá não receberás mais picks.\n"
            "Para voltar envia /start.",
            chat_id=chat_id
        )


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    """Handle incoming Telegram messages and callback queries."""
    data = flask_request.get_json(silent=True) or {}

    # ── Inline button callback ──
    cb = data.get("callback_query")
    if cb:
        cb_id      = cb.get("id", "")
        cb_data    = cb.get("data", "")
        cb_chat_id = (cb.get("message") or {}).get("chat", {}).get("id") or \
                     (cb.get("from") or {}).get("id")
        cb_user    = (cb.get("from") or {})
        if cb_chat_id:
            _handle_tg_callback(cb_id, cb_chat_id, cb_data,
                                 cb_user.get("username"), cb_user.get("first_name"))
        return "", 200

    # ── Normal message ──
    msg        = data.get("message") or data.get("edited_message") or {}
    text       = (msg.get("text") or "").strip()
    chat_id    = (msg.get("chat") or {}).get("id")
    username   = (msg.get("from") or {}).get("username")
    first_name = (msg.get("from") or {}).get("first_name")

    if not chat_id:
        return "", 200

    log.info(f"Telegram: chat_id={chat_id} username={username}: {text[:60]}")

    if text.startswith("/start"):
        # Parse deep-link payload: "/start instagram_oct" → "instagram_oct"
        parts = text.split(maxsplit=1)
        start_param = parts[1].strip() if len(parts) > 1 else None
        _tg_log_start(chat_id, username, first_name, start_param)
        _tg_subscribe(chat_id, username=username, first_name=first_name, source=start_param)
        # Send engaging banner first, then welcome text + menu
        banner = _build_welcome_banner()
        if banner:
            _send_telegram_photo(chat_id, banner)
        _send_telegram_buttons(_build_welcome(first_name), chat_id, _build_main_menu())

    elif text.startswith("/whoami"):
        admin_tag = "  ✅ <i>(admin)</i>" if _is_tg_admin(chat_id) else ""
        _send_telegram(
            f"🆔 <b>Your IDs</b>\n\n"
            f"chat_id: <code>{chat_id}</code>{admin_tag}\n"
            f"username: @{username or '—'}\n"
            f"first_name: {first_name or '—'}",
            chat_id=chat_id
        )

    elif text.startswith("/admin_stats"):
        if _is_tg_admin(chat_id):
            _send_telegram(_tg_admin_stats(), chat_id=chat_id)
        else:
            _send_telegram(
                "🚫 Comando restrito.\n"
                f"Pede ao admin para adicionar <code>{chat_id}</code> a TELEGRAM_ADMIN_CHAT_IDS.",
                chat_id=chat_id
            )

    elif text.startswith("/stop"):
        _tg_unsubscribe(chat_id)
        _send_telegram(
            "🔕 <b>Notificações canceladas.</b>\n\nJá não receberás mais picks.\n"
            "Para voltar envia /start.",
            chat_id=chat_id
        )

    elif text.startswith("/status"):
        subs = _tg_subscribers()
        if str(chat_id) in subs:
            _send_telegram_buttons(
                "✅ Estás inscrito e a receber picks em tempo real.",
                chat_id, _build_main_menu()
            )
        else:
            _send_telegram(
                "⚠️ Não estás inscrito. Envia /start para receber picks.",
                chat_id=chat_id
            )

    elif text.startswith("/stats") or text.startswith("/results"):
        _handle_tg_callback("", chat_id, "cb_stats", username, first_name)

    elif text.startswith("/live"):
        _handle_tg_callback("", chat_id, "cb_live", username, first_name)

    elif text.startswith("/menu"):
        _send_telegram_buttons("Menu principal 👇", chat_id, _build_main_menu())

    return "", 200

# Sofascore tournament name → The Odds API sport key mapping
TOURNAMENT_TO_SPORT_KEY = {
    # England
    "premier league": "soccer_epl",
    "epl": "soccer_epl",
    "fa cup": "soccer_fa_cup",
    "efl cup": "soccer_efl_cup",
    "championship": "soccer_efl_champ",
    "league one": "soccer_england_league1",
    "league two": "soccer_england_league2",
    # Spain
    "laliga": "soccer_spain_la_liga",
    "la liga": "soccer_spain_la_liga",
    "primera division": "soccer_spain_la_liga",
    "segunda": "soccer_spain_segunda_division",
    "segunda división": "soccer_spain_segunda_division",
    "copa del rey": "soccer_spain_copa_del_rey",
    # Italy
    "serie a": "soccer_italy_serie_a",
    "serie b": "soccer_italy_serie_b",
    "coppa italia": "soccer_italy_coppa_italia",
    # Germany
    "bundesliga": "soccer_germany_bundesliga",
    "2. bundesliga": "soccer_germany_bundesliga2",
    "dfb pokal": "soccer_germany_dfb_pokal",
    # France
    "ligue 1": "soccer_france_ligue_one",
    "ligue 2": "soccer_france_ligue_two",
    "coupe de france": "soccer_france_coupe_de_france",
    # Portugal
    "liga portugal": "soccer_portugal_primeira_liga",
    "primeira liga": "soccer_portugal_primeira_liga",
    "liga portugal betclic": "soccer_portugal_primeira_liga",
    "taça de portugal": "soccer_portugal_taca_de_portugal",
    # Netherlands
    "eredivisie": "soccer_netherlands_eredivisie",
    # Belgium
    "jupiler pro league": "soccer_belgium_first_div",
    # Turkey
    "süper lig": "soccer_turkey_super_league",
    "super lig": "soccer_turkey_super_league",
    # UEFA
    "champions league": "soccer_uefa_champs_league",
    "uefa champions league": "soccer_uefa_champs_league",
    "europa league": "soccer_uefa_europa_league",
    "uefa europa league": "soccer_uefa_europa_league",
    "conference league": "soccer_uefa_europa_conference_league",
    "europa conference league": "soccer_uefa_europa_conference_league",
    # Americas
    "mls": "soccer_usa_mls",
    "major league soccer": "soccer_usa_mls",
    "brasileirão": "soccer_brazil_campeonato",
    "brasileirão série a": "soccer_brazil_campeonato",
    "serie a brazil": "soccer_brazil_campeonato",
    "campeonato brasileiro série a": "soccer_brazil_campeonato",
    "liga profesional": "soccer_argentina_primera_division",
    "primera división": "soccer_argentina_primera_division",
    # CONMEBOL
    "copa libertadores": "soccer_conmebol_copa_libertadores",
    "conmebol libertadores": "soccer_conmebol_copa_libertadores",
    "libertadores": "soccer_conmebol_copa_libertadores",
    "copa sudamericana": "soccer_conmebol_copa_sudamericana",
    "conmebol sudamericana": "soccer_conmebol_copa_sudamericana",
    "sudamericana": "soccer_conmebol_copa_sudamericana",
    "recopa sudamericana": "soccer_conmebol_recopa",
    # Asia/Oceania
    "j1 league": "soccer_japan_j_league",
    "k league 1": "soccer_korea_kleague1",
    "a-league": "soccer_australia_aleague",
    # Scandinavia
    "allsvenskan": "soccer_sweden_allsvenskan",
    "eliteserien": "soccer_norway_eliteserien",
    "superligaen": "soccer_denmark_superliga",
    # Other
    "super league": "soccer_switzerland_superleague",
    "swiss super league": "soccer_switzerland_superleague",
    "ekstraklasa": "soccer_poland_ekstraklasa",
    "czech first league": "soccer_czech_republic_league",
    "greek super league": "soccer_greece_super_league",
    "stoiximan super league": "soccer_greece_super_league",  # sponsor name used by Sofascore
    "super league greece": "soccer_greece_super_league",
    # Belgium — "Jupiler" often absent in Sofascore tournament name
    "pro league": "soccer_belgium_first_div",
    "first division a": "soccer_belgium_first_div",
    # Austria
    "austrian bundesliga": "soccer_austria_bundesliga",
    "admiral bundesliga": "soccer_austria_bundesliga",
    "osterreichische bundesliga": "soccer_austria_bundesliga",
    "2. liga austria": "soccer_austria_bundesliga2",
    # Scotland
    "scottish premiership": "soccer_spl",
    "scottish premier league": "soccer_spl",
    # Russia
    "russian premier league": "soccer_russia_premier_league",
    "russia premier league":  "soccer_russia_premier_league",
    "rpl":                    "soccer_russia_premier_league",
    # Brazil Serie B
    "brasileirão série b":              "soccer_brazil_serie_b",
    "serie b brazil":                   "soccer_brazil_serie_b",
    "campeonato brasileiro série b":    "soccer_brazil_serie_b",
    # Chile
    "primera división de chile":        "soccer_chile_campeonato",
    "campeonato nacional":              "soccer_chile_campeonato",
    # League of Ireland
    "league of ireland":                "soccer_league_of_ireland",
    "sse airtricity league":            "soccer_league_of_ireland",
    "airtricity league":                "soccer_league_of_ireland",
    # Saudi Arabia
    "saudi pro league":                 "soccer_saudi_arabia_pro_league",
    "saudi professional league":        "soccer_saudi_arabia_pro_league",
    "roshn saudi league":               "soccer_saudi_arabia_pro_league",
    # UEFA – extra competitions
    "champions league qualification":   "soccer_uefa_champs_league_qualification",
    "ucl qualification":                "soccer_uefa_champs_league_qualification",
    "women's champions league":         "soccer_uefa_champs_league_women",
    "uefa women's champions league":    "soccer_uefa_champs_league_women",
    "european championship":            "soccer_uefa_european_championship",
    "uefa european championship":       "soccer_uefa_european_championship",
    "uefa euro":                        "soccer_uefa_european_championship",
    "euro 2024":                        "soccer_uefa_european_championship",
    "euro qualification":               "soccer_uefa_euro_qualification",
    "euro qualifying":                  "soccer_uefa_euro_qualification",
    "nations league":                   "soccer_uefa_nations_league",
    "uefa nations league":              "soccer_uefa_nations_league",
    # FIFA
    "fifa world cup":                   "soccer_fifa_world_cup",
    "world cup":                        "soccer_fifa_world_cup",
    "world cup qualifiers europe":      "soccer_fifa_world_cup_qualifiers_europe",
    "world cup qualifiers south america": "soccer_fifa_world_cup_qualifiers_south_america",
    "world cup qualifying":             "soccer_fifa_world_cup_qualifiers_europe",
    "women's world cup":                "soccer_fifa_world_cup_womens",
    "fifa women's world cup":           "soccer_fifa_world_cup_womens",
    "club world cup":                   "soccer_fifa_club_world_cup",
    "fifa club world cup":              "soccer_fifa_club_world_cup",
    # Copa America
    "copa america":                     "soccer_conmebol_copa_america",
    "conmebol copa america":            "soccer_conmebol_copa_america",
    # ── Explicit NON-monitored entries — prevent false positives in keyword search ──
    # These map real tournaments to sport keys NOT in MONITORED_SPORT_KEYS
    "fnl":                           "soccer_russia_fnl_na",
    "mozzart bet superliga":         "soccer_serbia_superliga_na",
    "serbian superliga":             "soccer_serbia_superliga_na",
    "superliga serbia":              "soccer_serbia_superliga_na",
    "1. hnl":                        "soccer_croatia_hnl_na",
    "latvian higher league":         "soccer_latvia_na",
    "estonian premium liiga":        "soccer_estonia_na",
    "georgian erovnuli liga":        "soccer_georgia_na",
    "armenian premier league":       "soccer_armenia_na",
    "kazakh premier league":         "soccer_kazakhstan_na",
    "ukrainian premier league":      "soccer_ukraine_na",
    "premier league ukraine":        "soccer_ukraine_na",  # explicit block
    "upl":                           "soccer_ukraine_na",
    "scottish championship": "soccer_scotland_championship",
    # Norway / Finland
    "veikkausliiga": "soccer_finland_veikkausliiga",
    # Romania / Hungary / Serbia / Croatia
    "liga 1": "soccer_romania_1_liga",
    "otp bank liga": "soccer_hungary_otp_bank_liga",
    "nemzeti bajnokság": "soccer_hungary_otp_bank_liga",
    "super liga": "soccer_serbia_superliga",
    "hnl": "soccer_croatia_hnl",
    # Czech / Slovakia
    "fortuna liga": "soccer_czech_republic_league",
    "nike liga": "soccer_slovakia_superliga",
    # Israel
    "ligat ha\'al": "soccer_israel_premier_league",
    "liga mx": "soccer_mexico_ligamx",
}

# Bookmaker priority per market type
# Stale threshold: 120s (2 min) — bookies without recent update are skipped
# Order: sharpest/exchange first, then broad-coverage bookmakers
# Pinnacle is EXCLUDED by design
STALE_MAX = 120
BOOKMAKER_PRIORITY = {
    "h2h": [
        ("betfair_ex_eu",  STALE_MAX),   # EU exchange — sharpest live prices
        ("betfair_ex_uk",  STALE_MAX),   # UK exchange
        ("betfair",        STALE_MAX),   # Betfair generic
        ("matchbook",      STALE_MAX),   # exchange
        ("coolbet",        STALE_MAX),   # sharp, good SA coverage
        ("nordicbet",      STALE_MAX),   # covers SA/international
        ("betsson",        STALE_MAX),
        ("unibet_eu",      STALE_MAX),
        ("bet365",         STALE_MAX),   # very broad global coverage
        ("williamhill",    STALE_MAX),
        ("sport888",       STALE_MAX),
    ],
    "totals": [
        ("betfair_ex_eu",  STALE_MAX),
        ("matchbook",      STALE_MAX),
        ("coolbet",        STALE_MAX),
        ("betsson",        STALE_MAX),
        ("bet365",         STALE_MAX),
    ],
    "spreads": [
        ("betfair_ex_eu",  STALE_MAX),
        ("coolbet",        STALE_MAX),   # sharp, boa cobertura europeia
        ("matchbook",      STALE_MAX),
    ],
}


# ════════════════════════════════════════════════════════════
#  BENTER RATIO TABLE — Model vs Bookie weight by minute
# ════════════════════════════════════════════════════════════

BENTER_TABLE = [
    (0,  10, 0.10, 0.90),
    (10, 20, 0.20, 0.80),
    (20, 30, 0.30, 0.70),
    (30, 40, 0.40, 0.60),
    (40, 50, 0.50, 0.50),
    (50, 60, 0.60, 0.40),
    (60, 70, 0.70, 0.30),
    (70, 80, 0.80, 0.20),
    (80, 100, 0.90, 0.10),
]


def get_benter_weights(minute):
    if minute is None:
        return (0.10, 0.90)
    for from_m, to_m, mw, bw in BENTER_TABLE:
        if from_m <= minute < to_m:
            return (mw, bw)
    return (0.90, 0.10)


# ════════════════════════════════════════════════════════════
#  INTERVAL ADJUSTS — Goal-rate momentum by time segment
#  Source: Premier League historical distribution
#  Formula per row: segment_% / average(all_segments_up_to_now)
#  Applied to remaining xG projection to correct for late-game momentum
# ════════════════════════════════════════════════════════════

INTERVAL_ADJUSTS = [
    #  from  to    goals%   adjust
    (  0,   15,  11.50,   1.00),
    ( 16,   30,  14.10,   1.10),
    ( 31,   45,  15.90,   1.15),
    ( 46,   60,  15.70,   1.10),
    ( 61,   75,  18.20,   1.21),
    ( 76,  100,  24.60,   1.48),
]


def get_interval_adjust(minute):
    """Return momentum adjustment factor for remaining xG based on current minute.

    The factor reflects that goal rates increase as matches progress —
    e.g. at minute 65 the remaining time has 1.21× the average goal rate,
    so projected remaining xG is scaled up accordingly.
    """
    if minute is None or minute <= 0:
        return 1.0
    for from_m, to_m, _pct, adjust in INTERVAL_ADJUSTS:
        if from_m <= minute <= to_m:
            return adjust
    return INTERVAL_ADJUSTS[-1][3]  # extra time → use last factor (1.48)


# ════════════════════════════════════════════════════════════
#  TEAM NAME MATCHING — Fuzzy + persistent alias DB
# ════════════════════════════════════════════════════════════

_team_aliases = {}
_alias_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "team_aliases.json")
_alias_lock = threading.Lock()


def _load_aliases():
    global _team_aliases
    try:
        if os.path.exists(_alias_db_path):
            with open(_alias_db_path, "r") as f:
                _team_aliases = json.load(f)
                log.info(f"Loaded {len(_team_aliases)} team aliases")
    except Exception as e:
        log.warning(f"Could not load team aliases: {e}")


def _save_aliases():
    try:
        with open(_alias_db_path, "w") as f:
            json.dump(_team_aliases, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning(f"Could not save team aliases: {e}")


def _normalize_team(name):
    if not name:
        return ""
    n = name.lower().strip()
    for suffix in [" fc", " cf", " sc", " ac", " afc", " ssc", " bc",
                   " calcio", " sport", " club", " fk", " sk", " if",
                   " de futebol", " futebol clube", " cp", " sl",
                   " football club", " futbol club"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    for prefix in ["fc ", "cf ", "sc ", "ac ", "sl ", "ss ", "as ", "us "]:
        if n.startswith(prefix):
            n = n[len(prefix):].strip()
    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a", "ä": "a",
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "í": "i", "ì": "i", "î": "i", "ï": "i",
        "ó": "o", "ò": "o", "õ": "o", "ô": "o", "ö": "o",
        "ú": "u", "ù": "u", "û": "u", "ü": "u",
        "ç": "c", "ñ": "n", "ß": "ss",
        "ø": "o", "å": "a", "æ": "ae",
    }
    for k, v in replacements.items():
        n = n.replace(k, v)
    n = n.replace(" de ", " ")
    return n


TEAM_HARDCODED_ALIASES = {
    "internazionale": "inter milan",
    "inter": "inter milan",
    "fc internazionale milano": "inter milan",
    "wolverhampton wanderers": "wolves",
    "wolverhampton": "wolves",
    "nottingham forest": "nott'm forest",
    "rb leipzig": "leipzig",
    "rasenballsport leipzig": "leipzig",
    "bayer leverkusen": "bayer 04 leverkusen",
    "celta vigo": "celta de vigo",
    "real sociedad": "real sociedad san sebastian",
    "psv eindhoven": "psv",
    "ajax amsterdam": "ajax",
    "1899 hoffenheim": "hoffenheim",
    "hertha berlin": "hertha bsc",
    "sporting cp": "sporting lisbon",
    "sporting clube de portugal": "sporting lisbon",
}


def _similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def _find_best_match(sofascore_name, odds_api_teams):
    ss_norm = _normalize_team(sofascore_name)

    # 0) Hardcoded aliases
    if ss_norm in TEAM_HARDCODED_ALIASES:
        alias_norm = TEAM_HARDCODED_ALIASES[ss_norm]
        for t in odds_api_teams:
            tn = _normalize_team(t)
            if tn == alias_norm or alias_norm in tn or tn in alias_norm:
                _learn_alias(sofascore_name, t)
                return (t, 1.0)

    # 1) Alias DB
    alias_key = f"ss:{ss_norm}"
    with _alias_lock:
        if alias_key in _team_aliases:
            known = _team_aliases[alias_key]
            for t in odds_api_teams:
                if _normalize_team(t) == _normalize_team(known):
                    return (t, 1.0)

    # 2) Exact normalized match
    for t in odds_api_teams:
        if _normalize_team(t) == ss_norm:
            _learn_alias(sofascore_name, t)
            return (t, 1.0)

    # 3) Substring match
    for t in odds_api_teams:
        tn = _normalize_team(t)
        if len(ss_norm) >= 4 and len(tn) >= 4:
            if ss_norm in tn or tn in ss_norm:
                _learn_alias(sofascore_name, t)
                return (t, 0.95)

    # 4) Fuzzy match
    best, best_score = None, 0
    for t in odds_api_teams:
        score = _similarity(ss_norm, _normalize_team(t))
        if score > best_score:
            best, best_score = t, score

    if best_score >= 0.65:
        _learn_alias(sofascore_name, best)
        return (best, best_score)

    return (None, 0)


def _learn_alias(sofascore_name, odds_api_name):
    ss_norm = _normalize_team(sofascore_name)
    with _alias_lock:
        _team_aliases[f"ss:{ss_norm}"] = odds_api_name
        _team_aliases[f"oa:{_normalize_team(odds_api_name)}"] = sofascore_name
    threading.Thread(target=_save_aliases, daemon=True).start()


# ════════════════════════════════════════════════════════════
#  ODDS CACHE — TTL-based per sport key
# ════════════════════════════════════════════════════════════

_odds_cache = {}
_odds_cache_lock = threading.Lock()
ODDS_CACHE_TTL = 120         # 2 min — used when there are LIVE monitored games for this sport
ODDS_CACHE_TTL_IDLE = 7200   # 2 hours — used when NO live games (saves quota for client-side polling)
_api_requests_remaining = None
_api_quotas = {}   # api_key → remaining (tracks quota per key independently)


def _get_odds_api(url, params=None, api_key=None):
    global _api_requests_remaining
    import requests as req

    effective_key = api_key or _active_odds_key()
    if params is None:
        params = {}
    params["apiKey"] = effective_key

    try:
        resp = req.get(url, params=params, timeout=15)

        remaining = resp.headers.get("x-requests-remaining")
        used = resp.headers.get("x-requests-used")
        if remaining is not None:
            r = int(remaining)
            _api_requests_remaining = r
            _api_quotas[effective_key] = r
            log.info(f"Odds API quota [{effective_key[:8]}…] — remaining: {r}, used: {used}")

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 401:
            log.error("Odds API: invalid API key")
        elif resp.status_code == 422:
            log.warning(f"Odds API: invalid params — {resp.text[:200]}")
        elif resp.status_code == 429:
            log.error("Odds API: rate limited / quota exceeded")
        else:
            log.warning(f"Odds API: HTTP {resp.status_code}")
        return None
    except Exception as e:
        log.error(f"Odds API request error: {e}")
        return None


def _has_live_for_sport(sport_key) -> bool:
    """Check if there's any live game in _live_state matching this sport_key."""
    try:
        with _state_lock:
            for entry in _live_state.values():
                m = entry.get("match", {})
                tourn = m.get("tournament", "")
                country = m.get("country", "")
                if _resolve_sport_key(tourn, country) == sport_key:
                    return True
    except Exception:
        pass
    return False


def get_odds_for_sport(sport_key, force=False, api_key=None):
    now = time.time()
    # Normalize cache key by effective API key (resolved), so background cycle
    # and frontend requests using the same key share the same cache entry.
    effective_key = api_key or _active_odds_key() or "default"
    cache_key = f"{sport_key}:{effective_key}"

    # Adaptive TTL: when no live game exists for this sport, use long TTL
    # to prevent burning quota on idle client-side polling (frontend opening
    # match detail pages for games that haven't started yet).
    has_live = _has_live_for_sport(sport_key)
    effective_ttl = ODDS_CACHE_TTL if has_live else ODDS_CACHE_TTL_IDLE

    with _odds_cache_lock:
        cached = _odds_cache.get(cache_key)
        if cached and not force and (now - cached["ts"]) <= effective_ttl:
            mode = "LIVE" if has_live else "IDLE"
            log.info(f"Odds cache HIT [{mode}] for {cache_key} ({now - cached['ts']:.0f}s old, ttl={effective_ttl}s)")
            return cached["data"]

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    data = _get_odds_api(url, {
        "regions": "eu",      # Apenas região europeia — Betfair EU, bet365, Matchbook, Coolbet, Unibet
        "markets": "h2h,totals,spreads",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }, api_key=api_key)

    if data is None:
        with _odds_cache_lock:
            cached = _odds_cache.get(cache_key)
        if cached:
            log.info(f"Using stale cache for {cache_key}")
            return cached["data"]
        log.warning(f"Odds API returned None for {sport_key} (API error or quota exceeded)")
        return None   # distinguish from empty []

    with _odds_cache_lock:
        _odds_cache[cache_key] = {"data": data, "ts": now}

    log.info(f"Fetched {len(data)} events for {sport_key} (regions: eu)")
    return data


def _normalize_tournament(name):
    """Strip Sofascore suffixes like ', Group A', ', Conference League Playoffs', etc."""
    import re
    # Remove anything after a comma that looks like a sub-tournament qualifier
    # e.g. "Pro League, Conference League Playoffs" → "Pro League"
    #      "Austrian Bundesliga, Relegation Round" → "Austrian Bundesliga"
    #      "Copa Libertadores, Group G" → "Copa Libertadores"
    cleaned = re.sub(
        r'\s*,\s+(group|grp|round|phase|stage|pool|matchday|md|jornada|giornata|journée|'
        r'spieltag|playoff|play-off|play off|qualification|qualifying|relegation|promotion|'
        r'conference|champions|europa|cup|shield|super|final|semi|quarter)\b.*$',
        '', name, flags=re.IGNORECASE
    ).strip()
    # Also strip any remaining trailing ", Anything" (catch-all for unknown qualifiers)
    cleaned = re.sub(r'\s*,.*$', '', cleaned).strip()
    # Remove trailing parenthetical qualifiers: "Premier League (Women)"
    cleaned = re.sub(r'\s*\(.*\)\s*$', '', cleaned).strip()
    return cleaned.lower()


def _resolve_sport_key(tournament_name, country=None):
    if not tournament_name:
        return None

    raw  = tournament_name.lower().strip()
    norm = _normalize_tournament(tournament_name)  # already lowercase

    # 1. Exact match — try normalized first so "Pro League, Championship Round"
    #    resolves via "pro league" before keyword scan finds "championship".
    for t in [norm, raw]:
        if t in TOURNAMENT_TO_SPORT_KEY:
            return TOURNAMENT_TO_SPORT_KEY[t]

    # 2. Keyword scan — sorted longest-first so "austrian bundesliga" beats "bundesliga".
    #    Scan normalized name first for the same reason as above.
    sorted_map = sorted(TOURNAMENT_TO_SPORT_KEY.items(), key=lambda x: -len(x[0]))
    for t in [norm, raw]:
        for keyword, sport_key in sorted_map:
            if keyword in t:
                return sport_key

    # 3. Country-prefixed scan (last resort)
    if country:
        cc = country.lower()
        for t in [norm, raw]:
            combined = f"{cc} {t}"
            for keyword, sport_key in sorted_map:
                if keyword in combined:
                    return sport_key

    return None


def _extract_bookmaker_odds(bookmakers, market_key):
    priority = BOOKMAKER_PRIORITY.get(market_key, [])
    now_iso = datetime.now(timezone.utc)

    # Filter out Pinnacle completely from bookmakers list
    bookmakers_filtered = [bm for bm in bookmakers if bm.get("key", "").lower() != "pinnacle"]

    for bookie_key, max_stale in priority:
        for bm in bookmakers_filtered:
            if bm["key"] == bookie_key:
                for mkt in bm.get("markets", []):
                    mkt_key = mkt["key"]
                    if mkt_key == market_key or (market_key == "h2h" and mkt_key in ("h2h", "h2h_lay")):
                        last_update = mkt.get("last_update") or bm.get("last_update", "")
                        staleness = None
                        if last_update:
                            try:
                                lu_dt = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                                staleness = (now_iso - lu_dt).total_seconds()
                            except Exception:
                                pass

                        if staleness is not None and staleness > max_stale:
                            log.debug(f"{bookie_key} {market_key} stale ({staleness:.0f}s > {max_stale}s)")
                            break

                        return {
                            "bookmaker": bm["key"],
                            "bookmakerTitle": bm["title"],
                            "market": mkt_key,
                            "lastUpdate": last_update,
                            "staleness": round(staleness, 1) if staleness else None,
                            "outcomes": mkt["outcomes"],
                        }

    # Fallback: any non-Pinnacle bookmaker
    for bm in bookmakers_filtered:
        for mkt in bm.get("markets", []):
            if mkt["key"] == market_key:
                last_update = mkt.get("last_update") or bm.get("last_update", "")
                return {
                    "bookmaker": bm["key"],
                    "bookmakerTitle": bm["title"],
                    "market": mkt["key"],
                    "lastUpdate": last_update,
                    "staleness": None,
                    "outcomes": mkt["outcomes"],
                    "isFallback": True,
                }

    return None


def _remove_vig(outcomes):
    if not outcomes:
        return {}

    implied = {}
    total = 0
    for o in outcomes:
        price = o.get("price", 0)
        if price > 0:
            p = 1.0 / price
            implied[o["name"]] = p
            total += p

    if total == 0:
        return {}

    return {name: round(p / total, 6) for name, p in implied.items()}


# ════════════════════════════════════════════════════════════
#  POISSON MODEL — Convert xG to match outcome probabilities
# ════════════════════════════════════════════════════════════

def _poisson_pmf(k, lam):
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def xg_to_probabilities(home_xg, away_xg, home_goals, away_goals, minute,
                        max_goals=8):
    # Normalizar minuto — nunca usar None ou 0 como elapsed (causaria divisão por 0
    # ou projeções astronómicas ao tratar todo o xG como ganho em 1 minuto)
    if minute is None or minute <= 0:
        minute = 45   # fallback seguro: assume que estamos a meio do jogo

    # Duração efetiva: 95 min (tempo regulamentar + ~5 min compensação) ou 125 (prolongamento + comp.)
    # Estimar tempo de jogo real é importante para projeção de xG remanescente.
    full_duration = 125 if minute > 90 else 95
    elapsed = min(minute, full_duration)
    remaining = max(full_duration - elapsed, 1)

    home_rate = home_xg / elapsed if elapsed > 0 else 0
    away_rate = away_xg / elapsed if elapsed > 0 else 0

    # Sanity cap: no team can generate > 0.08 xG/min legitimately (= 7.2 xG/90).
    # If we get higher it means elapsed is unrealistically small (transition glitch).
    # Cap the rate and log a warning so we can diagnose if needed.
    MAX_XG_RATE = 0.08
    if home_rate > MAX_XG_RATE or away_rate > MAX_XG_RATE:
        log.warning(
            f"xG rate sanity cap triggered: home={home_rate:.4f} away={away_rate:.4f} "
            f"xG/min (elapsed={elapsed}min). Capping at {MAX_XG_RATE}. "
            f"Raw xG: home={home_xg:.3f} away={away_xg:.3f}"
        )
        home_rate = min(home_rate, MAX_XG_RATE)
        away_rate = min(away_rate, MAX_XG_RATE)

    # Apply momentum adjustment: goals cluster increasingly in later intervals
    interval_adj = get_interval_adjust(minute)
    remaining_home_xg = home_rate * remaining * interval_adj
    remaining_away_xg = away_rate * remaining * interval_adj

    remaining_home_xg = max(remaining_home_xg, 0.01)
    remaining_away_xg = max(remaining_away_xg, 0.01)

    home_probs = [_poisson_pmf(k, remaining_home_xg) for k in range(max_goals + 1)]
    away_probs = [_poisson_pmf(k, remaining_away_xg) for k in range(max_goals + 1)]

    p_home_win = 0
    p_draw = 0
    p_away_win = 0
    total_goals_dist = {}

    for h_remaining in range(max_goals + 1):
        for a_remaining in range(max_goals + 1):
            prob = home_probs[h_remaining] * away_probs[a_remaining]
            final_home = home_goals + h_remaining
            final_away = away_goals + a_remaining
            final_total = final_home + final_away

            if final_home > final_away:
                p_home_win += prob
            elif final_home == final_away:
                p_draw += prob
            else:
                p_away_win += prob

            total_goals_dist[final_total] = total_goals_dist.get(final_total, 0) + prob

    over_under = {}
    for line in [0.5, 1.5, 2.5, 3.5, 4.5]:
        p_over = sum(p for g, p in total_goals_dist.items() if g > line)
        over_under[str(line)] = {
            "over": round(p_over, 6),
            "under": round(1 - p_over, 6),
        }

    return {
        "homeWin": round(p_home_win, 6),
        "draw": round(p_draw, 6),
        "awayWin": round(p_away_win, 6),
        "overUnder": over_under,
        "projectedXg": {
            "homeRemaining": round(remaining_home_xg, 4),
            "awayRemaining": round(remaining_away_xg, 4),
            "homeTotal": round(home_xg + remaining_home_xg, 4),
            "awayTotal": round(away_xg + remaining_away_xg, 4),
            "intervalAdjust": round(interval_adj, 2),
        },
    }


# ════════════════════════════════════════════════════════════
#  BENTER VALUE CALCULATION
# ════════════════════════════════════════════════════════════

def calculate_benter_value(model_probs, bookie_novig, bookie_odds, minute):
    model_w, bookie_w = get_benter_weights(minute)

    results = {}
    for outcome in model_probs:
        m_prob = model_probs.get(outcome, 0)
        b_prob = bookie_novig.get(outcome, 0)

        blended = (model_w * m_prob) + (bookie_w * b_prob)

        odds = bookie_odds.get(outcome, 0)
        value = (blended * odds - 1) if odds > 0 else 0

        results[outcome] = {
            "modelProb": round(m_prob, 4),
            "bookieNoVig": round(b_prob, 4),
            "blendedProb": round(blended, 4),
            "impliedOdds": round(1 / blended, 3) if blended > 0 else None,
            "bookieOdds": odds,
            "value": round(value, 4),
            "isValue": value * 100 >= get_setting("min_edge_pct", 10.0),
            "edge": round(value * 100, 2),
        }

    return {
        "benterWeights": {"model": model_w, "bookie": bookie_w},
        "minute": minute,
        "outcomes": results,
    }


def get_full_odds_analysis(match, shots, api_key=None):
    """Full pipeline: fetch odds, compute xG model probs, apply Benter, return value analysis."""
    tournament = match.get("tournament", "")
    country = match.get("country", "")
    sport_key = _resolve_sport_key(tournament, country)

    if not sport_key:
        return {
            "available": False,
            "reason": f"No odds mapping for tournament: {tournament}",
            "sportKey": None,
        }

    odds_events = get_odds_for_sport(sport_key, api_key=api_key)

    if odds_events is None:
        return {
            "available": False,
            "reason": "Odds API error — quota esgotada ou chave inválida. Verifica a tua API key.",
            "sportKey": sport_key,
        }

    if not odds_events:
        return {
            "available": False,
            "reason": f"Sem eventos disponíveis em {sport_key} neste momento",
            "sportKey": sport_key,
        }

    home_team = match.get("homeTeam", "")
    away_team = match.get("awayTeam", "")

    all_odds_teams = set()
    for ev in odds_events:
        all_odds_teams.add(ev.get("home_team", ""))
        all_odds_teams.add(ev.get("away_team", ""))

    home_match, home_conf = _find_best_match(home_team, list(all_odds_teams))
    away_match, away_conf = _find_best_match(away_team, list(all_odds_teams))

    if not home_match or not away_match:
        return {
            "available": False,
            "reason": f"Could not match teams: {home_team} (conf:{home_conf:.2f}), {away_team} (conf:{away_conf:.2f})",
            "sportKey": sport_key,
            "matchAttempt": {
                "homeTeam": {"sofascore": home_team, "oddsApi": home_match, "confidence": round(home_conf, 2)},
                "awayTeam": {"sofascore": away_team, "oddsApi": away_match, "confidence": round(away_conf, 2)},
            },
        }

    matched_event = None
    for ev in odds_events:
        if ((ev.get("home_team") == home_match and ev.get("away_team") == away_match) or
            (ev.get("home_team") == away_match and ev.get("away_team") == home_match)):
            matched_event = ev
            break

    if not matched_event:
        return {
            "available": False,
            "reason": "Teams matched individually but no combined event found",
            "sportKey": sport_key,
        }

    bookmakers = matched_event.get("bookmakers", [])

    h2h_data = _extract_bookmaker_odds(bookmakers, "h2h")
    totals_data = _extract_bookmaker_odds(bookmakers, "totals")
    spreads_data = _extract_bookmaker_odds(bookmakers, "spreads")

    odds_result = {
        "available": True,
        "sportKey": sport_key,
        "oddsApiEventId": matched_event.get("id"),
        "commenceTime": matched_event.get("commence_time"),
        "teamMapping": {
            "home": {"sofascore": home_team, "oddsApi": home_match, "confidence": round(home_conf, 2)},
            "away": {"sofascore": away_team, "oddsApi": away_match, "confidence": round(away_conf, 2)},
        },
        "h2h": None,
        "totals": None,
        "spreads": None,
        "benter": None,
        "modelProbabilities": None,
    }

    # ── Process 1X2 odds ──
    if h2h_data:
        outcomes = h2h_data["outcomes"]
        novig = _remove_vig(outcomes)

        raw_odds = {}
        for o in outcomes:
            name = o["name"]
            if name == "Draw":
                raw_odds["draw"] = o["price"]
            elif _normalize_team(name) == _normalize_team(home_match):
                raw_odds["home"] = o["price"]
            else:
                raw_odds["away"] = o["price"]

        novig_mapped = {}
        for o in outcomes:
            name = o["name"]
            if name == "Draw":
                novig_mapped["draw"] = novig.get(name, 0)
            elif _normalize_team(name) == _normalize_team(home_match):
                novig_mapped["home"] = novig.get(name, 0)
            else:
                novig_mapped["away"] = novig.get(name, 0)

        h2h_data["rawOdds"] = raw_odds
        h2h_data["noVig"] = novig_mapped
        odds_result["h2h"] = h2h_data

    # ── Process totals ──
    if totals_data:
        outcomes = totals_data["outcomes"]
        novig = _remove_vig(outcomes)

        raw_odds = {}
        novig_mapped = {}
        point = None
        for o in outcomes:
            name = o["name"].lower()
            if "over" in name:
                raw_odds["over"] = o["price"]
                novig_mapped["over"] = novig.get(o["name"], 0)
                point = o.get("point")
            elif "under" in name:
                raw_odds["under"] = o["price"]
                novig_mapped["under"] = novig.get(o["name"], 0)
                if point is None:
                    point = o.get("point")

        totals_data["point"] = point
        totals_data["rawOdds"] = raw_odds
        totals_data["noVig"] = novig_mapped
        odds_result["totals"] = totals_data

    # ── Process spreads/handicaps ──
    if spreads_data:
        outcomes = spreads_data["outcomes"]
        novig = _remove_vig(outcomes)

        raw_odds = {}
        novig_mapped = {}
        for o in outcomes:
            if _normalize_team(o["name"]) == _normalize_team(home_match):
                raw_odds["home"] = o["price"]
                novig_mapped["home"] = novig.get(o["name"], 0)
                spreads_data["homePoint"] = o.get("point")
            else:
                raw_odds["away"] = o["price"]
                novig_mapped["away"] = novig.get(o["name"], 0)
                spreads_data["awayPoint"] = o.get("point")

        spreads_data["rawOdds"] = raw_odds
        spreads_data["noVig"] = novig_mapped
        odds_result["spreads"] = spreads_data

    # ── xG Model probabilities ──
    minute = match.get("minute")
    home_xg = shots.get("homeXg", 0)
    away_xg = shots.get("awayXg", 0)
    home_goals = match.get("homeGoals", 0)
    away_goals = match.get("awayGoals", 0)

    if match.get("isLive") and (home_xg > 0 or away_xg > 0):
        model = xg_to_probabilities(home_xg, away_xg, home_goals, away_goals, minute)
        odds_result["modelProbabilities"] = model

        # ── Benter value for 1X2 ──
        if h2h_data and model:
            model_1x2 = {
                "home": model["homeWin"],
                "draw": model["draw"],
                "away": model["awayWin"],
            }
            bookie_novig_1x2 = h2h_data.get("noVig", {})
            bookie_raw_1x2 = h2h_data.get("rawOdds", {})

            benter_1x2 = calculate_benter_value(
                model_1x2, bookie_novig_1x2, bookie_raw_1x2, minute
            )
            benter_1x2["market"] = "1X2"

            # ── Benter value for totals ──
            benter_totals = None
            if totals_data and totals_data.get("point"):
                line = str(totals_data["point"])
                if line in model["overUnder"]:
                    model_ou = {
                        "over": model["overUnder"][line]["over"],
                        "under": model["overUnder"][line]["under"],
                    }
                    benter_totals = calculate_benter_value(
                        model_ou,
                        totals_data.get("noVig", {}),
                        totals_data.get("rawOdds", {}),
                        minute
                    )
                    benter_totals["market"] = f"O/U {line}"
                    benter_totals["line"] = float(line)

            # ── Benter value for spreads/handicaps ──
            benter_spreads = None
            if spreads_data and spreads_data.get("rawOdds"):
                home_pt = spreads_data.get("homePoint", 0) or 0
                away_pt = spreads_data.get("awayPoint", 0) or 0
                raw_sp = spreads_data.get("rawOdds", {})
                novig_sp = spreads_data.get("noVig", {})

                # Model probabilities for spreads: adjust goals by handicap line
                # Apply the handicap line to the projected final score distribution
                # and recompute win/lose probability using the same Poisson dist
                model_home_remaining = model["projectedXg"]["homeRemaining"]
                model_away_remaining = model["projectedXg"]["awayRemaining"]

                def hcp_probs(home_pt_val, away_pt_val, max_g=8):
                    """Compute P(home covers) and P(away covers) for Asian handicap line."""
                    h_probs = [_poisson_pmf(k, max(model_home_remaining, 0.01)) for k in range(max_g+1)]
                    a_probs = [_poisson_pmf(k, max(model_away_remaining, 0.01)) for k in range(max_g+1)]
                    p_home = 0.0
                    p_away = 0.0
                    for h in range(max_g+1):
                        for a in range(max_g+1):
                            p = h_probs[h] * a_probs[a]
                            final_h = home_goals + h + home_pt_val  # adjusted by handicap
                            final_a = away_goals + a
                            if final_h > final_a:
                                p_home += p
                            elif final_h < final_a:
                                p_away += p
                            # exact tie: half-win / push — split probability
                            else:
                                p_home += p * 0.5
                                p_away += p * 0.5
                    return p_home, p_away

                h_pt_val = float(home_pt) if home_pt is not None else 0.0
                a_pt_val = float(away_pt) if away_pt is not None else 0.0
                p_home_cov, p_away_cov = hcp_probs(h_pt_val, a_pt_val)

                model_sp = {"home": round(p_home_cov, 6), "away": round(p_away_cov, 6)}
                # noVig already keyed as home/away from the processing block above
                novig_home = novig_sp.get("home")
                novig_away = novig_sp.get("away")
                # Fallback: if keys are None (shouldn't happen), try first two values
                if novig_home is None or novig_away is None:
                    vals = list(novig_sp.values())
                    if len(vals) >= 2:
                        novig_home, novig_away = vals[0], vals[1]
                novig_mapped_sp = {"home": novig_home or 0.0, "away": novig_away or 0.0}

                benter_spreads = calculate_benter_value(
                    model_sp, novig_mapped_sp,
                    {"home": raw_sp.get("home", 0), "away": raw_sp.get("away", 0)},
                    minute
                )
                benter_spreads["market"] = "Handicap"
                benter_spreads["homePoint"] = home_pt
                benter_spreads["awayPoint"] = away_pt
                benter_spreads["bookmaker"] = spreads_data.get("bookmakerTitle", "")

            odds_result["benter"] = {
                "h2h": benter_1x2,
                "totals": benter_totals,
                "spreads": benter_spreads,
            }

    # ── API quota info ──
    odds_result["quotaRemaining"] = _api_requests_remaining

    return odds_result


# ════════════════════════════════════════════════════════════
#  SOFASCORE — Client code
# ════════════════════════════════════════════════════════════

def _init_client():
    global _client_type, _session

    # 1) curl_cffi — try multiple impersonation profiles, newest first.
    # Sofascore upgrades their TLS fingerprint detection periodically; when
    # they do, the generic "chrome" profile starts returning 403. Trying
    # specific recent versions + non-Chrome browsers gives us redundancy.
    # As of 2026-05, Sofascore was blocking the default "chrome" profile.
    try:
        from curl_cffi.requests import Session as CffiSession
        profiles_to_try = [
            "chrome136", "chrome133a", "chrome131", "chrome124", "chrome123",
            "chrome120", "chrome119", "chrome116",
            "safari18_0", "safari17_0", "safari17_2_ios",
            "firefox133", "edge101",
            "chrome",  # fallback to generic
        ]
        for profile in profiles_to_try:
            try:
                _session = CffiSession(impersonate=profile)
                resp = _session.get(SOFASCORE_WEB, timeout=15)
                if resp.status_code == 200:
                    _client_type = f"curl_cffi:{profile}"
                    log.info(f"Using curl_cffi (TLS impersonation: {profile})")
                    return True
                else:
                    log.info(f"curl_cffi profile '{profile}' got status {resp.status_code}, trying next")
            except Exception as e:
                log.info(f"curl_cffi profile '{profile}' failed: {type(e).__name__}: {e}")
                continue
    except ImportError:
        log.info("curl_cffi not available")
    except Exception as e:
        log.warning(f"curl_cffi outer failed: {e}")

    # 2) cloudscraper
    try:
        import cloudscraper
        _session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True},
            delay=3,
        )
        resp = _session.get(SOFASCORE_WEB, timeout=15)
        if resp.status_code == 200:
            _client_type = "cloudscraper"
            log.info("Using cloudscraper")
            return True
    except ImportError:
        log.info("cloudscraper not available")
    except Exception as e:
        log.warning(f"cloudscraper failed: {e}")

    # 3) requests
    try:
        import requests as req
        _session = req.Session()
        _session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.sofascore.com/",
            "Origin": "https://www.sofascore.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Sec-Ch-Ua": '"Chromium";v="126", "Google Chrome";v="126"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        })
        resp = _session.get(SOFASCORE_WEB, timeout=15)
        _client_type = "requests"
        log.info(f"Using requests (status={resp.status_code})")
        return True
    except Exception as e:
        log.error(f"All clients failed: {e}")
        return False


_last_req = 0
REQ_GAP = 2.0


def _get_bytes(url, timeout: int = 8):
    """
    Like _get but returns raw bytes + content-type. Uses the same session
    (curl_cffi with Chrome TLS impersonation) so endpoints that 403 plain
    `requests` (e.g. Sofascore image CDN) work here.
    """
    global _last_req
    if _session is None:
        _init_client()
    extra_headers = None
    if "sofascore.com" in url:
        extra_headers = {
            "Origin":          "https://www.sofascore.com",
            "Referer":         "https://www.sofascore.com/",
            "Accept":          "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Sec-Fetch-Site":  "same-site",
            "Sec-Fetch-Mode":  "no-cors",
            "Sec-Fetch-Dest":  "image",
        }
    for attempt in range(2):
        try:
            wait = REQ_GAP - (time.time() - _last_req)
            if wait > 0:
                time.sleep(wait)
            _last_req = time.time()
            resp = _session.get(url, timeout=timeout, headers=extra_headers) if extra_headers else _session.get(url, timeout=timeout)
            if resp.status_code == 200:
                ct = resp.headers.get("Content-Type", "image/png")
                return resp.content, ct
            if resp.status_code in (403, 429):
                _init_client()
                time.sleep(2 * (attempt + 1))
            elif resp.status_code == 404:
                return None
        except Exception as e:
            log.warning(f"_get_bytes error for {url}: {e}")
            time.sleep(1)
    return None


def _get(url, retries=3):
    global _last_req
    if _session is None:
        _init_client()

    # Sofascore upgraded their API anti-bot in May 2026. The website now
    # passes the TLS check but api.sofascore.com still 403s unless the
    # request looks like a same-site fetch from the SPA (proper Origin,
    # Referer, Sec-Fetch-* headers). curl_cffi's impersonation provides
    # the right browser fingerprint but does not auto-set these for raw
    # API calls — we add them explicitly for any sofascore.com URL.
    extra_headers = None
    if "sofascore.com" in url:
        extra_headers = {
            "Origin":          "https://www.sofascore.com",
            "Referer":         "https://www.sofascore.com/",
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Fetch-Site":  "same-site",
            "Sec-Fetch-Mode":  "cors",
            "Sec-Fetch-Dest":  "empty",
        }

    for attempt in range(retries):
        wait = REQ_GAP - (time.time() - _last_req)
        if wait > 0:
            time.sleep(wait)
        _last_req = time.time()

        try:
            resp = _session.get(url, timeout=15, headers=extra_headers) if extra_headers else _session.get(url, timeout=15)

            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    t = resp.text.strip()
                    if t.startswith(("{", "[")):
                        return json.loads(t)
                    return None

            elif resp.status_code == 403:
                log.warning(f"403 on {url} (attempt {attempt+1}/{retries})")
                if attempt < retries - 1:
                    log.info("Re-initializing session...")
                    _init_client()
                    time.sleep(3 * (attempt + 1))

            elif resp.status_code == 404:
                return None

            elif resp.status_code == 429:
                time.sleep(10 * (attempt + 1))

            else:
                log.warning(f"HTTP {resp.status_code} on {url}")
                time.sleep(2 * (attempt + 1))

        except Exception as e:
            log.error(f"Request error: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
                _init_client()

    return None


def _parse_event(ev):
    home  = ev.get("homeTeam", {})
    away  = ev.get("awayTeam", {})
    hs    = ev.get("homeScore", {})
    aws   = ev.get("awayScore", {})
    st    = ev.get("status", {})
    tourn = ev.get("tournament", {})

    code = st.get("code", 0)  # 6=1ª parte, 7=2ª parte, 31=intervalo, 100=FT
    desc = st.get("description")  # texto: "1st half", "2nd half", "Halftime", etc.

    minute = None

    # currentPeriodStartTimestamp — timestamp exato de quando o período atual
    # começou (confirmado na API da Sofascore). Fonte mais fiável: não depende
    # de estimativas da duração do intervalo.
    # A Sofascore envia currentPeriodStartTimestamp em dois sítios:
    #   - /event/{id}          → top-level do evento
    #   - /events/live (lista) → dentro de ev["time"]
    # Verificamos os dois.
    period_ts = (
        ev.get("currentPeriodStartTimestamp")
        or ev.get("time", {}).get("currentPeriodStartTimestamp")
    )

    if period_ts and code in (6, 7):
        now = int(time.time())
        elapsed_secs = max(0, now - period_ts)
        elapsed = elapsed_secs // 60

        # Guard: if period_ts was set < 90 seconds ago the timestamp was just reset
        # (Sofascore updates it when transitioning between periods).
        # With elapsed=0 the rate calculation explodes — discard and use fallback.
        if elapsed_secs >= 90:
            if code == 6:
                minute = 1 + elapsed   # 1ª parte começa em 1'
            else:
                minute = 46 + elapsed  # 2ª parte começa em 46'
        # else: leave minute=None, fall through to startTimestamp fallback below

    # Fallback: startTimestamp (para ligas menores sem currentPeriodStartTimestamp)
    if minute is None and code in (6, 7):
        ts = ev.get("startTimestamp", 0)
        if ts:
            now = int(time.time())
            total_elapsed = max(0, now - ts) // 60
            if code == 6:
                minute = min(total_elapsed, 45)
            else:
                # ~64 min = 1ª parte (~47 min) + intervalo (~17 min)
                minute = min(45 + max(0, total_elapsed - 64), 95)

    # Estados fixos — sem timestamp de período, atribuir minuto convencional
    # code 31 = Intervalo          → considerar 45 min decorridos
    # code 41 = Prolongamento 1ªP  → considerar 105 min (ET começa no 90')
    # code 42 = Prolongamento 2ªP  → considerar 120 min (mas calcular igual)
    # code 80 = Penáltis            → jogo decidido, não há projeção útil
    if minute is None:
        if code == 31:
            minute = 45   # Intervalo: 45' decorridos, 45' restantes
        elif code == 41:
            minute = 105  # Prolongamento 1ª parte
        elif code == 42:
            minute = 120  # Prolongamento 2ª parte

    # Separar tempo de compensação do minuto base para:
    # 1) exibição correta ("45+8'" em vez de "53'")
    # 2) modelo usa minuto capped (45 ou 90) para "remaining" correto
    injury_time = 0
    if minute is not None:
        if code == 6 and minute > 45:
            injury_time = minute - 45
            minute = 45
        elif code == 7 and minute > 90:
            injury_time = minute - 90
            minute = 90

    return {
        "id": ev.get("id"),
        "slug": ev.get("slug", ""),
        "homeTeam": home.get("name", "?"),
        "homeTeamId": home.get("id"),
        "awayTeam": away.get("name", "?"),
        "awayTeamId": away.get("id"),
        "homeGoals": hs.get("current", hs.get("display", 0)) or 0,
        "awayGoals": aws.get("current", aws.get("display", 0)) or 0,
        "statusCode": code,
        "statusType": st.get("type", ""),
        "statusDesc": str(desc) if desc else "",
        "minute": minute,
        "injuryTime": injury_time,
        "startTimestamp": ev.get("startTimestamp"),
        "currentPeriodStartTimestamp": period_ts,
        "tournament":   tourn.get("name", ""),
        "tournamentId": (tourn.get("uniqueTournament") or {}).get("id"),
        "country":      tourn.get("category", {}).get("name", ""),
        "isLive": st.get("type") == "inprogress",
        "isFinished": st.get("type") == "finished",
        "isScheduled": st.get("type") == "notstarted",
    }


def get_live():
    data = _get(f"{SOFASCORE_API}/sport/football/events/live")
    return [_parse_event(e) for e in (data or {}).get("events", [])]


def get_event(event_id: int):
    """Fetch a single Sofascore event by ID."""
    data = _get(f"{SOFASCORE_API}/event/{event_id}")
    ev = (data or {}).get("event")
    return _parse_event(ev) if ev else None


def get_scheduled(date_str=None):
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = _get(f"{SOFASCORE_API}/sport/football/scheduled-events/{date_str}")
    return [_parse_event(e) for e in (data or {}).get("events", [])]


def search(query):
    q = query.lower()
    seen = set()
    out = []
    for m in get_live() + get_scheduled():
        if m["id"] not in seen and (q in m["homeTeam"].lower() or q in m["awayTeam"].lower() or q in m.get("tournament", "").lower()):
            out.append(m)
            seen.add(m["id"])
    return out


def get_shotmap(eid):
    data = _get(f"{SOFASCORE_API}/event/{eid}/shotmap")
    if not data or "shotmap" not in data:
        return {"homeShots": [], "awayShots": [], "homeXg": 0, "awayXg": 0, "totalShots": 0}

    hs, aws = [], []
    hx, ax = 0.0, 0.0

    for s in data["shotmap"]:
        xg = float(s.get("xg") or s.get("expectedGoals") or 0)
        situation = (s.get("situation") or "").lower()
        is_penalty = "penalty" in situation

        p = {
            "xg": round(xg, 4),
            "minute": s.get("time", 0),
            "addedTime": s.get("addedTime", 0),
            "player": s.get("player", {}).get("name", "?"),
            "playerId": s.get("player", {}).get("id"),
            "shotType": s.get("shotType", ""),
            "situation": s.get("situation", ""),
            "bodyPart": s.get("bodyPart", ""),
            "goalMouthLocation": s.get("goalMouthLocation", ""),
            "isGoal": s.get("shotType") == "goal",
            "isHome": s.get("isHome", False),
            "isPenalty": is_penalty,
            "x": s.get("playerCoordinates", {}).get("x"),
            "y": s.get("playerCoordinates", {}).get("y"),
        }
        # Exclude penalty xG from team totals (penalties are not representative of true xG)
        xg_for_total = 0 if is_penalty else p["xg"]

        if p["isHome"]:
            hs.append(p); hx += xg_for_total
        else:
            aws.append(p); ax += xg_for_total

    hs.sort(key=lambda x: (x["minute"], x["addedTime"]))
    aws.sort(key=lambda x: (x["minute"], x["addedTime"]))
    return {"homeShots": hs, "awayShots": aws, "homeXg": round(hx, 4), "awayXg": round(ax, 4), "totalShots": len(hs) + len(aws)}


def get_incidents(eid):
    data = _get(f"{SOFASCORE_API}/event/{eid}/incidents")
    if not data or "incidents" not in data:
        return {"goals": [], "cards": [], "subs": [], "redCards": 0, "lastGoalMinute": None}

    goals, cards, subs = [], [], []
    rc, lgm = 0, None

    for inc in data["incidents"]:
        t = inc.get("incidentType", "")
        if t == "goal":
            m = inc.get("time", 0)
            goals.append({
                "minute": m, "addedTime": inc.get("addedTime", 0),
                "player": inc.get("player", {}).get("name", ""),
                "assist": (inc.get("assist1") or {}).get("name", ""),
                "isHome": inc.get("isHome", False),
                "goalType": inc.get("incidentClass", "regular"),
            })
            if lgm is None or m > lgm: lgm = m
        elif t == "card":
            ct = inc.get("incidentClass", "")
            cards.append({"minute": inc.get("time", 0), "player": inc.get("player", {}).get("name", ""), "isHome": inc.get("isHome", False), "cardType": ct})
            if ct in ("red", "yellowRed"): rc += 1
        elif t == "substitution":
            subs.append({"minute": inc.get("time", 0), "playerIn": inc.get("playerIn", {}).get("name", ""), "playerOut": inc.get("playerOut", {}).get("name", ""), "isHome": inc.get("isHome", False)})

    return {"goals": goals, "cards": cards, "subs": subs, "redCards": rc, "lastGoalMinute": lgm}


def get_event(eid):
    data = _get(f"{SOFASCORE_API}/event/{eid}")
    if not data or "event" not in data:
        return None
    return _parse_event(data["event"])


def get_track(eid, api_key=None):
    det = get_event(eid)
    if not det:
        return {"error": f"Event {eid} not found"}
    shots = get_shotmap(eid)
    incidents = get_incidents(eid)
    odds = get_full_odds_analysis(det, shots, api_key=api_key)
    return {
        "match": det,
        "shots": shots,
        "incidents": incidents,
        "odds": odds,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# ── Routes ──

@app.route("/")
def index():
    return jsonify({
        "service": "Sofascore xG Scraper + Live Odds",
        "version": "4.0",
        "client": _client_type,
        "oddsApi": True,
        "oddsQuotaRemaining": _api_requests_remaining,
    })

@app.route("/api/live")
def r_live():
    try: return jsonify({"count": len(m := get_live()), "matches": m})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/today")
def r_today():
    try: return jsonify({"count": len(m := get_scheduled()), "matches": m})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/schedule/<date>")
def r_sched(date):
    try: return jsonify({"count": len(m := get_scheduled(date)), "matches": m})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/search")
def r_search():
    q = flask_request.args.get("q", "")
    if not q: return jsonify({"error": "?q= required"}), 400
    try: return jsonify({"query": q, "count": len(m := search(q)), "matches": m})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/match/<int:eid>")
def r_match(eid):
    """Full match data including odds & value analysis."""
    try:
        d = get_event(eid)
        if not d: return jsonify({"error": "Not found"}), 404
        shots = get_shotmap(eid)
        incidents = get_incidents(eid)
        odds = get_full_odds_analysis(d, shots)

        return jsonify({
            "match": d,
            "shots": shots,
            "incidents": incidents,
            "odds": odds,
        })
    except Exception as e:
        log.exception(f"Error in /api/match/{eid}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/shots/<int:eid>")
def r_shots(eid):
    try: return jsonify(get_shotmap(eid))
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/incidents/<int:eid>")
def r_inc(eid):
    try: return jsonify(get_incidents(eid))
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/track/<int:eid>")
def r_track(eid):
    """Full tracking with odds & Benter value (used by dashboard auto-refresh)."""
    try:
        api_key = flask_request.args.get("apiKey", "").strip() or None
        d = get_track(eid, api_key=api_key)
        if "error" in d: return jsonify(d), 404
        return jsonify(d)
    except Exception as e: return jsonify({"error": str(e)}), 500


# ── Odds-specific routes ──

@app.route("/api/odds/<int:eid>")
def r_odds(eid):
    """Get just the odds & value analysis for a match."""
    try:
        api_key = flask_request.args.get("apiKey", "").strip() or None
        d = get_event(eid)
        if not d: return jsonify({"error": "Not found"}), 404
        shots = get_shotmap(eid)
        odds = get_full_odds_analysis(d, shots, api_key=api_key)
        return jsonify(odds)
    except Exception as e:
        log.exception(f"Error in /api/odds/{eid}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/odds/sport/<sport_key>")
def r_odds_sport(sport_key):
    """Get raw odds for a sport key (for debugging/exploration)."""
    try:
        api_key = flask_request.args.get("apiKey", "").strip() or None
        data = get_odds_for_sport(sport_key, api_key=api_key)
        return jsonify({"sportKey": sport_key, "count": len(data), "events": data})
    except Exception as e: return jsonify({"error": str(e)}), 500


def _probe_key(key: str) -> int | None:
    """Probe a single API key against /sports endpoint and update _api_quotas."""
    global _api_requests_remaining
    if not key:
        return None
    try:
        import requests as _req
        r = _req.get(f"{ODDS_API_BASE}/sports", params={"apiKey": key}, timeout=5)
        remaining_hdr = r.headers.get("x-requests-remaining")
        used_hdr = r.headers.get("x-requests-used")
        if remaining_hdr is not None:
            rem = int(remaining_hdr)
            _api_quotas[key] = rem
            if key == ODDS_API_KEY:
                _api_requests_remaining = rem
            log.info(f"Quota probe [{key[:8]}…] — remaining: {rem}, used: {used_hdr}")
            return rem
    except Exception as e:
        log.warning(f"Quota probe failed for [{key[:8]}…]: {e}")
    return None


@app.route("/api/odds/quota")
def r_odds_quota():
    """
    Check remaining Odds API quota.
    - No params           → returns full rotation status (all keys + active one).
    - ?apiKey=...         → returns quota for that specific key.
    - ?probe=1            → also probe every cached-as-None key (uses 1 request per probed key).
    """
    global _api_requests_remaining
    api_key  = flask_request.args.get("apiKey", "").strip() or None
    do_probe = flask_request.args.get("probe", "").strip() in ("1", "true", "yes")

    # Specific key lookup
    if api_key:
        rem = _api_quotas.get(api_key)
        if rem is None:
            rem = _probe_key(api_key)
        return jsonify({"remaining": rem, "key": api_key[:8] + "…"})

    # Default: full rotation status
    keys_status = []
    for idx, k in enumerate(ODDS_API_KEYS):
        rem = _api_quotas.get(k)
        if rem is None and do_probe:
            rem = _probe_key(k)
        keys_status.append({
            "index":     idx,
            "key":       k[:8] + "…",
            "remaining": rem,
            "exhausted": rem is not None and rem < ODDS_API_KEY_THRESHOLD,
        })

    active_key = _active_odds_key()
    active_idx = ODDS_API_KEYS.index(active_key) if active_key in ODDS_API_KEYS else -1
    active_rem = _api_quotas.get(active_key)
    if active_rem is None:
        active_rem = _probe_key(active_key)

    return jsonify({
        # Back-compat fields (used by dashboard / older clients)
        "remaining": active_rem,
        "key":       (active_key or "")[:8] + "…" if active_key else None,
        # Rotation summary
        "active":    {"index": active_idx, "key": (active_key or "")[:8] + "…", "remaining": active_rem},
        "keys":      keys_status,
        "total":     len(ODDS_API_KEYS),
        "threshold": ODDS_API_KEY_THRESHOLD,
    })


@app.route("/api/odds/cache")
def r_odds_cache():
    """View cache status."""
    with _odds_cache_lock:
        status = {}
        now = time.time()
        for sport, cached in _odds_cache.items():
            age = now - cached["ts"]
            status[sport] = {
                "events": len(cached["data"]),
                "ageSeconds": round(age, 1),
                "isStale": age > ODDS_CACHE_TTL,
            }
    return jsonify({"cacheTtl": ODDS_CACHE_TTL, "sports": status})


@app.route("/api/odds/aliases")
def r_odds_aliases():
    """View team name alias database."""
    with _alias_lock:
        return jsonify({"count": len(_team_aliases), "aliases": _team_aliases})


@app.route("/api/benter")
def r_benter_table():
    """Return the Benter ratio table."""
    table = []
    for from_m, to_m, mw, bw in BENTER_TABLE:
        table.append({
            "fromMin": from_m, "toMin": to_m,
            "modelWeight": mw, "bookieWeight": bw,
        })
    return jsonify({"table": table})


@app.route("/api/intervals")
def r_intervals():
    """Return the interval adjustment table and the current factor for a given minute."""
    minute = flask_request.args.get("minute", type=int)
    table = []
    for from_m, to_m, pct, adjust in INTERVAL_ADJUSTS:
        table.append({
            "fromMin": from_m, "toMin": to_m,
            "goalsPercent": pct, "adjust": adjust,
            "isCurrent": (minute is not None and from_m <= minute <= to_m),
        })
    return jsonify({
        "table": table,
        "currentAdjust": get_interval_adjust(minute) if minute is not None else None,
        "note": "Adjust multiplied into remaining xG projection. Source: Premier League goals by 15-min segment.",
    })


@app.route("/api/odds/sports")
def r_odds_sports():
    """List all mapped sport keys and their Sofascore tournament names."""
    mapped = {}
    for tourn, sport_key in TOURNAMENT_TO_SPORT_KEY.items():
        if sport_key not in mapped:
            mapped[sport_key] = []
        mapped[sport_key].append(tourn)
    return jsonify({"count": len(mapped), "sports": mapped})

# ─────────────────────────────────────────────────────────────────────────────
# PATCH — adicionar ao server.py logo antes do bloco "# ── CLI Test ──"
# (ou seja, após o último @app.route existente)
#
# Adiciona um proxy para a The Odds API que:
#   • Evita o erro de CORS (pedido feito server-side, não pelo browser)
#   • Reencaminha os headers x-requests-remaining / x-requests-used
#   • Expõe: GET /proxy/odds/sports/<sport>/odds?...
#             GET /proxy/odds/sports            (lista de desportos)
# ─────────────────────────────────────────────────────────────────────────────

def _odds_get(path, params):
    """Fetch from The Odds API server-side and return (data, status_code, quota_headers)."""
    import requests as req_lib
    url = f"{ODDS_API_BASE}{path}"
    try:
        r = req_lib.get(url, params=params, timeout=15)
        quota = {
            "x-requests-remaining": r.headers.get("x-requests-remaining"),
            "x-requests-used":      r.headers.get("x-requests-used"),
        }
        if r.status_code == 200:
            return r.json(), 200, quota
        else:
            return {"error": r.text[:300], "status": r.status_code}, r.status_code, quota
    except Exception as e:
        return {"error": str(e)}, 500, {}


@app.route("/proxy/odds/sports")
def proxy_odds_sports():
    api_key = flask_request.args.get("apiKey", "")
    if not api_key:
        return jsonify({"error": "apiKey required"}), 400
    data, status, quota = _odds_get("/sports", {"apiKey": api_key})
    resp = jsonify(data)
    resp.status_code = status
    for k, v in quota.items():
        if v: resp.headers[k] = v
    return resp


@app.route("/proxy/odds/sports/<sport>/odds")
def proxy_odds_sport_odds(sport):
    api_key = flask_request.args.get("apiKey", "")
    if not api_key:
        return jsonify({"error": "apiKey required"}), 400
    params = {
        "apiKey":      api_key,
        "regions":     flask_request.args.get("regions", "eu"),
        "markets":     flask_request.args.get("markets", "h2h"),
        "oddsFormat":  flask_request.args.get("oddsFormat", "decimal"),
    }
    # optional filters
    for opt in ("eventIds", "bookmakers", "commenceTimeFrom", "commenceTimeTo"):
        v = flask_request.args.get(opt)
        if v: params[opt] = v

    data, status, quota = _odds_get(f"/sports/{sport}/odds", params)
    resp = jsonify(data)
    resp.status_code = status
    for k, v in quota.items():
        if v: resp.headers[k] = v
    log.info(f"[OddsProxy] {sport} → {status} | remaining={quota.get('x-requests-remaining')}")
    return resp


@app.route("/proxy/odds/sports/<sport>/events")
def proxy_odds_sport_events(sport):
    api_key = flask_request.args.get("apiKey", "")
    if not api_key:
        return jsonify({"error": "apiKey required"}), 400
    data, status, quota = _odds_get(f"/sports/{sport}/events", {"apiKey": api_key})
    resp = jsonify(data)
    resp.status_code = status
    for k, v in quota.items():
        if v: resp.headers[k] = v
    return resp

# ── CLI Test ──

def cli_test():
    print("=" * 60)
    print("  Sofascore xG Scraper v4 — Test")
    print("  + Live Odds & Benter Value Engine")
    print("=" * 60)

    print("\n[1/5] Init client...")
    ok = _init_client()
    if not ok:
        print("  FAIL — install: pip install curl_cffi")
        sys.exit(1)
    print(f"  OK → {_client_type}")

    print("\n[2/5] Live matches...")
    live = get_live()
    print(f"  {len(live)} live")
    for m in live[:8]:
        mn = f" {m['minute']}'" if m.get("minute") else ""
        print(f"  🔴 [{m['id']}] {m['homeTeam']} {m['homeGoals']}-{m['awayGoals']} {m['awayTeam']}{mn} — {m['tournament']}")

    print("\n[3/5] Today's schedule...")
    today = get_scheduled()
    fin = [m for m in today if m["isFinished"]]
    prog = [m for m in today if m["isLive"]]
    sched = [m for m in today if m["isScheduled"]]
    print(f"  {len(today)} total (🔴 {len(prog)} live, ✅ {len(fin)} finished, ⏰ {len(sched)} scheduled)")
    for m in (prog + fin)[:8]:
        icon = "🔴" if m["isLive"] else "✅"
        mn = f" {m['minute']}'" if m.get("minute") else ""
        print(f"  {icon} [{m['id']}] {m['homeTeam']} {m['homeGoals']}-{m['awayGoals']} {m['awayTeam']}{mn} — {m['tournament']}")

    TOP_KW = ["premier league", "serie a", "laliga", "la liga", "bundesliga", "ligue 1",
              "liga portugal", "eredivisie", "champions league", "europa league",
              "championship", "süper lig", "primeira liga", "mls", "brasileir",
              "libertadores", "sudamericana", "copa libertadores"]

    def is_top(m):
        t = (m.get("tournament") or "").lower()
        return any(k in t for k in TOP_KW)

    all_pool = live + [m for m in today if m["isFinished"]] + [m for m in today if m["isLive"]]
    top = sorted([m for m in all_pool if is_top(m)],
                 key=lambda m: m.get("homeGoals", 0) + m.get("awayGoals", 0), reverse=True)
    pool = top + [m for m in all_pool if not is_top(m)]

    if pool:
        print(f"\n[4/5] Testing xG shot map ({len(top)} top-league matches)...")
        found_match = None
        for t in pool[:10]:
            tag = "⭐" if is_top(t) else "  "
            print(f"\n  {tag} [{t['id']}] {t['homeTeam']} {t['homeGoals']}-{t['awayGoals']} {t['awayTeam']} — {t['tournament']}")
            shots = get_shotmap(t["id"])
            print(f"     Home xG: {shots['homeXg']:.3f} ({len(shots['homeShots'])} shots) | Away xG: {shots['awayXg']:.3f} ({len(shots['awayShots'])} shots)")

            if shots["totalShots"] > 0:
                for lbl, arr in [("Home", shots["homeShots"]), ("Away", shots["awayShots"])]:
                    if arr:
                        print(f"\n     {lbl}:")
                        for s in arr:
                            g = "⚽" if s["isGoal"] else "  "
                            at = f"+{s['addedTime']}" if s["addedTime"] else ""
                            print(f"       {g} {s['minute']}'{at}  {s['player']:22s}  xG={s['xg']:.4f}  {s['situation']}/{s['bodyPart']}")

                inc = get_incidents(t["id"])
                print(f"\n     {len(inc['goals'])} goals, {len(inc['cards'])} cards, {inc['redCards']} red")
                for g in inc["goals"]:
                    sd = "H" if g["isHome"] else "A"
                    at2 = f"+{g['addedTime']}" if g.get("addedTime") else ""
                    print(f"       ⚽ {g['minute']}'{at2} [{sd}] {g['player']}")
                print(f"\n  ✓ xG SCRAPING WORKS — {shots['totalShots']} shots")
                found_match = t
                break
            else:
                print(f"     (no xG — minor league or not started)")

        # Test odds integration
        print(f"\n[5/5] Testing Live Odds & Benter Value...")
        test_target = found_match or (top[0] if top else (pool[0] if pool else None))
        if test_target and is_top(test_target):
            print(f"\n  Testing odds for: {test_target['homeTeam']} vs {test_target['awayTeam']} ({test_target['tournament']})")
            shots = get_shotmap(test_target["id"])
            odds = get_full_odds_analysis(test_target, shots)

            if odds and odds.get("available"):
                print(f"  ✓ Odds found!")
                tm = odds.get("teamMapping", {})
                print(f"    Home: {tm.get('home', {}).get('sofascore')} → {tm.get('home', {}).get('oddsApi')} (conf: {tm.get('home', {}).get('confidence')})")
                print(f"    Away: {tm.get('away', {}).get('sofascore')} → {tm.get('away', {}).get('oddsApi')} (conf: {tm.get('away', {}).get('confidence')})")

                h2h = odds.get("h2h")
                if h2h:
                    ro = h2h.get("rawOdds", {})
                    print(f"    1X2: Home={ro.get('home', '-')} Draw={ro.get('draw', '-')} Away={ro.get('away', '-')} ({h2h.get('bookmakerTitle', '?')})")

                benter = odds.get("benter")
                if benter and benter.get("h2h"):
                    bh = benter["h2h"]
                    print(f"    Benter ({bh.get('minute', '?')}′): model={bh['benterWeights']['model']:.0%} bookie={bh['benterWeights']['bookie']:.0%}")
                    for out, data in bh.get("outcomes", {}).items():
                        val_icon = "✅" if data.get("isValue") else "  "
                        print(f"      {val_icon} {out}: blend={data['blendedProb']:.1%} odds={data['bookieOdds']} edge={data['edge']:+.1f}%")
            else:
                reason = odds.get("reason", "unknown") if odds else "no response"
                print(f"  ⚠ No odds: {reason}")
        else:
            print("  ⚠ No top-league match to test odds with.")
    else:
        print("\n[4/5] No matches to test")
        print("[5/5] Skipped (no matches)")

    print(f"\n{'='*60}")
    print(f"  Client: {_client_type}")
    print(f"  Server: python3 server.py → http://localhost:5050")
    print(f"  Endpoints:")
    print(f"    GET /api/match/<eid>     — full match + odds + value")
    print(f"    GET /api/odds/<eid>      — odds & value only")
    print(f"    GET /api/track/<eid>     — auto-refresh with odds")
    print(f"    GET /api/odds/quota      — API quota remaining")
    print(f"    GET /api/odds/cache      — cache status")
    print(f"    GET /api/odds/aliases    — team name DB")
    print(f"    GET /api/benter          — Benter ratio table")
    print(f"{'='*60}")


# ════════════════════════════════════════════════════════════
#  BACKGROUND ENGINE — Pre-computes analysis every 2 minutes
#  Only runs for monitored leagues with live games
#  Budget: ~2 requests/sport_key/cycle (h2h + totals)
#  Spreads fetched on the same cycle (3 req total)
# ════════════════════════════════════════════════════════════

# The set of sport keys we actively monitor.
# Only games in these leagues trigger odds fetches.
MONITORED_SPORT_KEYS = {
    # England
    "soccer_epl", "soccer_efl_champ",
    # Top 5 European domestic
    "soccer_spain_la_liga", "soccer_italy_serie_a",
    "soccer_germany_bundesliga", "soccer_germany_bundesliga2", "soccer_france_ligue_one",
    # Other European domestic
    "soccer_portugal_primeira_liga", "soccer_netherlands_eredivisie",
    "soccer_belgium_first_div", "soccer_turkey_super_league",
    "soccer_austria_bundesliga", "soccer_spl",
    "soccer_greece_super_league",
    "soccer_sweden_allsvenskan", "soccer_norway_eliteserien", "soccer_denmark_superliga",
    "soccer_switzerland_superleague", "soccer_poland_ekstraklasa",
    "soccer_finland_veikkausliiga", "soccer_russia_premier_league",
    "soccer_league_of_ireland", "soccer_saudi_arabia_pro_league",
    # UEFA
    "soccer_uefa_champs_league", "soccer_uefa_champs_league_qualification",
    "soccer_uefa_champs_league_women",
    "soccer_uefa_europa_league", "soccer_uefa_europa_conference_league",
    "soccer_uefa_european_championship", "soccer_uefa_euro_qualification",
    "soccer_uefa_nations_league",
    # FIFA
    "soccer_fifa_world_cup", "soccer_fifa_world_cup_qualifiers_europe",
    "soccer_fifa_world_cup_qualifiers_south_america",
    "soccer_fifa_world_cup_womens", "soccer_fifa_club_world_cup",
    # Americas
    "soccer_usa_mls",
    "soccer_brazil_campeonato", "soccer_brazil_serie_b",
    "soccer_chile_campeonato", "soccer_mexico_ligamx",
    "soccer_conmebol_copa_libertadores", "soccer_conmebol_copa_sudamericana",
    "soccer_conmebol_copa_america",
    # Asia
    "soccer_japan_j_league",
}

BG_INTERVAL   = 120   # seconds between cycles (2 minutes)
ODDS_MIN_PICK = 1.40  # minimum odds to flag as value pick
ODDS_MAX_PICK = 4.00  # maximum odds to flag as value pick

# ── In-memory live state (rebuilt every cycle) ──
_live_state: dict = {}      # match_id → {match, shots, incidents, odds, tips, ts}
_state_lock = threading.Lock()
_last_cycle_ts = 0.0
_last_cycle_req = 0

# ── SQLite persistence ──
import sqlite3, pathlib

DB_PATH = pathlib.Path(os.environ.get("DB_PATH", "/data/tips.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def _init_db():
    with _db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS games (
            id            INTEGER PRIMARY KEY,
            home_team     TEXT NOT NULL,
            away_team     TEXT NOT NULL,
            home_goals    INTEGER DEFAULT 0,
            away_goals    INTEGER DEFAULT 0,
            tournament    TEXT,
            country       TEXT,
            is_finished   INTEGER DEFAULT 0,
            archived_at   INTEGER,
            start_ts      INTEGER,
            home_team_id  INTEGER,
            away_team_id  INTEGER
        );
        CREATE TABLE IF NOT EXISTS tips (
            tip_key      TEXT NOT NULL,
            match_id     INTEGER NOT NULL,
            market       TEXT NOT NULL,
            label        TEXT NOT NULL,
            odd_entry    REAL,
            odd_now      REAL,
            edge_entry   REAL,
            minute_entry INTEGER,
            wall_ts      INTEGER NOT NULL,
            result       TEXT DEFAULT NULL,
            PRIMARY KEY (match_id, tip_key),
            FOREIGN KEY (match_id) REFERENCES games(id)
        );
        CREATE INDEX IF NOT EXISTS idx_tips_match ON tips(match_id);
        CREATE INDEX IF NOT EXISTS idx_games_finished ON games(is_finished);
        CREATE TABLE IF NOT EXISTS tg_subscribers (
            chat_id       INTEGER PRIMARY KEY,
            username      TEXT,
            first_name    TEXT,
            subscribed_at INTEGER NOT NULL,
            active        INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS tg_starts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id      INTEGER NOT NULL,
            username     TEXT,
            first_name   TEXT,
            start_param  TEXT,
            started_at   INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tg_starts_chat ON tg_starts(chat_id);
        CREATE INDEX IF NOT EXISTS idx_tg_starts_param ON tg_starts(start_param);
        CREATE INDEX IF NOT EXISTS idx_tg_starts_ts ON tg_starts(started_at);
        CREATE TABLE IF NOT EXISTS settings (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at INTEGER,
            updated_by TEXT
        );
        CREATE TABLE IF NOT EXISTS competitions (
            sport_key       TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            country         TEXT,
            priority        INTEGER DEFAULT 99,
            active_algo     INTEGER DEFAULT 1,
            active_frontend INTEGER DEFAULT 1,
            updated_at      INTEGER
        );
        CREATE TABLE IF NOT EXISTS match_shots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id    INTEGER NOT NULL,
            minute      INTEGER NOT NULL,
            added_time  INTEGER DEFAULT 0,
            is_home     INTEGER NOT NULL,
            xg          REAL NOT NULL,
            is_goal     INTEGER DEFAULT 0,
            is_penalty  INTEGER DEFAULT 0,
            player      TEXT,
            shot_type   TEXT,
            situation   TEXT,
            body_part   TEXT,
            recorded_at INTEGER NOT NULL,
            UNIQUE(match_id, minute, added_time, is_home, player)
        );
        CREATE INDEX IF NOT EXISTS idx_match_shots_match ON match_shots(match_id);
        CREATE INDEX IF NOT EXISTS idx_match_shots_minute ON match_shots(match_id, minute);
        """)
    # Migration: add edge_entry column to existing DBs
    with _db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tips)").fetchall()]
        if "edge_entry" not in cols:
            conn.execute("ALTER TABLE tips ADD COLUMN edge_entry REAL")
            log.info("DB migration: added edge_entry column to tips")
        if "xg_home_at_entry" not in cols:
            conn.execute("ALTER TABLE tips ADD COLUMN xg_home_at_entry REAL")
            log.info("DB migration: added xg_home_at_entry column to tips")
        if "xg_away_at_entry" not in cols:
            conn.execute("ALTER TABLE tips ADD COLUMN xg_away_at_entry REAL")
            log.info("DB migration: added xg_away_at_entry column to tips")

    # Migration: change PRIMARY KEY from tip_key alone to (match_id, tip_key)
    # Bug: when 2 different games produced picks with same market+label
    # (e.g. "1X2|Empate"), 2nd game's INSERT failed silently with
    # UNIQUE constraint, dropping all subsequent same-key tips.
    with _db() as conn:
        pk_info = conn.execute("PRAGMA table_info(tips)").fetchall()
        pk_cols = sorted([r[1] for r in pk_info if r[5] > 0])  # rows with pk index > 0
        if pk_cols == ["tip_key"]:
            log.info("DB migration: rebuilding tips with composite PK (match_id, tip_key)")
            conn.executescript("""
                CREATE TABLE tips_new (
                    tip_key      TEXT NOT NULL,
                    match_id     INTEGER NOT NULL,
                    market       TEXT NOT NULL,
                    label        TEXT NOT NULL,
                    odd_entry    REAL,
                    odd_now      REAL,
                    edge_entry   REAL,
                    minute_entry INTEGER,
                    wall_ts      INTEGER NOT NULL,
                    result       TEXT DEFAULT NULL,
                    PRIMARY KEY (match_id, tip_key),
                    FOREIGN KEY (match_id) REFERENCES games(id)
                );
                INSERT INTO tips_new (tip_key, match_id, market, label, odd_entry,
                                      odd_now, edge_entry, minute_entry, wall_ts, result)
                SELECT tip_key, match_id, market, label, odd_entry,
                       odd_now, edge_entry, minute_entry, wall_ts, result
                FROM tips;
                DROP TABLE tips;
                ALTER TABLE tips_new RENAME TO tips;
                CREATE INDEX IF NOT EXISTS idx_tips_match ON tips(match_id);
            """)
            log.info("DB migration: tips table rebuilt with composite PK")
    log.info(f"DB ready: {DB_PATH}")


def _persist_shots(match_id: int, shots: dict) -> None:
    """
    Persist shot events for a live match to match_shots table.
    Called every background cycle — uses INSERT OR IGNORE so duplicates are safe.
    Only stores shots with xg > 0 (filters noise like blocked shots with no xG value).
    """
    if not shots:
        return
    now_ts = int(time.time())
    rows = []
    for shot in shots.get("homeShots", []) + shots.get("awayShots", []):
        if shot.get("xg", 0) <= 0:
            continue
        rows.append((
            match_id,
            shot.get("minute", 0),
            shot.get("addedTime", 0),
            1 if shot.get("isHome") else 0,
            shot["xg"],
            1 if shot.get("isGoal") else 0,
            1 if shot.get("isPenalty") else 0,
            (shot.get("player") or "")[:80],
            (shot.get("shotType") or "")[:40],
            (shot.get("situation") or "")[:40],
            (shot.get("bodyPart") or "")[:40],
            now_ts,
        ))
    if not rows:
        return
    try:
        with _db() as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO match_shots
                    (match_id, minute, added_time, is_home, xg, is_goal, is_penalty,
                     player, shot_type, situation, body_part, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
        log.debug(f"_persist_shots: match {match_id} — {len(rows)} shots upserted")
    except Exception as e:
        log.warning(f"_persist_shots failed for match {match_id}: {e}")


# ════════════════════════════════════════════════════════════
#  DYNAMIC SETTINGS — read from DB, cached, hot-reloadable
# ════════════════════════════════════════════════════════════

ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "liveedge-admin-2024")
SETTINGS_RELOAD_INTERVAL = 30  # seconds — short so admin changes apply quickly

_settings_cache: dict = {}
_settings_last_load = 0.0
_settings_lock = threading.Lock()

# Defaults — used when key not present in DB
_SETTINGS_DEFAULTS = {
    "min_odds":              1.40,
    "max_odds":              4.00,
    "min_edge_pct":          10.0,   # value > 0.10 → edge >= 10%
    "stake_per_bet":         100.0,
    "min_minute_for_tips":   25,
    "max_minute_for_tips":   85,
    "goal_cooldown_minutes": 4,
    "hcp_min_gap_minutes":   8,
    "odds_max_age_seconds":  120,
    "bg_interval_seconds":   120,
    "blocked_keywords":      "",     # comma-separated extra tournament fragments to block
}

def _coerce_setting(key: str, raw: str):
    """Convert string from DB to int/float/str based on the default's type."""
    default = _SETTINGS_DEFAULTS.get(key)
    if isinstance(default, str):
        return raw  # keep as string (e.g. blocked_keywords)
    if isinstance(default, int) and not isinstance(default, bool):
        try: return int(float(raw))
        except: return default
    if isinstance(default, float):
        try: return float(raw)
        except: return default
    return raw

def _load_settings(force: bool = False) -> dict:
    """Load settings from DB into cache. Refreshes every SETTINGS_RELOAD_INTERVAL seconds."""
    global _settings_cache, _settings_last_load
    now = time.time()
    with _settings_lock:
        if force or (now - _settings_last_load) > SETTINGS_RELOAD_INTERVAL:
            try:
                with _db() as conn:
                    rows = conn.execute("SELECT key, value FROM settings").fetchall()
                    _settings_cache = {r["key"]: _coerce_setting(r["key"], r["value"]) for r in rows}
                _settings_last_load = now
            except Exception as e:
                log.warning(f"settings load error: {e}")
        return _settings_cache

def get_setting(key: str, default=None):
    """Get a setting value. Falls back to _SETTINGS_DEFAULTS, then provided default."""
    settings = _load_settings()
    if key in settings:
        return settings[key]
    if key in _SETTINGS_DEFAULTS:
        return _SETTINGS_DEFAULTS[key]
    return default

def _check_admin_auth() -> bool:
    """Validate Bearer token matches ADMIN_SECRET."""
    auth = flask_request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
        return token == ADMIN_SECRET
    return False


def _upsert_game(match: dict):
    """Insert or update a game record."""
    with _db() as conn:
        # Migrate: add team ID columns if missing
        cols = {r[1] for r in conn.execute("PRAGMA table_info(games)")}
        if "home_team_id" not in cols:
            conn.execute("ALTER TABLE games ADD COLUMN home_team_id INTEGER")
        if "away_team_id" not in cols:
            conn.execute("ALTER TABLE games ADD COLUMN away_team_id INTEGER")
        conn.execute("""
            INSERT INTO games (id, home_team, away_team, home_goals, away_goals,
                               tournament, country, is_finished, start_ts,
                               home_team_id, away_team_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                home_goals   = excluded.home_goals,
                away_goals   = excluded.away_goals,
                is_finished  = excluded.is_finished,
                home_team_id = COALESCE(excluded.home_team_id, games.home_team_id),
                away_team_id = COALESCE(excluded.away_team_id, games.away_team_id)
        """, (
            match["id"], match["homeTeam"], match["awayTeam"],
            match["homeGoals"], match["awayGoals"],
            match.get("tournament"), match.get("country"),
            1 if match.get("isFinished") else 0,
            match.get("startTimestamp"),
            match.get("homeTeamId"), match.get("awayTeamId"),
        ))
        if match.get("isFinished"):
            conn.execute(
                "UPDATE games SET archived_at = ? WHERE id = ? AND archived_at IS NULL",
                (int(time.time()), match["id"])
            )

GOAL_COOLDOWN_MINUTES = 4   # block new tips for this many minutes after a goal
HCP_MIN_GAP_MINUTES   = 8   # minimum minutes between HCP tips for the same team
MIN_MINUTE_FOR_TIPS   = 25  # minimum match minute before tips can be generated
MAX_MINUTE_FOR_TIPS   = 85  # maximum match minute before tips can be generated (avoid late-game chaos)
MAX_TIPS_PER_GAME     = 6   # hard cap on tips per game
ODDS_MAX_AGE_SECONDS  = 120 # block picks if odds lastUpdate is older than this (2 minutes)

def _hcp_canonical(label: str) -> str:
    """Normalise HCP label for dedup: strip trailing .0, lowercase team prefix."""
    import re as _re
    m = _re.search(r'([+-][\d.]+)$', label)
    if not m:
        return label.lower()
    value = float(m.group(1))
    # Format as int if whole number, else keep decimal
    val_str = str(int(value)) if value == int(value) else str(value)
    team = label[:label.rfind(m.group(0))].strip().lower()
    return f"{team}|{val_str}"

def _filter_redundant_live_picks(picks: list) -> list:
    """
    Strip equivalent picks from a candidate list before exposing them as
    live "VALUE PICKS NOW" to the frontend. Mirrors the cross-market dedup
    that _sync_tips_db applies before committing to DB:

      - 1X2 'Team' is equivalent to Handicap 'Team -0.5' (same outcome).
        When both are present, keep the higher-edge one.
      - O/U Over X.5 contradicts Under Y.5 when Y <= X.
        When both are present, keep the higher-edge one.

    Pure function — does not touch DB. Returns a new list.
    """
    import re as _re
    if not picks:
        return picks

    # Build keyed views
    team_picks: dict[str, list[int]] = {}   # team_lower → list of indices into picks
    for i, p in enumerate(picks):
        market = p.get("market", "")
        label  = (p.get("label") or "").strip()
        team_key = None
        if market == "1X2" and label.lower() != "draw":
            team_key = label.lower()
        elif market == "Handicap":
            m = _re.search(r'([+-][\d.]+)$', label)
            if m:
                try:
                    val = float(m.group(1))
                except ValueError:
                    val = None
                # Only -0.5 handicap is equivalent to a straight 1X2 win
                if val == -0.5:
                    team_key = label[:label.rfind(m.group(0))].strip().lower()
        if team_key:
            team_picks.setdefault(team_key, []).append(i)

    drop: set[int] = set()
    for team, idxs in team_picks.items():
        if len(idxs) <= 1:
            continue
        # Keep the one with the largest edge; drop the rest
        idxs_sorted = sorted(idxs, key=lambda i: -(picks[i].get("edge") or 0))
        for i in idxs_sorted[1:]:
            drop.add(i)

    # O/U cross-line: same direction is fine, but Over X.5 + Under Y.5 with Y<=X is contradictory
    overs:  list[tuple[int, float]] = []   # (index, line)
    unders: list[tuple[int, float]] = []
    for i, p in enumerate(picks):
        if i in drop:
            continue
        if not p.get("market", "").startswith("O/U"):
            continue
        m = _re.match(r'^(Over|Under)\s+([\d.]+)$', p.get("label", ""), _re.IGNORECASE)
        if not m:
            continue
        try:
            line = float(m.group(2))
        except ValueError:
            continue
        (overs if m.group(1).lower() == "over" else unders).append((i, line))

    for io, lo in overs:
        for iu, lu in unders:
            if lu <= lo and io not in drop and iu not in drop:
                # Contradictory pair — drop the one with the smaller edge
                eo = picks[io].get("edge") or 0
                eu = picks[iu].get("edge") or 0
                drop.add(iu if eu <= eo else io)

    return [p for i, p in enumerate(picks) if i not in drop]


def _sync_tips_db(match_id: int, picks: list, minute: int, odds: dict,
                  last_goal_minute=None, match: dict = None, shots: dict = None) -> list:
    """
    Sync server-computed picks into the DB.
    Returns the full tip list for this match (including historical).
    """
    import re as _re
    now_ts = int(time.time())

    # ── DIAGNOSTIC LOGGING ──
    # Track per-pick decisions so we can later understand why client-visible
    # picks (localStorage) sometimes never make it into the server DB.
    _decision_log = []   # list of (label, "ACCEPTED"|reason)
    _tag_summary = "TIP-SYNC"
    _home = (match.get("homeTeam") if match else "") or ""
    _away = (match.get("awayTeam") if match else "") or ""
    _match_label = f"{_home} vs {_away}".strip(" vs")
    if picks:
        log.info(
            f"[{_tag_summary}] match {match_id} ({_match_label}) "
            f"min={minute} lastGoal={last_goal_minute} "
            f"received {len(picks)} candidate pick(s): "
            + ", ".join(f"{p.get('market','?')}|{p.get('label','?')}@{p.get('odds','?')}/edge={p.get('edge','?')}" for p in picks)
        )

    def _reject(p, reason):
        _decision_log.append((f"{p.get('market','?')}|{p.get('label','?')}", reason))
        log.info(f"[{_tag_summary}] match {match_id} REJECT {p.get('market','?')}|{p.get('label','?')} — {reason}")

    def _accept(p):
        _decision_log.append((f"{p.get('market','?')}|{p.get('label','?')}", "ACCEPTED"))
        log.info(f"[{_tag_summary}] match {match_id} ACCEPT {p.get('market','?')}|{p.get('label','?')} @ {p.get('odds','?')} (edge={p.get('edge','?')})")

    # Goal cooldown: suppress NEW tip insertions within GOAL_COOLDOWN_MINUTES of a goal
    in_cooldown = (
        last_goal_minute is not None
        and minute is not None
        and minute > 0  # only check if we have a real minute value
        and (minute - last_goal_minute) >= 0  # goal was in past (same or earlier minute)
        and (minute - last_goal_minute) < get_setting("goal_cooldown_minutes", 4)
    )
    if in_cooldown:
        log.info(f"[{_tag_summary}] match {match_id}: goal cooldown active (goal@{last_goal_minute}', now@{minute}') — new picks will be blocked")

    with _db() as conn:
        # Pre-load all existing tips for this game
        existing_all = conn.execute(
            "SELECT tip_key, market, label, minute_entry FROM tips WHERE match_id = ?",
            (match_id,)
        ).fetchall()
        existing_keys      = {r["tip_key"] for r in existing_all}
        existing_hcp_rows  = [r for r in existing_all if r["market"] == "Handicap"]
        existing_hcp_canonical = {_hcp_canonical(r["label"]) for r in existing_hcp_rows}
        existing_1x2_rows  = [r for r in existing_all if r["market"] == "1X2"]

        # O/U conflict index — track Over/Under lines as numeric values for cross-line check.
        # Cross-line rule: Over X.5 contradicts Under Y.5 iff Y <= X
        #   e.g. Over 2.5 (3+ goals) contradicts Under 2.5 (≤2) and Under 1.5 (≤1) but NOT Under 3.5 (≤3, overlap)
        existing_overs  = []   # list of float(X) for stored "Over X.5"
        existing_unders = []   # list of float(Y) for stored "Under Y.5"
        for r in existing_all:
            if r["market"].startswith("O/U"):
                m_ou = _re.match(r'^(Over|Under)\s+([\d.]+)$', r["label"], _re.IGNORECASE)
                if m_ou:
                    try:
                        line_val = float(m_ou.group(2))
                    except ValueError:
                        continue
                    if m_ou.group(1).lower() == "over":
                        existing_overs.append(line_val)
                    else:
                        existing_unders.append(line_val)

        total_tips = len(existing_all)

        # ── Pre-filter: keep only best-edge pick per team direction within this cycle ──
        # Determines if a pick is in favour of home or away team
        def _pick_direction(p, match):
            """Returns 'home', 'away', or None."""
            home = (match.get("homeTeam") or "").lower()
            away = (match.get("awayTeam") or "").lower()
            lbl  = (p.get("label") or "").lower()
            mkt  = p.get("market", "")
            if mkt == "1X2":
                # label is team name
                if home and (home.split()[0] in lbl or lbl in home):
                    return "home"
                if away and (away.split()[0] in lbl or lbl in away):
                    return "away"
                return "draw" if "empate" in lbl or "draw" in lbl else None
            if mkt == "Handicap":
                hm = _re.search(r'([+-][\d.]+)$', p["label"])
                if not hm:
                    return None
                team_lbl = p["label"][:p["label"].rfind(hm.group(0))].strip().lower()
                if home and (home.split()[0] in team_lbl or team_lbl in home):
                    return "home"
                if away and (away.split()[0] in team_lbl or team_lbl in away):
                    return "away"
            return None

        # Within this cycle: for each direction keep only the pick with highest edge
        if match:
            dir_best: dict = {}  # direction → best pick so far
            for p in picks:
                d = _pick_direction(p, match)
                if d is None:
                    continue
                cur = dir_best.get(d)
                if cur is None or (p.get("edge") or 0) > (cur.get("edge") or 0):
                    dir_best[d] = p
            # Rebuild picks: only best per direction + picks with no direction (e.g. O/U)
            picks_with_dir  = {id(p) for p in dir_best.values()}
            picks_no_dir    = [p for p in picks if _pick_direction(p, match) is None]
            new_picks       = list(dir_best.values()) + picks_no_dir
            # DIAG: log picks dropped by direction-best filter
            kept_ids = {id(p) for p in new_picks}
            for p in picks:
                if id(p) not in kept_ids:
                    d = _pick_direction(p, match)
                    _reject(p, f"dropped by dir-best filter (direction={d}, lower edge than best in same direction)")
            picks = new_picks

        # ── Block: if a pick in the same direction already exists since the last goal, skip whole cycle ──
        phase_cutoff_global = (last_goal_minute or 0)
        if match:
            active_directions = set()
            for r in existing_all:
                if (r["minute_entry"] or 0) >= phase_cutoff_global:
                    fake = {"market": r["market"], "label": r["label"], "edge": 0}
                    d = _pick_direction(fake, match)
                    if d and d != "draw":
                        active_directions.add(d)
            # Remove picks whose direction is already active (same phase, no goal)
            new_picks = [p for p in picks if _pick_direction(p, match) not in active_directions
                         or p.get("market", "").startswith("O/U")]
            # DIAG: log picks dropped by active-directions filter
            kept_ids = {id(p) for p in new_picks}
            for p in picks:
                if id(p) not in kept_ids:
                    d = _pick_direction(p, match)
                    _reject(p, f"direction '{d}' already has tip in current phase (cutoff={phase_cutoff_global}')")
            picks = new_picks

        for p in picks:
            key = f"{p['market']}|{p['label']}"

            if key in existing_keys:
                # Tip already stored — update current odd if still open
                conn.execute(
                    "UPDATE tips SET odd_now = ? WHERE tip_key = ? AND match_id = ? AND result IS NULL",
                    (p["odds"], key, match_id)
                )
                continue

            # ── All checks below only apply to brand-new tips ──

            # Hard cap
            if total_tips >= MAX_TIPS_PER_GAME:
                _reject(p, f"tip cap reached ({total_tips} >= MAX_TIPS_PER_GAME={MAX_TIPS_PER_GAME})")
                continue

            # Goal cooldown
            if in_cooldown:
                _reject(p, f"goal cooldown (goal@{last_goal_minute}', now@{minute}', window={GOAL_COOLDOWN_MINUTES}')")
                continue

            # Minimum minute threshold
            _min_min = get_setting("min_minute_for_tips", 25)
            if minute is not None and minute < _min_min:
                _reject(p, f"below MIN_MINUTE_FOR_TIPS (minute={minute} < {_min_min})")
                continue

            # Maximum minute threshold (avoid late-game chaos)
            _max_min = get_setting("max_minute_for_tips", 85)
            if minute is not None and minute > _max_min:
                _reject(p, f"above MAX_MINUTE_FOR_TIPS (minute={minute} > {_max_min})")
                continue

            # O/U conflict: block any contradictory line, not only the same one.
            # Over X.5 ⇄ Under Y.5 are contradictory iff Y <= X (no overlap of viable totals).
            if p["market"].startswith("O/U"):
                m_ou = _re.match(r'^(Over|Under)\s+([\d.]+)$', p["label"], _re.IGNORECASE)
                if m_ou:
                    try:
                        new_line = float(m_ou.group(2))
                    except ValueError:
                        new_line = None
                    if new_line is not None:
                        direction = m_ou.group(1).lower()
                        if direction == "over":
                            # Over X.5 conflicts with any Under Y.5 stored where Y <= X
                            conflicts = [y for y in existing_unders if y <= new_line]
                            if conflicts:
                                _reject(p, f"O/U cross-line conflict: Over {new_line} contradicts existing Under {min(conflicts)}")
                                continue
                        else:
                            # Under Y.5 conflicts with any Over X.5 stored where X >= Y
                            conflicts = [x for x in existing_overs if x >= new_line]
                            if conflicts:
                                _reject(p, f"O/U cross-line conflict: Under {new_line} contradicts existing Over {max(conflicts)}")
                                continue

            # 1X2 ↔ HCP conflict: same team, same phase (no goal between them)
            phase_cutoff = (last_goal_minute or 0)
            if p["market"] == "Handicap":
                hm_new = _re.search(r'([+-][\d.]+)$', p["label"])
                if hm_new:
                    team_part = p["label"][:p["label"].rfind(hm_new.group(0))].strip().lower()
                    if any(r["label"].strip().lower() == team_part and (r["minute_entry"] or 0) >= phase_cutoff for r in existing_1x2_rows):
                        _reject(p, f"HCP↔1X2 conflict: 1X2 for '{team_part}' already in this phase (cutoff={phase_cutoff}')")
                        continue
            if p["market"] == "1X2":
                team_part_1x2 = p["label"].strip().lower()
                if any(
                    (_re.search(r'([+-][\d.]+)$', r["label"]) and
                     r["label"][:r["label"].rfind(_re.search(r'([+-][\d.]+)$', r["label"]).group(0))].strip().lower() == team_part_1x2 and
                     (r["minute_entry"] or 0) >= phase_cutoff)
                    for r in existing_hcp_rows
                ):
                    _reject(p, f"1X2↔HCP conflict: HCP for '{team_part_1x2}' already in this phase (cutoff={phase_cutoff}')")
                    continue

            # HCP dedup: same canonical value already stored
            if p["market"] == "Handicap":
                canon = _hcp_canonical(p["label"])
                if canon in existing_hcp_canonical:
                    _reject(p, f"duplicate HCP (canonical='{canon}' already stored)")
                    continue

                # HCP gap: same team, less than HCP_MIN_GAP_MINUTES ago
                hm = _re.search(r'([+-][\d.]+)$', p["label"])
                if hm:
                    team_part = p["label"][:p["label"].rfind(hm.group(0))].strip().lower()
                    skip_due_to_gap = False
                    for r in existing_hcp_rows:
                        rt = r["label"][:r["label"].rfind(_re.search(r'([+-][\d.]+)$', r["label"]).group(0))].strip().lower() \
                             if _re.search(r'([+-][\d.]+)$', r["label"]) else ""
                        if rt == team_part and r["minute_entry"] is not None:
                            gap = (minute or 0) - r["minute_entry"]
                            _hcp_gap = get_setting("hcp_min_gap_minutes", 8)
                            if 0 <= gap < _hcp_gap:
                                _reject(p, f"HCP gap too small (same team '{team_part}' tipped {gap}' ago, min={_hcp_gap}')")
                                skip_due_to_gap = True
                                break
                    if skip_due_to_gap:
                        continue

            try:
                _xg_home = (shots or {}).get("homeXg") if shots else None
                _xg_away = (shots or {}).get("awayXg") if shots else None
                conn.execute("""
                    INSERT INTO tips (tip_key, match_id, market, label,
                                      odd_entry, odd_now, edge_entry, minute_entry, wall_ts,
                                      xg_home_at_entry, xg_away_at_entry)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (key, match_id, p["market"], p["label"],
                      p["odds"], p["odds"], p.get("edge"), minute, now_ts,
                      _xg_home, _xg_away))
                existing_keys.add(key)
                total_tips += 1
                _accept(p)
                # SSE broadcast to web clients
                if match:
                    try:
                        _broadcast_pick(match, p, minute)
                    except Exception as sse_err:
                        log.error(f"SSE broadcast failed: {sse_err}")
                # Telegram notification for new tip
                if match:
                    try:
                        _send_telegram(_format_pick_alert(match, p, minute, shots=shots))
                    except Exception as tg_err:
                        log.error(f"Telegram alert failed: {tg_err}")
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    # Should never happen now (composite PK is per-match) but log
                    # loudly so future regressions surface immediately.
                    _reject(p, f"UNIQUE constraint failed (composite PK violation: {e})")
                    log.warning(f"[{_tag_summary}] match {match_id}: UNIQUE collision for {key} — {e}")
                else:
                    raise
            if p["market"] == "Handicap":
                existing_hcp_canonical.add(_hcp_canonical(p["label"]))
                existing_hcp_rows.append({"label": p["label"], "minute_entry": minute,
                                          "market": "Handicap", "tip_key": key})
            if p["market"] == "1X2":
                existing_1x2_rows.append({"label": p["label"], "minute_entry": minute,
                                          "market": "1X2", "tip_key": key})
            if p["market"].startswith("O/U"):
                m_ou = _re.match(r'^(Over|Under)\s+([\d.]+)$', p["label"], _re.IGNORECASE)
                if m_ou:
                    try:
                        new_line_acc = float(m_ou.group(2))
                        if m_ou.group(1).lower() == "over":
                            existing_overs.append(new_line_acc)
                        else:
                            existing_unders.append(new_line_acc)
                    except ValueError:
                        pass

        # DIAG: summary line per cycle (only when there were candidates)
        if _decision_log:
            from collections import Counter
            counts = Counter(reason for _, reason in _decision_log)
            accepted = counts.pop("ACCEPTED", 0)
            rej_summary = "; ".join(f"{n}× {r}" for r, n in counts.most_common())
            log.info(
                f"[{_tag_summary}] match {match_id} ({_match_label}) SUMMARY: "
                f"{accepted} accepted, {sum(counts.values())} rejected"
                + (f" — {rej_summary}" if rej_summary else "")
            )

        # Auto-resolve based on current state
        all_tips = conn.execute(
            "SELECT * FROM tips WHERE match_id = ?", (match_id,)
        ).fetchall()
        return [dict(t) for t in all_tips]


def _auto_resolve_db(match_id: int, match: dict, inc: dict):
    """Auto-resolve tips in DB based on current score/status."""
    hg = match.get("homeGoals", 0)
    ag = match.get("awayGoals", 0)
    total = hg + ag
    finished = match.get("isFinished", False)

    with _db() as conn:
        tips = conn.execute(
            "SELECT * FROM tips WHERE match_id = ? AND result IS NULL", (match_id,)
        ).fetchall()
        for t in tips:
            lbl = t["label"]; mkt = t["market"]
            new_result = None

            # O/U totals
            import re as _re
            om = _re.match(r'^Over\s+([\d.]+)$', lbl, _re.IGNORECASE)
            um = _re.match(r'^Under\s+([\d.]+)$', lbl, _re.IGNORECASE)
            if om:
                line = float(om.group(1))
                if total > line:          new_result = "green"
                elif finished:            new_result = "red"
            elif um:
                line = float(um.group(1))
                if total > line:          new_result = "red"
                elif finished:            new_result = "green"

            # 1X2 — only at FT
            if mkt == "1X2" and finished:
                ft = "home" if hg > ag else ("draw" if hg == ag else "away")
                out_map = {"home": match.get("homeTeam",""), "draw": "Draw", "away": match.get("awayTeam","")}
                for side, name in out_map.items():
                    if lbl.lower() in name.lower() or (len(lbl) > 3 and name.lower().startswith(lbl[:4].lower())):
                        new_result = "green" if side == ft else "red"
                        break

            # HCP — only at FT
            if mkt == "Handicap" and finished and new_result is None:
                hm = _re.search(r'([+-][\d.]+)$', lbl)
                if hm:
                    hcp = float(hm.group(1))
                    team_part = lbl[:lbl.rfind(hm.group(0))].strip()
                    home_name = match.get("homeTeam", "")
                    is_home = team_part.lower() in home_name.lower() or \
                              (len(team_part) > 3 and home_name.lower().startswith(team_part[:4].lower()))
                    margin = (hg - ag) if is_home else (ag - hg)
                    adj = margin + hcp
                    new_result = "green" if adj > 0 else ("red" if adj < 0 else "void")

            if new_result:
                conn.execute(
                    "UPDATE tips SET result = ? WHERE tip_key = ? AND match_id = ?",
                    (new_result, t["tip_key"], match_id)
                )

def _odds_are_stale(odds: dict) -> tuple:
    """Return (is_stale, age_seconds) based on the most recent lastUpdate across all markets.

    Picks should be blocked when odds are older than ODDS_MAX_AGE_SECONDS (default 120s).
    Returns (False, None) when odds are unavailable or have no timestamps (non-blocking).
    """
    if not odds or not odds.get("available"):
        return False, None  # no odds → upstream already handles this

    now = datetime.now(timezone.utc)
    latest_ts = None

    for market_key in ("h2h", "totals", "spreads"):
        mkt = odds.get(market_key)
        if not mkt:
            continue
        lu = mkt.get("lastUpdate")
        if not lu:
            continue
        try:
            lu_dt = datetime.fromisoformat(lu.replace("Z", "+00:00"))
            if latest_ts is None or lu_dt > latest_ts:
                latest_ts = lu_dt
        except Exception:
            pass

    if latest_ts is None:
        return False, None  # no timestamps available — assume fresh to avoid false blocks

    age = (now - latest_ts).total_seconds()
    return age > get_setting("odds_max_age_seconds", 120), round(age, 1)


def _extract_picks_from_odds(odds: dict, match: dict) -> list:
    """Extract value picks from pre-computed odds dict (mirrors frontend logic)."""
    picks = []
    if not odds or not odds.get("available"):
        return picks

    def valid_odds(o):
        od = o.get("bookieOdds", 0) or 0
        return get_setting("min_odds", 1.40) <= od <= get_setting("max_odds", 4.00)

    benter = odds.get("benter") or {}

    # 1X2
    bh = benter.get("h2h")
    if bh and bh.get("outcomes"):
        out_lbls = {
            "home": match.get("homeTeam", "Casa"),
            "draw": "Draw",
            "away": match.get("awayTeam", "Fora"),
        }
        h2x = [(k, o) for k, o in bh["outcomes"].items() if o.get("isValue") and valid_odds(o)]
        has_away = any(k == "away" for k, _ in h2x)
        has_draw = any(k == "draw" for k, _ in h2x)
        has_home = any(k == "home" for k, _ in h2x)
        if has_away and has_draw and not has_home:
            pass  # anti-double: skip both, let HCP handle
        else:
            for k, o in h2x:
                picks.append({
                    "market": "1X2", "label": out_lbls.get(k, k),
                    "odds": o.get("bookieOdds"), "edge": o.get("edge", 0),
                    "blend": o.get("blendedProb", 0), "model": o.get("modelProb", 0),
                })

    # Totals
    bt = benter.get("totals")
    if bt and bt.get("outcomes"):
        ou_lbl = {"over": f"Over {bt.get('line','')}", "under": f"Under {bt.get('line','')}"}
        for k, o in bt["outcomes"].items():
            if o.get("isValue") and valid_odds(o):
                picks.append({
                    "market": f"O/U {bt.get('line','')}",
                    "label": ou_lbl.get(k, k),
                    "odds": o.get("bookieOdds"), "edge": o.get("edge", 0),
                    "blend": o.get("blendedProb", 0), "model": o.get("modelProb", 0),
                })

    # Spreads
    bs = benter.get("spreads")
    if bs and bs.get("outcomes"):
        def _fmt_hcp_pt(pt):
            """Normalise HCP point: strip trailing .0 (e.g. -2.0 → -2, +0.25 → +0.25)."""
            if pt is None:
                return ""
            sign = "+" if pt >= 0 else ""
            return sign + (str(int(pt)) if pt == int(pt) else str(pt))
        hpt_h = _fmt_hcp_pt(bs.get("homePoint"))
        hpt_a = _fmt_hcp_pt(bs.get("awayPoint"))
        sp_lbl = {
            "home": f"{match.get('homeTeam','Casa')} {hpt_h}".strip(),
            "away": f"{match.get('awayTeam','Fora')} {hpt_a}".strip(),
        }
        for k, o in bs["outcomes"].items():
            if o.get("isValue") and valid_odds(o):
                picks.append({
                    "market": "Handicap", "label": sp_lbl.get(k, k),
                    "odds": o.get("bookieOdds"), "edge": o.get("edge", 0),
                    "blend": o.get("blendedProb", 0), "model": o.get("modelProb", 0),
                })

    return picks


def _run_background_cycle():
    """
    Single background cycle:
    1. Fetch Sofascore live list
    2. Filter to monitored leagues
    3. Prime odds cache (1 fetch per sport key = 3 req)
    4. Compute full analysis per game
    5. Sync tips to DB
    6. Update _live_state
    """
    global _last_cycle_ts, _last_cycle_req

    t0 = time.time()
    req_before = _api_requests_remaining or 0

    try:
        live = get_live()
    except Exception as e:
        log.error(f"BG: get_live() failed: {e}")
        return

    # Filter to monitored leagues only — sport key check + strict name check
    monitored = []
    for m in live:
        tourn   = m.get("tournament", "")
        country = m.get("country", "")
        sk = _resolve_sport_key(tourn, country)
        if sk in MONITORED_SPORT_KEYS and _is_monitored_league_strict(tourn, country):
            m["_sport_key"] = sk
            monitored.append(m)

    log.info(f"BG cycle: {len(live)} live total, {len(monitored)} in monitored leagues")

    if not monitored:
        with _state_lock:
            _live_state.clear()
        # CRITICAL: even with 0 monitored live games, we MUST still:
        #   - finalize/resolve tips for games that finished off-camera
        #   - refresh the upcoming cache (powers league/team pages + footer)
        #   - refresh the footer cache itself
        # Otherwise, every period with no monitored game live (common during
        # weekday daytimes, post-deploy, etc.) leaves all our cached UI
        # data empty until the next monitored kickoff — sometimes hours later.
        try:
            _finalize_dropped_games(set())
            _resolve_finished_tips()
        except Exception as e:
            log.warning(f"BG: post-empty finalize/resolve failed: {e}")
        try:
            _refresh_upcoming_cache(days_ahead=3)
        except Exception as e:
            log.warning(f"BG: post-empty _refresh_upcoming_cache failed: {e}")
        try:
            _refresh_slug_index()
        except Exception as e:
            log.warning(f"BG: post-empty _refresh_slug_index failed: {e}")
        try:
            _refresh_footer_cache()
        except Exception as e:
            log.warning(f"BG: post-empty _refresh_footer_cache failed: {e}")
        _last_cycle_ts = time.time()
        return

    # Group by sport key → 1 odds fetch per sport key
    sport_keys = {m["_sport_key"] for m in monitored}
    for sk in sport_keys:
        try:
            get_odds_for_sport(sk)  # populates cache, 3 req
        except Exception as e:
            log.error(f"BG: odds fetch failed for {sk}: {e}")

    # Compute full analysis per game
    new_state = {}
    for m in monitored:
        mid = m["id"]
        try:
            shots     = get_shotmap(mid)
            incidents = get_incidents(mid)
            odds      = get_full_odds_analysis(m, shots)

            # Persist shot timeline for xG replay animation
            _persist_shots(mid, shots)

            # Upsert game in DB
            _upsert_game(m)

            # Extract picks + sync to DB
            minute = m.get("minute")  # None if not available
            # Suppress new picks when there's a red card — superioridade numérica
            # invalida o modelo (regra também mostrada na UI).
            red_cards = incidents.get("redCards", 0) if incidents else 0
            odds_stale, odds_age = _odds_are_stale(odds)
            if red_cards > 0:
                picks = []
                log.info(f"BG: Skipping picks for match {mid} — {red_cards} red card(s) invalidate model")
            elif odds_stale:
                picks = []
                log.warning(
                    f"BG: Skipping picks for match {mid} — odds are {odds_age:.0f}s old "
                    f"(>{ODDS_MAX_AGE_SECONDS}s threshold)"
                )
            else:
                picks = _extract_picks_from_odds(odds, m) if odds else []
            last_goal_minute = incidents.get("lastGoalMinute") if incidents else None
            tips   = _sync_tips_db(mid, picks, minute, odds or {}, last_goal_minute, match=m, shots=shots)
            _auto_resolve_db(mid, m, incidents)

            # Re-read tips after resolution
            with _db() as conn:
                tips = [dict(t) for t in conn.execute(
                    "SELECT * FROM tips WHERE match_id = ? ORDER BY wall_ts", (mid,)
                ).fetchall()]

            # ── livePicks: ONLY picks that have value AT THIS MOMENT ──
            # These are computed from the current live odds + current model probabilities.
            # If a tip's value disappeared (odds moved against us), it won't appear here,
            # even if it's still stored in `tips` (for historical track record).
            # Apply the same cross-market dedup as _sync_tips_db so the UI never
            # shows two equivalent picks (e.g. 1X2 Team + HCP Team -0.5).
            live_picks = []
            for p in _filter_redundant_live_picks(picks):
                live_picks.append({
                    "market":     p.get("market"),
                    "label":      p.get("label"),
                    "odds":       p.get("odds"),       # current live bookmaker odds
                    "edge":       p.get("edge", 0),    # current edge (positive by definition)
                    "blend":      p.get("blend", 0),
                    "model":      p.get("model", 0),
                    "minute":     minute,
                })

            # Inject logos inline — zero extra requests from the frontend
            m["home_logo"] = _quick_logo(m.get("home_team", ""))
            m["away_logo"] = _quick_logo(m.get("away_team", ""))

            new_state[mid] = {
                "match":     m,
                "shots":     shots,
                "incidents": incidents,
                "odds":      odds,
                "tips":      tips,        # all historical tips (for track record)
                "livePicks": live_picks,  # only currently-valid picks (for "ANÁLISE · VALUE")
                "ts":        datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            log.error(f"BG: failed to process game {mid}: {e}")

    with _state_lock:
        _live_state.clear()
        _live_state.update(new_state)

    # Finalize games that dropped off the live feed, then resolve their tips
    live_ids = {m["id"] for m in monitored}
    _finalize_dropped_games(live_ids)
    _resolve_finished_tips()

    # Keep upcoming cache warm — runs in same bg thread (curl_cffi safe here)
    try:
        _refresh_upcoming_cache(days_ahead=3)
    except Exception as e:
        log.warning(f"BG: _refresh_upcoming_cache failed: {e}")

    # Refresh dynamic footer payload for /api/footer/dynamic (cheap, all in-memory + 1 DB query)
    try:
        _refresh_footer_cache()
    except Exception as e:
        log.warning(f"BG: _refresh_footer_cache failed: {e}")

    # Keep SEO slug index fresh (cheap, one DB query)
    try:
        _refresh_slug_index()
    except Exception as e:
        log.warning(f"BG: _refresh_slug_index failed: {e}")

    # Pre-warm league logo CDN bytes + tournament IDs so first user
    # request to /api/seo/league/{slug} is sub-100ms.
    try:
        _warmup_seo_caches()
    except Exception as e:
        log.warning(f"BG: _warmup_seo_caches failed: {e}")

    req_after = _api_requests_remaining or 0
    _last_cycle_ts = time.time()
    _last_cycle_req = req_before - req_after
    log.info(
        f"BG cycle done in {time.time()-t0:.1f}s — "
        f"{len(new_state)} games processed, {_last_cycle_req} API req used"
    )


def _finalize_dropped_games(live_ids: set):
    """
    For any DB game with is_finished=0 that is NOT in the current live feed,
    fetch its current state from Sofascore directly. If it's finished, mark it
    and update the score so _resolve_finished_tips() can settle its tips.
    """
    # Only consider games that should plausibly be over: kickoff at least
    # 90 min ago. Avoids hammering Sofascore with one request per scheduled
    # game just to be told "still notstarted" — important when this runs
    # with an empty live_ids set (no monitored games live).
    cutoff_ts = int(time.time()) - 90 * 60
    with _db() as conn:
        pending = conn.execute(
            "SELECT id FROM games WHERE is_finished = 0 AND start_ts <= ?",
            (cutoff_ts,)
        ).fetchall()

    dropped = [r["id"] for r in pending if r["id"] not in live_ids]
    if not dropped:
        return

    log.info(f"_finalize_dropped_games: checking {len(dropped)} dropped game(s): {dropped}")
    for gid in dropped:
        try:
            ev = get_event(gid)
            if ev is None:
                continue
            if ev.get("isFinished"):
                hg = ev.get("homeGoals", 0)
                ag = ev.get("awayGoals", 0)
                now_ts = int(time.time())
                with _db() as conn:
                    conn.execute(
                        "UPDATE games SET is_finished=1, home_goals=?, away_goals=?, "
                        "archived_at=COALESCE(archived_at,?) WHERE id=?",
                        (hg, ag, now_ts, gid)
                    )
                log.info(f"Finalized dropped game {gid} ({ev['homeTeam']} {hg}-{ag} {ev['awayTeam']})")
            # Also handle still-live games: update score silently
            elif ev.get("isLive"):
                hg = ev.get("homeGoals", 0)
                ag = ev.get("awayGoals", 0)
                with _db() as conn:
                    conn.execute(
                        "UPDATE games SET home_goals=?, away_goals=? WHERE id=?",
                        (hg, ag, gid)
                    )
        except Exception as e:
            log.warning(f"_finalize_dropped_games: failed for game {gid}: {e}")


def _resolve_finished_tips():
    """
    After each cycle, resolve any unresolved tips for games already marked
    finished in the DB. These games have left the live feed so _auto_resolve_db
    never ran for their final state.
    """
    import re as _re
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT g.id, g.home_team, g.away_team, g.home_goals, g.away_goals,
                       t.tip_key, t.market, t.label
                FROM games g
                JOIN tips t ON t.match_id = g.id
                WHERE g.is_finished = 1 AND t.result IS NULL
            """).fetchall()

            for r in rows:
                hg, ag = r["home_goals"], r["away_goals"]
                total   = hg + ag
                lbl, mkt = r["label"], r["market"]
                new_result = None

                om = _re.match(r'^Over\s+([\d.]+)$',  lbl, _re.IGNORECASE)
                um = _re.match(r'^Under\s+([\d.]+)$', lbl, _re.IGNORECASE)
                if om:
                    line = float(om.group(1))
                    new_result = "green" if total > line else "red"
                elif um:
                    line = float(um.group(1))
                    new_result = "red" if total > line else "green"
                elif mkt == "1X2":
                    ft = "home" if hg > ag else ("draw" if hg == ag else "away")
                    out_map = {"home": r["home_team"], "draw": "Draw", "away": r["away_team"]}
                    for side, name in out_map.items():
                        if lbl.lower() in name.lower() or (len(lbl) > 3 and name.lower().startswith(lbl[:4].lower())):
                            new_result = "green" if side == ft else "red"
                            break
                elif mkt == "Handicap":
                    # HCP: "Team +X.X" or "Team -X.X"
                    hm = _re.search(r'([+-][\d.]+)$', lbl)
                    if hm:
                        hcp = float(hm.group(1))
                        # Determine if home or away team
                        team_part = lbl[:lbl.rfind(hm.group(0))].strip()
                        is_home = team_part.lower() in r["home_team"].lower() or \
                                  (len(team_part) > 3 and r["home_team"].lower().startswith(team_part[:4].lower()))
                        margin = (hg - ag) if is_home else (ag - hg)
                        adj = margin + hcp
                        if adj > 0:    new_result = "green"
                        elif adj < 0:  new_result = "red"
                        else:          new_result = "void"

                if new_result:
                    conn.execute(
                        "UPDATE tips SET result = ? WHERE tip_key = ? AND match_id = ?",
                        (new_result, r["tip_key"], r["id"])
                    )
                    log.info(f"Resolved tip {r['tip_key']} ({lbl}) → {new_result}")
    except Exception as e:
        log.error(f"_resolve_finished_tips failed: {e}")


def _background_loop():
    """Runs forever, sleeping BG_INTERVAL seconds between cycles."""
    # Stagger first cycle by 5s to let Gunicorn/Flask finish starting
    time.sleep(5)
    while True:
        try:
            _run_background_cycle()
        except Exception as e:
            log.error(f"BG loop unhandled error: {e}")
        time.sleep(BG_INTERVAL)


# ── New API endpoints ──

# Keywords that appear in legitimate monitored league names only
# Used for strict matching in the "today" filter
_MONITORED_LEAGUE_STRICT_KEYWORDS = {
    # England
    "premier league": {"england", "english"},  # "uk" removed — substring matches "ukraine"
    "championship": {"england", "english"},
    "efl": {"england", "english"},
    # Spain — restrict to Spain so Chilean "Copa de la Liga" doesn't slip in
    "la liga": {"spain", "spanish"},
    "laliga": {"spain", "spanish"},
    "primera division": {"spain", "spanish"},
    # Italy
    "serie a": {"italy", "italian"},
    "serie b": None,
    # Germany
    "bundesliga": {"germany", "german"},
    "2. bundesliga": None,
    "bundesliga 2": None,
    # France
    "ligue 1": {"france", "french"},
    "ligue 2": None,
    # Portugal
    "liga portugal": None,
    "primeira liga": {"portugal", "portuguese"},
    # Netherlands
    "eredivisie": {"netherlands", "dutch", "holland"},
    # Belgium
    "jupiler": None,
    "pro league": {"belgium"},
    "first division a": {"belgium"},
    # Greece
    "stoiximan super league": None,
    "greek super league": None,
    "super league greece": None,
    # Turkey
    "süper lig": None,
    "super lig": {"turkey"},
    # Austria
    "austrian bundesliga": None,
    "admiral bundesliga": None,
    "austrian bundesliga 2": None,
    "admiral bundesliga 2": None,
    "second league": {"austria"},
    # Scotland
    "scottish premiership": None,
    "scottish premier league": None,
    # Scandinavia
    "allsvenskan": None,
    "eliteserien": None,
    "superligaen": None,
    "veikkausliiga": None,
    # Other Europe
    "ekstraklasa": None,
    "swiss super league": None,
    "league of ireland": None,
    "airtricity league": None,
    # Russia / Saudi
    "russian premier league": None,
    "saudi pro league": None,
    "roshn saudi league": None,
    "saudi professional league": None,
    # UEFA
    "champions league": None,
    "europa league": None,
    "conference league": None,
    "nations league": None,
    "european championship": None,
    "uefa euro": None,
    "euro qualification": None,
    "euro qualifying": None,
    "women's champions league": None,
    # FIFA
    "fifa world cup": None,
    "world cup": None,
    "women's world cup": None,
    "club world cup": None,
    # Americas
    "mls": None,
    "major league soccer": None,
    "brasileirão": None,
    "campeonato brasileiro": None,
    "primera división": {"chile"},
    "copa libertadores": None,
    "conmebol libertadores": None,
    "copa sudamericana": None,
    "conmebol sudamericana": None,
    "copa america": None,
    "liga mx": None,
    "liga bbva": None,
    "campeonato nacional": {"chile"},
    "brasileirão série b": None,
}

_YOUTH_KEYWORDS = {"u23","u21","u20","u19","u18","u17","u15","youth","reserve","b team"}

# Countries whose competitions are never monitored (block by country field)
_BLOCKED_COUNTRIES = {
    "china", "chinese",
    "india",
    "south korea", "korea",
    "indonesia",
    "vietnam",
    "thailand",
    "malaysia",
    "iran",
    "uzbekistan",
}

# Tournament fragments that always mean NOT a monitored competition
_BLOCKED_TOURNAMENT_FRAGMENTS = {
    "série d", "serie d", "série c", "serie c",   # Brazil lower divisions
    "série a2", "série a3", "seria a2", "serie a3",
    "paulista", "carioca", "gaúcho", "mineiro", "baiano",  # Brazil state leagues
    "ligapro",                                    # Ecuador
    "usl", "nisa", "next pro",                    # US non-MLS, lower divisions
    "frauen", "women", "femminile",               # Women's competitions (German + English + Italian)
    "liga portugal 2",                            # Portuguese 2nd division
    "laliga 2", "la liga 2",                      # Spanish 2nd division (no live xG from Sofascore)
    "j1 league",                                  # Japanese J1 (no live xG from Sofascore)
    "damallsvenskan",                             # Swedish women's league (no live xG from Sofascore)
    "northern premier",                           # English amateur pyramid (all divisions)
    "amateur",                                    # Any amateur competition
    "copa de la liga",                            # Chilean cup (not Spanish LaLiga)
}

def _get_blocked_fragments() -> set:
    """Return merged set of hardcoded + admin-defined blocked keywords."""
    extra_raw = get_setting("blocked_keywords", "")
    if extra_raw and isinstance(extra_raw, str):
        extra = {kw.strip().lower() for kw in extra_raw.split(",") if kw.strip()}
    else:
        extra = set()
    return _BLOCKED_TOURNAMENT_FRAGMENTS | extra


def _is_monitored_league_strict(tournament, country):
    """Strict check: only pass leagues explicitly in our monitored list."""
    import re as _re
    # Use raw lowercase for fragment/youth checks (before normalization strips suffixes)
    raw = tournament.lower()
    t = _normalize_tournament(tournament).lower()
    c = (country or "").lower()
    # Exclude blocked countries — any competition from these countries is rejected
    for bc in _BLOCKED_COUNTRIES:
        if _re.search(r'\b' + _re.escape(bc) + r'\b', c):
            return False
    # Exclude youth/reserve competitions
    for yk in _YOUTH_KEYWORDS:
        if yk in raw:
            return False
    # Exclude blocked fragments: hardcoded + dynamic from admin panel
    for frag in _get_blocked_fragments():
        if frag in raw:
            return False
    # Check against strict keyword map
    # Use word-boundary matching for country to prevent "uk" matching "ukraine" etc.
    def _country_match(ac: str, country_str: str, tourn_str: str) -> bool:
        import re as _re2
        pat = _re2.compile(r'\b' + _re2.escape(ac) + r'\b')
        return bool(pat.search(country_str) or pat.search(tourn_str))

    for kw, allowed_countries in sorted(_MONITORED_LEAGUE_STRICT_KEYWORDS.items(), key=lambda x: -len(x[0])):
        if kw in t:
            if allowed_countries is None:
                return True
            for ac in allowed_countries:
                if _country_match(ac, c, t):
                    return True
            return False  # keyword found but country doesn't match
    return False


@app.route("/api/today/monitored")
def r_today_monitored():
    """Scheduled games for a given date (default: today) for monitored leagues only."""
    try:
        # Parse optional date parameter (YYYY-MM-DD)
        date_str = flask_request.args.get("date")
        if not date_str:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Fetch scheduled games for the given date
        url = f"{SOFASCORE_API}/sport/football/scheduled-events/{date_str}"
        data = _get(url)
        all_events = data.get("events", []) if data else []

        # Build date range for the requested date (UTC)
        from datetime import date as _date
        req_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_start = int(req_date.timestamp())
        day_end   = day_start + 86400

        result = []
        for m in all_events:
            if m.get("isFinished") or m.get("isLive"):
                continue
            # Only include games that actually start on the requested date
            ts = m.get("startTimestamp", 0)
            if ts and not (day_start <= ts < day_end):
                continue
            # Extract tournament name (could be dict or string)
            tourn = m.get("tournament", {})
            tourn_name = tourn.get("name", "") if isinstance(tourn, dict) else str(tourn or "")
            country = m.get("country", {})
            country_name = country.get("name", "") if isinstance(country, dict) else str(country or "")

            if _is_monitored_league_strict(tourn_name, country_name):
                sk = _resolve_sport_key(tourn_name, country_name)
                m["_sport_key"] = sk
                # Extract team names from team objects if needed
                if isinstance(m.get("homeTeam"), dict):
                    m["homeTeam"] = m["homeTeam"].get("name", "")
                if isinstance(m.get("awayTeam"), dict):
                    m["awayTeam"] = m["awayTeam"].get("name", "")
                result.append(m)
        result.sort(key=lambda m: m.get("startTimestamp") or 0)
        return jsonify({"count": len(result), "matches": result, "date": date_str})
    except Exception as e:
        log.error(f"r_today_monitored error: {e}")
        return jsonify({"error": str(e)}), 500


# ── Upcoming matches cache — keyed by date string, refreshed in background ──
# Dict operations in CPython are GIL-protected; no lock needed for basic get/set.
_upcoming_cache: dict = {}   # date_str → {"matches": [...], "ts": float}
_UPCOMING_TTL = 900          # 15 minutes; background loop refreshes every cycle


# ── Footer "dynamic" cache ──────────────────────────────────────────────────
# Powers the /api/footer/dynamic endpoint that the Lovable footer calls.
# Refreshed by the background loop alongside _upcoming_cache so the request
# path never blocks on DB or live data computation.
_footer_cache: dict = {"data": None, "ts": 0.0}
_FOOTER_TTL = 300            # 5 minutes


def _compute_footer_data() -> dict:
    """
    Build the 3 columns for the dynamic footer:

      live_now      — currently-live matches, ordered by league priority
                      (Premier League before Brazilian 2nd division), max 5
      next_kickoff  — next monitored matches starting within the next 24h,
                      ordered by kickoff time ascending, max 5
      week_leagues  — leagues by # of fixtures in the next 7 days, max 6

    Wording rules (per user feedback 2026-05-11):
      * Always UTC timestamps — never local timezones
      * "picks" count only for currently-LIVE matches; not for upcoming
      * Never show win-rate; ROI/profit only, only if positive
      * Markets column dropped — didn't add value to the footer experience
    """
    out = {
        "live_now":     [],
        "next_kickoff": [],
        "week_leagues": [],
    }

    # ── Build league priority map from the competitions table
    #    (lower priority number = more important league)
    priority_by_sk: dict = {}
    try:
        with _db() as conn:
            for r in conn.execute("SELECT sport_key, priority FROM competitions"):
                priority_by_sk[r["sport_key"]] = r["priority"] or 99
    except Exception as e:
        log.warning(f"footer: priority map fetch failed: {e}")

    def _priority_for(tournament: str, country: str) -> int:
        try:
            sk = _resolve_sport_key(tournament, country)
            return priority_by_sk.get(sk, 99)
        except Exception:
            return 99

    # ── 1. LIVE NOW — sorted by league priority, then by kick-off recency
    try:
        with _state_lock:
            live_snapshot = list(_live_state.values())
        live_items = []
        for entry in live_snapshot:
            m = entry.get("match") or {}
            if m.get("isFinished") or m.get("homeGoals") is None:
                continue
            home   = m.get("homeTeam", "")
            away   = m.get("awayTeam", "")
            hg     = m.get("homeGoals", 0) or 0
            ag     = m.get("awayGoals", 0) or 0
            mid    = m.get("id")
            minute = m.get("minute")
            tourn  = m.get("tournament", "")
            country = m.get("country", "")
            picks_now = len(entry.get("livePicks") or entry.get("tips") or [])
            subtitle_bits = []
            if minute is not None:
                subtitle_bits.append(f"{minute}'")
            if tourn:
                subtitle_bits.append(tourn)
            if picks_now > 0:
                subtitle_bits.append(f"{picks_now} pick" + ("s" if picks_now != 1 else ""))
            live_items.append({
                "title":     f"{home} {hg}-{ag} {away}",
                "subtitle":  " · ".join(subtitle_bits),
                "url":       _match_url(mid, home, away),
                "_priority": _priority_for(tourn, country),
                "_minute":   minute or 0,
            })
        # Lower priority number = better. Tie-break: more recent (higher minute) first.
        live_items.sort(key=lambda x: (x["_priority"], -x["_minute"]))
        for it in live_items[:5]:
            it.pop("_priority", None)
            it.pop("_minute", None)
        out["live_now"] = live_items[:5]
    except Exception as e:
        log.warning(f"footer.live_now build failed: {e}")

    # ── 2. NEXT KICKOFF — upcoming monitored matches in the next 24h
    try:
        now_ts    = int(time.time())
        cutoff_ts = now_ts + 24 * 3600
        candidates = []
        now_utc = datetime.now(timezone.utc)
        for offset in range(2):  # today + tomorrow covers any 24h window
            date_str = (now_utc + timedelta(days=offset)).strftime("%Y-%m-%d")
            cached = _upcoming_cache.get(date_str)
            if not cached:
                continue
            for m in cached.get("matches", []):
                ts = m.get("startTimestamp", 0) or 0
                if ts <= now_ts or ts > cutoff_ts:
                    continue
                sk = m.get("_sport_key")
                # Only show matches in our monitored leagues — avoids
                # Norwegian 3rd division etc cluttering the footer.
                if sk not in MONITORED_SPORT_KEYS:
                    continue
                home  = m.get("homeTeam", "")
                away  = m.get("awayTeam", "")
                tourn = m.get("tournament", "")
                kick_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M UTC")
                candidates.append({
                    "title":    f"{home} vs {away}",
                    "subtitle": f"{kick_str}" + (f" · {tourn}" if tourn else ""),
                    "url":      _match_url(m.get("id"), home, away),
                    "_ts":      ts,
                    "_priority": priority_by_sk.get(sk, 99),
                })
        # Soonest first; tie-break by league priority
        candidates.sort(key=lambda x: (x["_ts"], x["_priority"]))
        for it in candidates[:5]:
            it.pop("_ts", None)
            it.pop("_priority", None)
        out["next_kickoff"] = candidates[:5]
    except Exception as e:
        log.warning(f"footer.next_kickoff build failed: {e}")

    # ── 3. WEEK LEAGUES — aggregate next 7 days from _upcoming_cache
    try:
        now_utc = datetime.now(timezone.utc)
        league_counts = {}
        for offset in range(7):
            date_str = (now_utc + timedelta(days=offset)).strftime("%Y-%m-%d")
            cached = _upcoming_cache.get(date_str)
            if not cached:
                continue
            for m in cached.get("matches", []):
                tourn = m.get("tournament", "").strip()
                if not tourn:
                    continue
                league_counts[tourn] = league_counts.get(tourn, 0) + 1
        ranked = sorted(league_counts.items(), key=lambda x: -x[1])[:6]
        for tourn, count in ranked:
            out["week_leagues"].append({
                "title":    tourn,
                "subtitle": f"{count} match" + ("es" if count != 1 else "") + " this week",
                "url":      f"{SITE_URL}/league/{_slug(tourn)}",
            })
    except Exception as e:
        log.warning(f"footer.week_leagues build failed: {e}")

    return out


def _refresh_footer_cache():
    """Recompute the footer payload and store in cache. Called by BG loop."""
    try:
        _footer_cache["data"] = _compute_footer_data()
        _footer_cache["ts"]   = time.time()
    except Exception as e:
        log.warning(f"_refresh_footer_cache failed: {e}")


@app.route("/api/footer/dynamic")
def r_footer_dynamic():
    """
    Returns a 4-column payload for the site footer. Pure read of the
    in-memory `_footer_cache`, populated by the background loop.

    Frontend (Lovable) calls this once per page load and renders 3 columns:
      live_now      — Live matches now, ordered by league priority (max 5)
      next_kickoff  — Next monitored matches starting in <24h (max 5)
      week_leagues  — Top leagues by # of fixtures in next 7 days (max 6)
    """
    data = _footer_cache.get("data")
    if data is None:
        # Cold cache — best-effort synchronous build (~50-200ms)
        try:
            data = _compute_footer_data()
            _footer_cache["data"] = data
            _footer_cache["ts"]   = time.time()
        except Exception as e:
            log.warning(f"r_footer_dynamic cold build failed: {e}")
            data = {"live_now": [], "next_kickoff": [], "week_leagues": []}

    return jsonify({
        **data,
        "cached_at": int(_footer_cache.get("ts") or 0),
    })


def _fetch_day_matches(date_str: str) -> list:
    """
    Fetch & filter scheduled matches for one date from Sofascore.
    Returns list of match dicts ready for the API response.
    Must only be called from a background thread (curl_cffi blocks gevent).
    """
    try:
        day_dt    = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_start = int(day_dt.timestamp())
        day_end   = day_start + 86400

        data       = _get(f"{SOFASCORE_API}/sport/football/scheduled-events/{date_str}")
        all_events = data.get("events", []) if data else []
    except Exception as e:
        log.warning(f"_fetch_day_matches: Sofascore fetch failed for {date_str}: {e}")
        return []

    day_matches = []
    for m in all_events:
        if m.get("isFinished") or m.get("isLive"):
            continue
        ts = m.get("startTimestamp", 0)
        if ts and not (day_start <= ts < day_end):
            continue

        tourn        = m.get("tournament", {})
        tourn_name   = tourn.get("name", "") if isinstance(tourn, dict) else str(tourn or "")
        country      = m.get("country", {})
        country_name = country.get("name", "") if isinstance(country, dict) else str(country or "")

        if not _is_monitored_league_strict(tourn_name, country_name):
            continue

        home = m.get("homeTeam", "")
        away = m.get("awayTeam", "")
        if isinstance(home, dict):
            home = home.get("name", "")
        if isinstance(away, dict):
            away = away.get("name", "")

        unique_tournament_id = ((tourn.get("uniqueTournament") or {}).get("id")
                                if isinstance(tourn, dict) else None)
        day_matches.append({
            "id":             m["id"],
            "homeTeam":       home,
            "awayTeam":       away,
            "home_logo":      _quick_logo(home),
            "away_logo":      _quick_logo(away),
            "tournament":     tourn_name,
            "tournamentId":   unique_tournament_id,
            "country":        country_name,
            "startTimestamp": ts,
            "_sport_key":     _resolve_sport_key(tourn_name, country_name),
        })

    day_matches.sort(key=lambda x: x.get("startTimestamp", 0))
    return day_matches


def _refresh_upcoming_cache(days_ahead: int = 3):
    """
    Called from the background loop thread — safe to block with curl_cffi.
    Refreshes the upcoming cache for today + next N days.
    """
    now_utc = datetime.now(timezone.utc)
    for offset in range(days_ahead):
        date_str = (now_utc + timedelta(days=offset)).strftime("%Y-%m-%d")
        cached   = _upcoming_cache.get(date_str)
        # Skip if fresh enough
        if cached and (time.time() - cached["ts"]) < _UPCOMING_TTL:
            continue
        matches = _fetch_day_matches(date_str)
        _upcoming_cache[date_str] = {"matches": matches, "ts": time.time()}
        log.info(f"_refresh_upcoming_cache: {date_str} → {len(matches)} matches cached")


# ── SEO slug index + page cache ───────────────────────────────────────────────
# Maps slugified team / tournament names → canonical name as stored in the DB.
# Refreshed by the background loop (cheap: one query, scans ~285 games).
# Plus a generic page-level cache for rendered HTML, so SEO pages never block.
_slug_index_cache: dict = {"teams": {}, "leagues": {}, "ts": 0.0}
_SLUG_INDEX_TTL    = 3600    # 1 hour
_seo_cache: dict   = {}      # cache_key → {"html": str, "ts": float}
_SEO_CACHE_TTL     = 900     # 15 minutes


def _normalize_tournament_pretty(name: str) -> str:
    """
    Like _normalize_tournament but preserves original case.
    'CONMEBOL Sudamericana, Group C'  → 'CONMEBOL Sudamericana'
    'Scottish Premiership, Championship Round' → 'Scottish Premiership'
    'Liga MX, Clausura Playoffs'      → 'Liga MX'
    """
    import re
    cleaned = re.sub(
        r'\s*,\s+(group|grp|round|phase|stage|pool|matchday|md|jornada|giornata|journée|'
        r'spieltag|playoff|play-off|play off|qualification|qualifying|relegation|promotion|'
        r'conference|champions|europa|cup|shield|super|final|semi|quarter)\b.*$',
        '', name, flags=re.IGNORECASE
    ).strip()
    cleaned = re.sub(r'\s*,.*$', '', cleaned).strip()
    cleaned = re.sub(r'\s*\(.*\)\s*$', '', cleaned).strip()
    return cleaned


def _refresh_slug_index():
    """
    Build slug → canonical-name maps for teams and leagues that have ≥1 pick.
    For leagues, group fragments ('CONMEBOL Sudamericana, Group C', 'Group H',
    etc.) are collapsed into a single canonical entry so the league page
    aggregates all phases/rounds into one URL.
    """
    if (time.time() - _slug_index_cache["ts"]) < _SLUG_INDEX_TTL:
        return
    try:
        teams: dict[str, str] = {}
        leagues: dict[str, str] = {}                    # slug → canonical pretty name
        league_variants: dict[str, list[str]] = {}      # canonical → list of DB tournament strings
        with _db() as conn:
            # Teams that have appeared in a game with at least one tip
            for row in conn.execute("""
                SELECT DISTINCT g.home_team AS name FROM games g
                JOIN tips t ON t.match_id = g.id
                UNION
                SELECT DISTINCT g.away_team AS name FROM games g
                JOIN tips t ON t.match_id = g.id
            """).fetchall():
                name = (row["name"] or "").strip()
                if name:
                    teams[_slug(name)] = name

            # Leagues — collapse "Foo, Group A" / "Foo, Group B" / "Foo, Knockout stage"
            # under a single canonical "Foo".
            for row in conn.execute("""
                SELECT DISTINCT g.tournament AS name FROM games g
                JOIN tips t ON t.match_id = g.id
                WHERE g.tournament IS NOT NULL AND g.tournament <> ''
            """).fetchall():
                raw = (row["name"] or "").strip()
                if not raw:
                    continue
                canonical = _normalize_tournament_pretty(raw) or raw
                slug      = _slug(canonical)
                leagues.setdefault(slug, canonical)
                league_variants.setdefault(canonical, []).append(raw)

        # Variant slug → canonical name. Lets /league/uefa-europa-league-knockout-stage
        # (whatever raw slug Lovable might build from a match's tournament field)
        # still resolve to the unified canonical league page.
        variant_slug_map: dict[str, str] = {}
        for canonical, variants in league_variants.items():
            for raw in variants:
                variant_slug_map[_slug(raw)] = canonical

        _slug_index_cache["teams"]                = teams
        _slug_index_cache["leagues"]              = leagues
        _slug_index_cache["league_variants"]      = league_variants
        _slug_index_cache["league_variant_slugs"] = variant_slug_map
        _slug_index_cache["ts"]                   = time.time()
        log.info(f"_refresh_slug_index: {len(teams)} teams, "
                 f"{len(leagues)} leagues "
                 f"({sum(len(v) for v in league_variants.values())} variant rows, "
                 f"{len(variant_slug_map)} variant slugs)")
    except Exception as e:
        log.warning(f"_refresh_slug_index failed: {e}")


def _resolve_league_tid_quick(name: str) -> int | None:
    """
    Request-path-safe deep resolution: scans only the next 3 days via
    Sofascore (≤3 HTTP calls, each cached upstream). Used by the logo
    proxy on cold-cache misses so the very first user gets the logo.
    Updates _league_tid_memo on success.
    """
    if name in _league_tid_memo and _league_tid_memo[name]:
        return _league_tid_memo[name]
    target = _normalize_tournament(name)
    today = datetime.now(timezone.utc)
    for offset in range(3):
        date_str = (today + timedelta(days=offset)).strftime("%Y-%m-%d")
        try:
            for m in get_scheduled(date_str):
                if (_normalize_tournament(m.get("tournament") or "") == target
                        and m.get("tournamentId")):
                    tid = m["tournamentId"]
                    _league_tid_memo[name] = tid
                    return tid
        except Exception:
            continue
    return None


def _resolve_league_tid_deep(name: str) -> int | None:
    """
    Background-only deep resolution: scans the next 14 days via Sofascore
    `get_scheduled()` (curl_cffi-safe in BG thread) when the in-memory
    upcoming cache doesn't contain a match for the league.
    Used by the warmup so leagues that don't play every day (UEFA knockout
    rounds, CONMEBOL between matchdays) still get their tid populated.
    """
    if name in _league_tid_memo and _league_tid_memo[name]:
        return _league_tid_memo[name]
    target = _normalize_tournament(name)
    today = datetime.now(timezone.utc)
    for offset in range(14):
        date_str = (today + timedelta(days=offset)).strftime("%Y-%m-%d")
        try:
            for m in get_scheduled(date_str):
                if (_normalize_tournament(m.get("tournament") or "") == target
                        and m.get("tournamentId")):
                    tid = m["tournamentId"]
                    _league_tid_memo[name] = tid
                    return tid
        except Exception:
            continue
    return None


def _warmup_seo_caches():
    """
    Proactive warmup of SEO caches. Runs in the background loop after
    slug index + upcoming cache are fresh, so the very first user request
    to any league/team page gets a sub-100ms response.

    Resolves tournament IDs (using a 14-day Sofascore scan as fallback so
    sporadic competitions like UEFA knockouts also resolve) and pre-fetches
    each league's logo bytes via curl_cffi.
    """
    leagues = _slug_index_cache.get("leagues") or {}
    if not leagues:
        return

    for slug, name in leagues.items():
        # Deep resolution allowed here (we're in the BG thread)
        tid = _resolve_league_tid_deep(name)
        if not tid:
            continue
        if slug in _league_logo_bytes:
            continue
        try:
            fetched = _get_bytes(
                f"https://api.sofascore.com/api/v1/unique-tournament/{tid}/image",
                timeout=6,
            )
            if fetched:
                _league_logo_bytes[slug] = fetched
        except Exception as e:
            log.debug(f"warmup logo {slug}: {e}")

    # The JSON cache may have been written before tids were resolved.
    # Drop entries with stale null logo_url so they re-build with the
    # logos populated.
    stale_keys = []
    for k, v in list(_seo_cache.items()):
        if k.startswith("jsonleague:") and v.get("html") and '"logo_url": null' in v["html"]:
            stale_keys.append(k)
    for k in stale_keys:
        _seo_cache.pop(k, None)

    log.info(f"_warmup_seo_caches: tids={len(_league_tid_memo)} "
             f"logo_bytes={len(_league_logo_bytes)} "
             f"json_cache_invalidated={len(stale_keys)}")


def _league_variants_for(canonical: str) -> list[str]:
    """All DB tournament strings that collapse into this canonical league name."""
    if not _slug_index_cache.get("league_variants"):
        _refresh_slug_index()
    return _slug_index_cache.get("league_variants", {}).get(canonical, [canonical])


def _team_by_slug(slug: str) -> str | None:
    return _slug_index_cache["teams"].get(slug)


def _league_by_slug(slug: str) -> str | None:
    """
    Slug → canonical league name. Falls back to variant slugs so that
    naive slugs like 'uefa-europa-league-knockout-stage' resolve to the
    canonical 'UEFA Europa League'.
    """
    direct = _slug_index_cache.get("leagues", {}).get(slug)
    if direct:
        return direct
    return _slug_index_cache.get("league_variant_slugs", {}).get(slug)


_league_tid_memo: dict = {}              # canonical name → uniqueTournament id
_league_logo_bytes: dict = {}            # slug → (bytes, content_type)

# Hardcoded Sofascore uniqueTournament IDs as a final safety-net for major
# competitions that may have no games in the next 14 days (between rounds
# of UEFA knockout phases, off-season, etc.). Keyed by lowercase canonical
# name (after _normalize_tournament).
_LEAGUE_TID_FALLBACK = {
    "premier league":         17,
    "championship":           18,
    "laliga":                  8,
    "la liga":                 8,
    "bundesliga":             35,
    "2. bundesliga":          44,
    "serie a":                23,
    "ligue 1":                34,
    "liga portugal":         238,
    "liga portugal betclic": 238,
    "primeira liga":         238,
    "eredivisie":             37,
    "vriendenloterij eredivisie": 37,
    "pro league":             38,
    "stoiximan super league": 185,
    "trendyol süper lig":     52,
    "süper lig":              52,
    "super lig":              52,
    "austrian bundesliga":    45,
    "scottish premiership":   36,
    "swiss super league":    215,
    "ekstraklasa":           202,
    "eliteserien":            20,
    "saudi pro league":      955,
    "mls":                   242,
    "liga mx":             11621,
    "brasileirão betano":    325,
    "brasileirão":           325,
    "brasileirão série b":   390,
    "uefa champions league":   7,
    "champions league":        7,
    "uefa europa league":    679,
    "europa league":         679,
    "uefa conference league": 17015,
    "conference league":   17015,
    "conmebol libertadores": 384,
    "copa libertadores":     384,
    "conmebol sudamericana": 480,
    "copa sudamericana":     480,
}


def _resolve_league_tid(name: str) -> int | None:
    """
    Resolve a canonical league name → Sofascore uniqueTournament ID.
    Tiered lookup: memo → upcoming cache → hardcoded fallback dict.
    Request-path-safe (no HTTP).
    """
    if name in _league_tid_memo and _league_tid_memo[name]:
        return _league_tid_memo[name]
    target = _normalize_tournament(name)
    # In-memory upcoming cache
    for date_key in _upcoming_cache:
        for m in _upcoming_cache[date_key].get("matches", []):
            if (_normalize_tournament(m.get("tournament") or "") == target
                    and m.get("tournamentId")):
                tid = m["tournamentId"]
                _league_tid_memo[name] = tid
                return tid
    # Hardcoded fallback for major competitions (handles between-rounds gaps)
    tid = _LEAGUE_TID_FALLBACK.get(target)
    if tid:
        _league_tid_memo[name] = tid
        return tid
    return None


def _league_logo(name: str) -> str | None:
    """
    Return the public logo URL for a competition. The URL is always
    deterministic — the proxy endpoint handles 404 gracefully when
    the tid hasn't been resolved yet (frontend falls back to monogram).
    This avoids JSON cache poisoning while warmup is still running.
    """
    if not name:
        return None
    target = _normalize_tournament(name)
    for slug, canonical in _slug_index_cache.get("leagues", {}).items():
        if _normalize_tournament(canonical) == target:
            return f"{API_BASE_URL}/api/league-logo/{slug}"
    return None


def _filter_monitored(matches: list[dict]) -> list[dict]:
    """
    Keep only matches whose (tournament, country) pair is in our strict
    monitored list. This is the same gate used by /api/today/monitored —
    correctly distinguishes English Premier League from Ukrainian PL,
    French Ligue 1 from Algerian Ligue 1, etc.
    """
    out = []
    for m in matches:
        tourn   = m.get("tournament") or ""
        country = m.get("country") or ""
        if _is_monitored_league_strict(tourn, country):
            out.append(m)
    return out


def _scan_upcoming(days_ahead: int = 3) -> list[dict]:
    """
    Read-only scan of `_upcoming_cache` for the next `days_ahead` days.
    NEVER calls Sofascore from the request path — that's the background
    loop's job (`_refresh_upcoming_cache`). Cold-cache responses just
    return whatever is in memory, which keeps p99 latency under 50ms.
    """
    now_ts = int(time.time())
    today  = datetime.now(timezone.utc)
    out: list[dict] = []
    seen: set = set()
    for offset in range(days_ahead):
        date_str = (today + timedelta(days=offset)).strftime("%Y-%m-%d")
        cached = _upcoming_cache.get(date_str)
        if not cached or not cached.get("matches"):
            continue
        for m in cached["matches"]:
            mid = m.get("id")
            ts  = m.get("startTimestamp") or 0
            if not mid or mid in seen or ts <= now_ts:
                continue
            seen.add(mid)
            out.append(m)
    out.sort(key=lambda x: x.get("startTimestamp") or 0)
    return out


def _upcoming_for_league(name: str, limit: int = 20) -> list[dict]:
    """
    Find upcoming matches for a canonical tournament name. Group/phase
    fragments are matched via _normalize_tournament so all stages of the
    same competition (e.g. Sudamericana Group A + Group H) appear together.
    Strict country gate prevents same-name leagues from other countries.
    """
    target = _normalize_tournament(name)  # lowercase canonical form
    out = []
    for m in _scan_upcoming():
        m_tourn = m.get("tournament") or ""
        if _normalize_tournament(m_tourn) != target:
            continue
        if not _is_monitored_league_strict(m_tourn, m.get("country") or ""):
            continue
        out.append({
            "match_id":   m.get("id"),
            "home_team":  m.get("homeTeam"),
            "away_team":  m.get("awayTeam"),
            "kickoff_ts": m.get("startTimestamp"),
        })
        if len(out) >= limit:
            break
    return out


def _team_performance(name: str, recent_n: int = 5) -> dict:
    """
    Aggregate recent on-pitch performance for a team from our local DB.

    Pulls the last `recent_n` finished games involving this team and computes:
      - recent_games:   list of {date, opponent, was_home, score, result, xg_for, xg_against}
      - form_letters:   chronological string of W/D/L for those games (e.g. "WWLDW")
      - streak:         {type: "win"|"loss"|"unbeaten"|"draw"|"none", length, text}
      - xg_summary:     {avg_for, avg_against, avg_goals_for, avg_goals_against,
                         sample_size, overperforming: bool|None}
      - sample_size:    how many tracked games we found (caller can adapt copy)

    All numbers are derived from data we actually observed (games + match_shots
    tables); we don't make up season totals or league standings.
    """
    out = {
        "recent_games":  [],
        "form_letters":  "",
        "streak":        {"type": "none", "length": 0, "text": ""},
        "xg_summary":    None,
        "sample_size":   0,
    }
    try:
        with _db() as conn:
            games = conn.execute("""
                SELECT id, home_team, away_team, home_goals, away_goals,
                       start_ts, tournament
                FROM games
                WHERE is_finished = 1
                  AND home_goals IS NOT NULL AND away_goals IS NOT NULL
                  AND (home_team = ? OR away_team = ?)
                ORDER BY start_ts DESC
                LIMIT ?
            """, (name, name, recent_n)).fetchall()

            if not games:
                return out
            out["sample_size"] = len(games)

            # Aggregate xG for/against from match_shots, keyed by match_id
            ids_csv = ",".join(str(g["id"]) for g in games)
            xg_rows = conn.execute(
                f"SELECT match_id, is_home, SUM(xg) AS sum_xg "
                f"FROM match_shots WHERE match_id IN ({ids_csv}) "
                f"GROUP BY match_id, is_home"
            ).fetchall()
            xg_by = {}
            for r in xg_rows:
                xg_by.setdefault(r["match_id"], {})[bool(r["is_home"])] = r["sum_xg"] or 0.0

        recent = []
        for g in games:  # already DESC by start_ts (most recent first)
            was_home = (g["home_team"] == name)
            opponent = g["away_team"] if was_home else g["home_team"]
            gf = g["home_goals"] if was_home else g["away_goals"]
            ga = g["away_goals"] if was_home else g["home_goals"]
            if gf > ga:    result = "W"
            elif gf < ga:  result = "L"
            else:          result = "D"

            xg_pair = xg_by.get(g["id"], {})
            xg_for     = xg_pair.get(was_home)         # team's own xG
            xg_against = xg_pair.get(not was_home)     # opponent's xG

            from datetime import datetime as _dt, timezone as _tz
            date_str = _dt.fromtimestamp(g["start_ts"], tz=_tz.utc).strftime("%b %d") if g["start_ts"] else ""

            recent.append({
                "match_id":   g["id"],
                "date":       date_str,
                "opponent":   opponent,
                "was_home":   was_home,
                "score":      f"{gf}-{ga}",     # always team's perspective
                "result":     result,
                "xg_for":     round(xg_for, 2)     if xg_for     is not None else None,
                "xg_against": round(xg_against, 2) if xg_against is not None else None,
                "tournament": g["tournament"] or "",
            })

        out["recent_games"] = recent
        # Form letters chronological (oldest → newest, like "WWLDW")
        out["form_letters"] = "".join(r["result"] for r in reversed(recent))

        # Streak — count consecutive most-recent results matching some category
        # Most recent first
        first = recent[0]["result"]
        if first == "W":
            i = 0
            while i < len(recent) and recent[i]["result"] == "W":
                i += 1
            out["streak"] = {
                "type": "win", "length": i,
                "text": (f"on a {i}-game winning run" if i >= 2 else "coming off a win"),
            }
        elif first == "L":
            i = 0
            while i < len(recent) and recent[i]["result"] == "L":
                i += 1
            out["streak"] = {
                "type": "loss", "length": i,
                "text": (f"coming off {i} consecutive losses" if i >= 2 else "coming off a defeat"),
            }
        else:  # first is "D"
            # Unbeaten run? Count consecutive non-losses
            i = 0
            while i < len(recent) and recent[i]["result"] in ("W", "D"):
                i += 1
            if i >= 2:
                out["streak"] = {
                    "type": "unbeaten", "length": i,
                    "text": f"unbeaten in their last {i}",
                }
            else:
                out["streak"] = {"type": "draw", "length": 1, "text": "drawing their last match"}

        # xG aggregates — only include matches where we have shot data on both sides
        valid_xg = [r for r in recent if r["xg_for"] is not None and r["xg_against"] is not None]
        if valid_xg:
            n = len(valid_xg)
            avg_for     = sum(r["xg_for"]     for r in valid_xg) / n
            avg_against = sum(r["xg_against"] for r in valid_xg) / n
            # Goals on the same sample for like-for-like comparison
            goals_for = []
            goals_against = []
            for r in valid_xg:
                gf, ga = r["score"].split("-")
                goals_for.append(int(gf))
                goals_against.append(int(ga))
            avg_gf = sum(goals_for) / n
            avg_ga = sum(goals_against) / n
            # Overperforming if scoring noticeably more than xG suggests
            overperforming = None
            if n >= 3 and abs(avg_gf - avg_for) > 0.3:
                overperforming = avg_gf > avg_for
            out["xg_summary"] = {
                "sample_size":       n,
                "avg_for":           round(avg_for, 2),
                "avg_against":       round(avg_against, 2),
                "avg_goals_for":     round(avg_gf, 2),
                "avg_goals_against": round(avg_ga, 2),
                "overperforming":    overperforming,
            }
    except Exception as e:
        log.warning(f"_team_performance({name}) failed: {e}")
    return out


def _next_fixture_for_team(name: str) -> dict | None:
    """Earliest upcoming match (any tournament) involving this team."""
    for m in _scan_upcoming():
        if m.get("homeTeam") != name and m.get("awayTeam") != name:
            continue
        return {
            "match_id":   m.get("id"),
            "home_team":  m.get("homeTeam"),
            "away_team":  m.get("awayTeam"),
            "tournament": m.get("tournament"),
            "kickoff_ts": m.get("startTimestamp"),
        }
    return None


def _seo_cache_get(key: str) -> str | None:
    entry = _seo_cache.get(key)
    if entry and (time.time() - entry["ts"]) < _SEO_CACHE_TTL:
        return entry["html"]
    return None


def _seo_cache_put(key: str, html: str) -> None:
    _seo_cache[key] = {"html": html, "ts": time.time()}


@app.route("/api/health/sofascore")
def r_health_sofascore():
    """
    Lightweight healthcheck for the Sofascore data pipeline. Used by the
    external auto-failover GitHub Action to decide whether to migrate the
    Fly.io app to a different region.

    Returns:
      live_games            — # of live monitored matches with score data
      upcoming_today        — # cached upcoming matches starting today (UTC)
      upcoming_tomorrow     — # cached upcoming matches starting tomorrow (UTC)
      last_cycle_age_s      — seconds since the last successful BG cycle
      healthy               — true if Sofascore looks reachable. False means
                              the watchdog should consider migrating regions.

    The "healthy" boolean is true when EITHER:
      - there is at least one live monitored match with score data, OR
      - there are upcoming matches in the next 2 days,
    AND the last BG cycle ran in the past 5 minutes.
    """
    try:
        now_utc       = datetime.now(timezone.utc)
        today_str     = now_utc.strftime("%Y-%m-%d")
        tomorrow_str  = (now_utc + timedelta(days=1)).strftime("%Y-%m-%d")

        with _state_lock:
            live_games = sum(
                1 for m in _live_state.values()
                if m.get("match", {}).get("homeGoals") is not None
                and not m.get("match", {}).get("isFinished", False)
            )

        up_today = len((_upcoming_cache.get(today_str)    or {}).get("matches", []))
        up_tomor = len((_upcoming_cache.get(tomorrow_str) or {}).get("matches", []))
        cycle_age = (time.time() - _last_cycle_ts) if _last_cycle_ts else 99999

        healthy = (
            (live_games > 0 or up_today > 0 or up_tomor > 0)
            and cycle_age < 300
        )

        return jsonify({
            "healthy":            healthy,
            "live_games":         live_games,
            "upcoming_today":     up_today,
            "upcoming_tomorrow":  up_tomor,
            "last_cycle_age_s":   round(cycle_age, 1),
            "ts":                 int(time.time()),
        })
    except Exception as e:
        log.error(f"r_health_sofascore error: {e}")
        return jsonify({"healthy": False, "error": str(e)}), 500


@app.route("/api/upcoming")
def r_upcoming():
    """
    Returns scheduled matches for the next N days, grouped by date.
    Purely served from in-memory cache — ZERO blocking I/O in the request path.
    Cache is populated/refreshed by the background loop (_refresh_upcoming_cache).
    If a date is not yet cached, returns [] for that day (frontend retries).
    """
    try:
        days_ahead  = flask_request.args.get("days", 3, type=int)
        days_ahead  = max(1, min(days_ahead, 7))
        now_utc     = datetime.now(timezone.utc)
        result_days = []
        total       = 0
        day_labels  = ["Today", "Tomorrow"]

        for offset in range(days_ahead):
            target_dt = now_utc + timedelta(days=offset)
            date_str  = target_dt.strftime("%Y-%m-%d")
            label     = day_labels[offset] if offset < len(day_labels) else target_dt.strftime("%A")
            cached    = _upcoming_cache.get(date_str)   # GIL-safe dict read, no lock needed
            matches   = cached["matches"] if cached else []
            result_days.append({"date": date_str, "label": label, "matches": matches})
            total += len(matches)

        return jsonify({"days": result_days, "total": total})
    except Exception as e:
        log.error(f"r_upcoming error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/state")
def r_state():
    """
    Returns the full pre-computed live state for all monitored games.
    This is what the dashboard polls — zero Odds API requests from the browser.
    """
    with _state_lock:
        state_copy = dict(_live_state)
    return jsonify({
        "games":    list(state_copy.values()),
        "count":    len(state_copy),
        "cycleTsIso": datetime.fromtimestamp(_last_cycle_ts, tz=timezone.utc).isoformat() if _last_cycle_ts else None,
        "cycleReq": _last_cycle_req,
        "quotaRemaining": _api_requests_remaining,
        "ts": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/match/<int:mid>/timeline")
def r_match_timeline(mid: int):
    """
    Returns full xG timeline data for a match — used for the animated replay UI.
    Response:
      {
        "match": { id, home_team, away_team, home_goals, away_goals, tournament, start_ts },
        "shots": [ { minute, added_time, is_home, xg, is_goal, is_penalty, player, shot_type } ],
        "tips":  [ { minute_entry, market, label, odd_entry, result,
                     xg_home_at_entry, xg_away_at_entry, wall_ts } ]
      }
    """
    try:
        with _db() as conn:
            game = conn.execute(
                "SELECT * FROM games WHERE id = ?", (mid,)
            ).fetchone()
            if not game:
                return jsonify({"error": "match not found"}), 404

            shots = conn.execute("""
                SELECT minute, added_time, is_home, xg, is_goal, is_penalty,
                       player, shot_type, situation, body_part
                FROM match_shots
                WHERE match_id = ?
                ORDER BY minute, added_time, is_home
            """, (mid,)).fetchall()

            tips = conn.execute("""
                SELECT minute_entry, market, label, odd_entry, result,
                       xg_home_at_entry, xg_away_at_entry, wall_ts, edge_entry
                FROM tips
                WHERE match_id = ?
                ORDER BY wall_ts
            """, (mid,)).fetchall()

        return jsonify({
            "match": dict(game),
            "shots": [dict(s) for s in shots],
            "tips":  [dict(t) for t in tips],
        })
    except Exception as e:
        log.exception(f"r_match_timeline failed for {mid}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/match/<int:mid>/timeline/export.png")
def r_match_timeline_export(mid: int):
    """
    Generates a branded 1200×630 PNG of the xG timeline — ready for social sharing.
    Uses matplotlib with Agg backend (no display needed).

    Query params:
      ?theme=dark|light   (default: dark)
    """
    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import matplotlib.patheffects as pe
    import numpy as np

    theme = flask_request.args.get("theme", "dark")
    is_dark = theme != "light"

    # ── Palette ─────────────────────────────────────────────────────────────
    BG      = "#0f172a" if is_dark else "#f8fafc"
    PANEL   = "#1e293b" if is_dark else "#ffffff"
    TEXT    = "#f1f5f9" if is_dark else "#0f172a"
    SUBTEXT = "#94a3b8" if is_dark else "#64748b"
    GRID    = "#334155" if is_dark else "#e2e8f0"
    HOME_C  = "#22c55e"   # green
    AWAY_C  = "#f97316"   # orange
    WIN_C   = "#22c55e"
    LOSS_C  = "#ef4444"
    VOID_C  = "#94a3b8"

    try:
        with _db() as conn:
            game = conn.execute("SELECT * FROM games WHERE id = ?", (mid,)).fetchone()
            if not game:
                return Response("Match not found", status=404, mimetype="text/plain")
            shots = conn.execute("""
                SELECT minute, added_time, is_home, xg, is_goal
                FROM match_shots WHERE match_id = ?
                ORDER BY minute, added_time
            """, (mid,)).fetchall()
            tips = conn.execute("""
                SELECT minute_entry, market, label, odd_entry, result
                FROM tips WHERE match_id = ?
                ORDER BY wall_ts
            """, (mid,)).fetchall()
        game = dict(game)
        shots = [dict(s) for s in shots]
        tips  = [dict(t) for t in tips]
    except Exception as e:
        log.exception(f"export png failed for {mid}")
        return Response(f"Error: {e}", status=500, mimetype="text/plain")

    home_team = game.get("home_team", "Home")
    away_team = game.get("away_team", "Away")
    home_g    = game.get("home_goals", 0) or 0
    away_g    = game.get("away_goals", 0) or 0
    tourn     = game.get("tournament", "")

    # ── Build cumulative xG series ────────────────────────────────────────
    max_min = max((s["minute"] for s in shots), default=90)
    max_min = max(max_min, 90)

    home_pts = [(0, 0.0)]
    away_pts = [(0, 0.0)]
    h_cum = 0.0
    a_cum = 0.0
    goal_minutes = []

    for s in shots:
        m = s["minute"] + (s.get("added_time") or 0) * 0.1
        if s["is_home"]:
            h_cum += s["xg"]
            home_pts.append((m, round(h_cum, 4)))
        else:
            a_cum += s["xg"]
            away_pts.append((m, round(a_cum, 4)))
        if s.get("is_goal"):
            goal_minutes.append(m)

    home_xs, home_ys = zip(*home_pts) if home_pts else ([0], [0])
    away_xs, away_ys = zip(*away_pts) if away_pts else ([0], [0])

    # ── Figure ────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(12, 6.3), facecolor=BG)
    ax  = fig.add_axes([0.07, 0.17, 0.88, 0.68], facecolor=PANEL)

    # Grid
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=GRID, linewidth=0.6, linestyle="--", alpha=0.6)
    ax.xaxis.grid(False)
    for spine in ax.spines.values():
        spine.set_color(GRID)
        spine.set_linewidth(0.5)

    # xG lines (step-after)
    ax.step(home_xs, home_ys, where="post", color=HOME_C, linewidth=2.5,
            label=home_team, solid_capstyle="round", zorder=3)
    ax.step(away_xs, away_ys, where="post", color=AWAY_C, linewidth=2.5,
            label=away_team, solid_capstyle="round", zorder=3)

    # Filled area under lines
    ax.fill_between(home_xs, home_ys, step="post", alpha=0.08, color=HOME_C)
    ax.fill_between(away_xs, away_ys, step="post", alpha=0.08, color=AWAY_C)

    # Goal markers — triangle on the x-axis
    for gm in goal_minutes:
        ax.axvline(gm, color=SUBTEXT, linewidth=0.8, linestyle=":", alpha=0.5, zorder=1)
        ax.scatter([gm], [0], marker="^", s=40, color=SUBTEXT,
                   zorder=4, clip_on=False)

    # Half-time line
    ax.axvline(45, color=SUBTEXT, linewidth=0.7, linestyle="--", alpha=0.35, zorder=1)

    # ── Pick annotations ─────────────────────────────────────────────────
    y_max = max(max(home_ys), max(away_ys), 0.5)
    y_max_pad = y_max * 1.15

    tip_label_y_offsets = []  # track y positions to avoid overlap
    for tip in tips:
        m   = tip.get("minute_entry") or 0
        res = (tip.get("result") or "").lower()
        col = WIN_C if res in ("green", "win") else (LOSS_C if res in ("red", "loss") else VOID_C)

        ax.axvline(m, color=col, linewidth=1.4, linestyle="--", alpha=0.85, zorder=2)

        # Badge box
        label_short = tip.get("label", "")[:14]
        mkt   = tip.get("market", "")
        badge = f"{mkt[:5]}  {label_short}  @{tip.get('odd_entry', '')}"

        # Stagger y position for overlapping tips at same minute
        y_pos = y_max_pad * 0.92
        for (prev_m, prev_y) in tip_label_y_offsets:
            if abs(prev_m - m) < 6:
                y_pos = prev_y - y_max_pad * 0.18
        tip_label_y_offsets.append((m, y_pos))

        ax.text(
            m, y_pos, badge,
            ha="center", va="center",
            fontsize=6.5, color="white" if is_dark else "#0f172a",
            fontweight="bold",
            bbox=dict(
                boxstyle="round,pad=0.35",
                facecolor=col,
                edgecolor="none",
                alpha=0.88,
            ),
            zorder=5,
        )

    # ── Axes formatting ───────────────────────────────────────────────────
    ax.set_xlim(0, max_min + 2)
    ax.set_ylim(0, y_max_pad)
    ax.tick_params(colors=SUBTEXT, labelsize=8)
    ax.set_xlabel("Minute", color=SUBTEXT, fontsize=9, labelpad=4)
    ax.set_ylabel("xG", color=SUBTEXT, fontsize=9, labelpad=4)
    for tl in ax.get_xticklabels() + ax.get_yticklabels():
        tl.set_color(SUBTEXT)

    # ── Title ─────────────────────────────────────────────────────────────
    title_line1 = f"{home_team}  {home_g} – {away_g}  {away_team}"
    fig.text(0.5, 0.94, title_line1, ha="center", va="top",
             fontsize=14, fontweight="bold", color=TEXT)
    if tourn:
        fig.text(0.5, 0.895, tourn, ha="center", va="top",
                 fontsize=9, color=SUBTEXT)

    # ── Legend ────────────────────────────────────────────────────────────
    h_patch = mpatches.Patch(color=HOME_C, label=f"{home_team}  (xG {round(h_cum,2)})")
    a_patch = mpatches.Patch(color=AWAY_C, label=f"{away_team}  (xG {round(a_cum,2)})")
    ax.legend(handles=[h_patch, a_patch], loc="upper left",
              facecolor=PANEL, edgecolor=GRID,
              labelcolor=TEXT, fontsize=8.5, framealpha=0.9)

    # ── Branding ─────────────────────────────────────────────────────────
    fig.text(0.96, 0.035, "webpronos.com", ha="right", va="bottom",
             fontsize=8, color=SUBTEXT, fontstyle="italic")
    fig.text(0.04, 0.035, "xG Timeline Replay", ha="left", va="bottom",
             fontsize=8, color=SUBTEXT)

    # ── Render to bytes ───────────────────────────────────────────────────
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=BG, edgecolor="none")
    plt.close(fig)
    buf.seek(0)

    fname = f"webpronos-{_slug(home_team)}-vs-{_slug(away_team)}.png"
    return Response(
        buf.read(),
        mimetype="image/png",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "public, max-age=86400",
        },
    )


@app.route("/api/state/tips")
def r_state_tips():
    """Returns tip history (all games with tips) from the DB, optionally filtered by date range."""
    from_ts = flask_request.args.get("from_ts", type=int)   # unix seconds
    to_ts   = flask_request.args.get("to_ts",   type=int)
    limit   = flask_request.args.get("limit", 500, type=int)

    date_where = ""
    params = []
    if from_ts:
        date_where += " AND coalesce(g.start_ts, g.archived_at) >= ?"
        params.append(from_ts)
    if to_ts:
        date_where += " AND coalesce(g.start_ts, g.archived_at) <= ?"
        params.append(to_ts)

    with _db() as conn:
        games = conn.execute(f"""
            SELECT g.*, COUNT(t.tip_key) as tip_count
            FROM games g
            LEFT JOIN tips t ON t.match_id = g.id
            WHERE 1=1 {date_where}
            GROUP BY g.id
            HAVING COUNT(t.tip_key) > 0
            ORDER BY coalesce(g.start_ts, g.archived_at) DESC
            LIMIT ?
        """, (*params, limit)).fetchall()
        result = []
        for g in games:
            gd = dict(g)
            tips_rows = conn.execute(
                "SELECT * FROM tips WHERE match_id = ? AND (minute_entry IS NULL OR minute_entry <= ?) ORDER BY wall_ts",
                (g["id"], get_setting("max_minute_for_tips", 85))
            ).fetchall()
            gd["tips"] = [dict(t) for t in tips_rows]
            # Inject logos directly so the frontend makes zero extra requests
            gd["home_logo"] = _quick_logo(g["home_team"])
            gd["away_logo"] = _quick_logo(g["away_team"])
            result.append(gd)
    return jsonify({"games": result, "count": len(result)})


@app.route("/api/admin/backfill-team-ids", methods=["POST"])
def r_backfill_team_ids():
    """
    Server-side backfill: fetch team IDs from Sofascore for all games missing them.
    Uses the server's curl_cffi session (bypasses Cloudflare).
    Run once via: POST /api/admin/backfill-team-ids
    """
    import threading

    def _do_backfill():
        with _db() as conn:
            rows = conn.execute("""
                SELECT id FROM games
                WHERE home_team_id IS NULL OR away_team_id IS NULL
                ORDER BY start_ts DESC
            """).fetchall()

        log.info(f"[backfill] {len(rows)} games need team IDs")
        updated = 0
        for r in rows:
            mid = r["id"]
            try:
                data = _get(f"{SOFASCORE_API}/event/{mid}")
                if not data:
                    continue
                event = data.get("event", {})
                ht_id = event.get("homeTeam", {}).get("id")
                at_id = event.get("awayTeam", {}).get("id")
                if ht_id or at_id:
                    with _db() as conn:
                        conn.execute("""
                            UPDATE games
                            SET home_team_id = COALESCE(?, home_team_id),
                                away_team_id = COALESCE(?, away_team_id)
                            WHERE id = ?
                        """, (ht_id, at_id, mid))
                    updated += 1
                    log.info(f"[backfill] {mid}: ht={ht_id} at={at_id}")
                time.sleep(0.3)
            except Exception as e:
                log.warning(f"[backfill] {mid} failed: {e}")

        log.info(f"[backfill] done — {updated}/{len(rows)} games updated")

    threading.Thread(target=_do_backfill, daemon=True).start()
    return jsonify({"status": "backfill started in background — check logs"})


@app.route("/api/teams")
def r_teams():
    """
    Returns every unique team in the DB with its Sofascore team ID.
    Used to build the logo map: logo URL = https://api.sofascore.app/api/v1/team/{id}/image
    """
    with _db() as conn:
        rows = conn.execute("""
            SELECT home_team as name, home_team_id as id FROM games WHERE home_team_id IS NOT NULL
            UNION
            SELECT away_team as name, away_team_id as id FROM games WHERE away_team_id IS NOT NULL
            ORDER BY name
        """).fetchall()
    teams = {r["name"]: r["id"] for r in rows}
    return jsonify({"teams": teams, "count": len(teams)})


@app.route("/api/state/tips/<int:match_id>", methods=["PATCH"])
def r_update_tip_result(match_id):
    """
    Manual result override for a tip.
    Body: {"tip_key": "...", "result": "green"|"red"|"void"|null}
    """
    body = flask_request.get_json(silent=True) or {}
    tip_key = body.get("tip_key")
    result  = body.get("result")  # null clears it

    if not tip_key:
        return jsonify({"error": "tip_key required"}), 400
    if result not in (None, "green", "red", "void"):
        return jsonify({"error": "result must be green|red|void|null"}), 400

    with _db() as conn:
        conn.execute(
            "UPDATE tips SET result = ? WHERE tip_key = ? AND match_id = ?",
            (result, tip_key, match_id)
        )
    return jsonify({"ok": True, "tip_key": tip_key, "result": result})


@app.route("/api/state/tips/<int:match_id>/delete", methods=["POST"])
def r_delete_tip(match_id):
    """
    Permanently delete a tip from the DB.
    Body: {"tip_key": "..."}
    """
    body = flask_request.get_json(silent=True) or {}
    tip_key = body.get("tip_key")

    if not tip_key:
        return jsonify({"error": "tip_key required"}), 400

    with _db() as conn:
        conn.execute(
            "DELETE FROM tips WHERE tip_key = ? AND match_id = ?",
            (tip_key, match_id)
        )
    log.info(f"Deleted tip {tip_key} from match {match_id}")
    return jsonify({"ok": True, "tip_key": tip_key})


# ── Team Logos ──
_LOGOS_SHEET = (
    "https://docs.google.com/spreadsheets/d/"
    "1tDUlWmZZcJKXHd0Nlr5QIm1V15OMsvkOgfhXUuPI9_M/"
    "gviz/tq?tqx=out:csv&sheet=footballstats+team+logo"
)
_logos_cache: dict = {}        # original_name → url
_logos_norm_cache: dict = {}   # normalized_name → url  (for fuzzy lookup)
_logos_ts: float  = 0.0
_LOGOS_TTL = 600               # refresh every 10 min

# Words to strip when normalizing team names for matching
_NOISE_WORDS = {"fc", "cf", "sc", "ac", "as", "afc", "fk", "sk", "bk", "1fc",
                "club", "calcio", "united", "city", "town", "rovers", "wanderers",
                "athletic", "athletics", "real", "sporting", "de", "du", "the"}

def _normalize_team_for_logo(name: str) -> str:
    """
    Normalize a team name for LOGO fuzzy matching only.
    NOTE: separate from `_normalize_team()` used for odds matching, which strips
    "SC "/"FC "/etc. prefixes — those are needed when comparing Odds API names
    but would corrupt logo lookups.
      'Atlético de Madrid' → 'atletico de madrid'
      'Manchester Utd'     → 'manchester utd'
    """
    import unicodedata, re as _re
    # strip accents
    nfkd = unicodedata.normalize("NFD", name)
    ascii_name = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    # lowercase, keep alphanumeric + spaces
    cleaned = _re.sub(r"[^a-z0-9 ]", " ", ascii_name.lower())
    # collapse whitespace
    cleaned = " ".join(cleaned.split())
    return cleaned

def _load_logos():
    global _logos_cache, _logos_norm_cache, _logos_ts
    import csv, io
    try:
        resp = _session.get(_LOGOS_SHEET, timeout=30)
        resp.raise_for_status()
        reader = csv.reader(io.StringIO(resp.text))
        logos = {}
        logos_norm = {}
        for row in reader:
            # Two paired columns: (col0=name, col1=url) and (col3=name, col5=url)
            for name_i, url_i in [(0, 1), (3, 5)]:
                if len(row) > url_i:
                    name = row[name_i].strip()
                    url  = row[url_i].strip()
                    if name and url.startswith("http"):
                        logos[name] = url
                        # also index by normalized key for fuzzy lookups
                        nkey = _normalize_team_for_logo(name)
                        if nkey and nkey not in logos_norm:
                            logos_norm[nkey] = url
        _logos_cache      = logos
        _logos_norm_cache = logos_norm
        _logos_ts         = time.time()
        log.info(f"Team logos loaded: {len(logos)} entries ({len(logos_norm)} normalized)")
    except Exception as e:
        log.error(f"Failed to load team logos: {e}")

def _get_logos():
    # NEVER block a gevent worker — curl_cffi is not gevent-compatible.
    # If cache is empty or stale, kick off a background thread and return
    # whatever we have immediately (empty dict on first boot, stale on refresh).
    if time.time() - _logos_ts > _LOGOS_TTL:
        threading.Thread(target=_load_logos, daemon=True).start()
    return _logos_cache  # may be {} until background thread finishes

# Memoization cache: team name → logo URL (or None). Persisted across requests
# so each unique team is fuzzy-matched only ONCE per server lifetime.
_fuzzy_logo_memo: dict = {}

def _quick_logo(name: str) -> str | None:
    """
    Fast logo lookup — exact + normalized only, NO fuzzy fallback.
    Use this in bulk endpoints (state/tips, upcoming, live state) where 700+
    teams may be looked up per request. Fuzzy matching against 10k logos is
    far too slow for that path. Returns None if no exact match.
    """
    if not name:
        return None
    if name in _fuzzy_logo_memo:
        return _fuzzy_logo_memo[name]
    logos = _get_logos()
    if not logos:
        return None
    # Exact match
    if name in logos:
        _fuzzy_logo_memo[name] = logos[name]
        return logos[name]
    # Normalized exact match
    nkey = _normalize_team_for_logo(name)
    if nkey in _logos_norm_cache:
        url = _logos_norm_cache[nkey]
        _fuzzy_logo_memo[name] = url
        return url
    # No exact match — cache miss as None to avoid retrying
    _fuzzy_logo_memo[name] = None
    return None


def _fuzzy_logo(name: str, threshold: float = 0.72) -> str | None:
    """
    Return a logo URL for *name* using a tiered lookup:
      1. Memoized result  (instant after first lookup)
      2. Exact match       (original keys)
      3. Normalized exact match
      4. Best fuzzy match via SequenceMatcher (above threshold)
    Returns None when no acceptable match is found.
    """
    if not name:
        return None

    # 1. memoized — avoids redoing SequenceMatcher across 10k logos every call
    if name in _fuzzy_logo_memo:
        return _fuzzy_logo_memo[name]

    logos = _get_logos()
    # If logos cache is still cold, don't write None to memo (so we retry later)
    if not logos:
        return None

    # 2. exact
    if name in logos:
        _fuzzy_logo_memo[name] = logos[name]
        return logos[name]

    # 3. normalized exact
    norm_cache = _logos_norm_cache
    nkey = _normalize_team_for_logo(name)
    if nkey in norm_cache:
        _fuzzy_logo_memo[name] = norm_cache[nkey]
        return norm_cache[nkey]

    # 4. fuzzy over normalized keys (slow — only runs once per unique name)
    best_score = 0.0
    best_url   = None
    for stored_norm, url in norm_cache.items():
        score = SequenceMatcher(None, nkey, stored_norm).ratio()
        if score > best_score:
            best_score = score
            best_url   = url
    result = best_url if best_score >= threshold else None
    _fuzzy_logo_memo[name] = result
    return result


_FUZZY_MEMO_PATH = str(DB_PATH.parent / "fuzzy_logo_memo.json")


def _load_fuzzy_memo_from_disk():
    """Load previously-computed fuzzy memo from disk into memory. Cheap, fast."""
    try:
        if os.path.exists(_FUZZY_MEMO_PATH):
            with open(_FUZZY_MEMO_PATH, "r", encoding="utf-8") as f:
                disk_memo = json.load(f)
            _fuzzy_logo_memo.update(disk_memo)
            log.info(f"Fuzzy logo memo loaded: {len(disk_memo)} entries from disk")
    except Exception as e:
        log.warning(f"Failed to load fuzzy memo from disk: {e}")


def _prewarm_fuzzy_logos():
    """
    Run the heavy fuzzy resolution in a SEPARATE OS process (subprocess) so
    it completely bypasses gevent's monkey-patching. The subprocess writes
    the memo to disk; the running gunicorn workers reload it via SIGUSR1
    or simply on the next boot.

    This guarantees zero impact on request latency — the gunicorn workers
    keep serving requests at full speed while the subprocess crunches.
    """
    import subprocess
    script_path = os.path.abspath(__file__)
    log.info("_prewarm_fuzzy_logos: spawning subprocess for fuzzy resolution...")
    try:
        # Use Popen so it's fully detached and doesn't block our caller.
        proc = subprocess.Popen(
            [sys.executable, script_path, "prewarm-logos"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        def _drain_and_reload():
            """Stream subprocess output to logs; reload memo when done."""
            try:
                if proc.stdout:
                    for line in proc.stdout:
                        log.info(f"[prewarm-subprocess] {line.decode().rstrip()}")
                proc.wait()
                log.info(f"_prewarm_fuzzy_logos: subprocess exited with code {proc.returncode}")
                _load_fuzzy_memo_from_disk()
            except Exception as e:
                log.warning(f"_prewarm_fuzzy_logos: drain failed: {e}")

        threading.Thread(target=_drain_and_reload, daemon=True).start()
    except Exception as e:
        log.error(f"_prewarm_fuzzy_logos: failed to spawn subprocess: {e}")


def _run_prewarm_cli():
    """
    Standalone CLI entry: `python server.py prewarm-logos`.
    Runs fuzzy resolution synchronously (no gevent here — pure CPython
    process) and persists the memo to disk.
    """
    print(f"[prewarm-cli] starting")
    _load_aliases()
    _init_db()
    _init_client()           # _load_logos uses _session
    _load_logos()
    if not _logos_cache:
        print("[prewarm-cli] FATAL: logos cache empty"); return

    with _db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT home_team AS name FROM games "
            "UNION SELECT DISTINCT away_team AS name FROM games"
        ).fetchall()
    names = sorted({r["name"] for r in rows if r["name"]})

    # Load existing memo to skip already-resolved teams
    if os.path.exists(_FUZZY_MEMO_PATH):
        try:
            with open(_FUZZY_MEMO_PATH, "r", encoding="utf-8") as f:
                _fuzzy_logo_memo.update(json.load(f))
            print(f"[prewarm-cli] loaded {len(_fuzzy_logo_memo)} existing entries")
        except Exception as e:
            print(f"[prewarm-cli] failed to load existing memo: {e}")

    pending = [n for n in names if n not in _fuzzy_logo_memo]
    print(f"[prewarm-cli] {len(pending)} new teams to resolve "
          f"({len(names) - len(pending)} cached)")

    t0 = time.time()
    resolved = 0
    for i, name in enumerate(pending):
        url = _fuzzy_logo(name)
        if url:
            resolved += 1
        if (i + 1) % 50 == 0:
            print(f"[prewarm-cli] progress: {i+1}/{len(pending)} "
                  f"({(time.time()-t0):.1f}s elapsed)")

    try:
        with open(_FUZZY_MEMO_PATH, "w", encoding="utf-8") as f:
            json.dump(_fuzzy_logo_memo, f, ensure_ascii=False)
        print(f"[prewarm-cli] persisted {len(_fuzzy_logo_memo)} entries → {_FUZZY_MEMO_PATH}")
    except Exception as e:
        print(f"[prewarm-cli] FATAL persist failed: {e}")

    print(f"[prewarm-cli] done in {time.time()-t0:.1f}s — {resolved}/{len(pending)} resolved")


@app.route("/api/admin/prewarm-logos", methods=["POST", "GET"])
def r_prewarm_logos():
    """Manually trigger the fuzzy logo prewarm (also runs automatically on boot)."""
    threading.Thread(target=_prewarm_fuzzy_logos, daemon=True).start()
    return jsonify({"ok": True, "message": "Prewarm started in background — check logs"})


@app.route("/api/team_logos")
def r_team_logos():
    return jsonify({"teams": _get_logos(), "count": len(_logos_cache)})

@app.route("/api/team_logo/<path:name>")
def r_team_logo_lookup(name: str):
    """
    Fuzzy logo lookup for a single team name.
    GET /api/team_logo/Manchester%20Utd
    → {"name": "Manchester Utd", "url": "https://...", "matched": true}
    """
    url = _fuzzy_logo(name)
    if url:
        return jsonify({"name": name, "url": url, "matched": True})
    return jsonify({"name": name, "url": None, "matched": False}), 404

@app.route("/api/team_logos/batch", methods=["POST"])
def r_team_logos_batch():
    """
    Resolve logos for many teams in one request.
    POST JSON: {"teams": ["Manchester Utd", "Real Betis", ...]}
    → {"results": {"Manchester Utd": "https://...", "Real Betis": null}}
    """
    data  = flask_request.get_json(force=True, silent=True) or {}
    names = data.get("teams", [])
    results = {}
    for n in names[:200]:   # cap at 200 per call
        results[n] = _fuzzy_logo(n)
    return jsonify({"results": results, "resolved": sum(1 for v in results.values() if v)})

@app.route("/api/team_logos/refresh", methods=["POST", "GET"])
def r_team_logos_refresh():
    """Force immediate reload of team logos from Google Sheets."""
    global _logos_ts
    _logos_ts = 0.0  # Expire cache immediately
    _load_logos()
    return jsonify({"ok": True, "count": len(_logos_cache), "message": f"Loaded {len(_logos_cache)} logos from Google Sheets"})


# ─── Sitemap helpers ───────────────────────────────────────────────────────────

def _slug(text: str) -> str:
    """Convert team name to URL-friendly slug: 'Sporting CP' → 'sporting-cp'."""
    import unicodedata, re as _re
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")  # strip accents
    text = text.lower().strip()
    text = _re.sub(r"[^\w\s-]", "", text)
    text = _re.sub(r"[\s_]+", "-", text)
    return text.strip("-")


def _match_url(match_id, home: str = "", away: str = "") -> str:
    """
    Canonical URL for a match page, including the home-vs-away slug.

    Use this everywhere internal links to /match/<id> are emitted so they
    line up with the URLs in /sitemap.xml. Without the slug the bare-id
    form returns the same content but Google sees two URLs for one page,
    splits authority, and may pick the wrong one as canonical.

    Falls back to the bare-id form if team names aren't available — better
    than a broken link.
    """
    if home and away:
        return f"{SITE_URL}/match/{match_id}/{_slug(home)}-vs-{_slug(away)}"
    return f"{SITE_URL}/match/{match_id}"


@app.route("/robots.txt")
def r_robots():
    """robots.txt — points crawlers to the sitemap index, blocks API/admin paths."""
    body = (
        "User-agent: *\n"
        "Disallow: /api/\n"
        "Disallow: /admin/\n"
        "Disallow: /telegram/\n"
        "Disallow: /prerender\n"
        "Disallow: /proxy/\n"
        "Allow: /\n"
        "\n"
        f"Sitemap: {SITE_URL}/sitemap.xml\n"
    )
    return Response(body, mimetype="text/plain", headers={"Cache-Control": "public, max-age=86400"})


def _xml_url(loc: str, lastmod: str, changefreq: str, priority: str) -> str:
    return (
        f"  <url>\n"
        f"    <loc>{loc}</loc>\n"
        f"    <lastmod>{lastmod}</lastmod>\n"
        f"    <changefreq>{changefreq}</changefreq>\n"
        f"    <priority>{priority}</priority>\n"
        f"  </url>"
    )


def _xml_response(body: str, count: int) -> "Response":
    return Response(
        body,
        mimetype="application/xml",
        headers={
            "Cache-Control": "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls": str(count),
        },
    )


def _sm_urls_pages() -> list[str]:
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    pages = [
        (SITE_URL + "/",                     today, "daily",   "1.0"),
        (SITE_URL + "/today",                today, "always",  "0.95"),
        (SITE_URL + "/tomorrow",             today, "daily",   "0.9"),
        (SITE_URL + "/history",              today, "daily",   "0.8"),
        (SITE_URL + "/blog",                 today, "weekly",  "0.7"),
        (SITE_URL + "/about",                today, "monthly", "0.5"),
        (SITE_URL + "/responsible-gambling", today, "yearly",  "0.3"),
        (SITE_URL + "/terms",                today, "yearly",  "0.3"),
        (SITE_URL + "/privacy",              today, "yearly",  "0.3"),
    ]
    for slug in _TIP_MARKET_LABELS.keys():
        pages.append((f"{SITE_URL}/tips/{slug}", today, "daily", "0.7"))
    return [_xml_url(*p) for p in pages]


def _sm_urls_teams() -> list[str]:
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not _slug_index_cache["teams"]:
        _refresh_slug_index()
    urls: list[str] = []
    try:
        with _db() as conn:
            for slug, name in _slug_index_cache["teams"].items():
                row = conn.execute("""
                    SELECT MAX(t.wall_ts) AS last_ts
                    FROM tips t JOIN games g ON g.id = t.match_id
                    WHERE g.home_team = ? OR g.away_team = ?
                """, (name, name)).fetchone()
                last_ts = row["last_ts"] if row and row["last_ts"] else None
                lastmod = (datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                           if last_ts else today)
                urls.append(_xml_url(f"{SITE_URL}/team/{slug}", lastmod, "weekly", "0.6"))
    except Exception as e:
        log.warning(f"sitemap-teams: {e}")
    return urls


def _sm_urls_leagues() -> list[str]:
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not _slug_index_cache["leagues"]:
        _refresh_slug_index()
    urls: list[str] = []
    try:
        with _db() as conn:
            for slug, name in _slug_index_cache["leagues"].items():
                variants = _league_variants_for(name)
                placeholders = ",".join("?" * len(variants))
                row = conn.execute(f"""
                    SELECT MAX(t.wall_ts) AS last_ts
                    FROM tips t JOIN games g ON g.id = t.match_id
                    WHERE g.tournament IN ({placeholders})
                """, tuple(variants)).fetchone()
                last_ts = row["last_ts"] if row and row["last_ts"] else None
                lastmod = (datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                           if last_ts else today)
                urls.append(_xml_url(f"{SITE_URL}/league/{slug}", lastmod, "daily", "0.7"))
    except Exception as e:
        log.warning(f"sitemap-leagues: {e}")
    return urls


def _sm_urls_matches() -> list[str]:
    """
    Includes:
      - Live matches in progress
      - Scheduled matches in the next 30 days (added ~48h before kickoff
        once they land in the games table; sitemap regenerated hourly)
      - Finished matches with ≥1 tip, but ONLY within the last 30 days
        (after that they drop out of the sitemap to free crawl budget;
        the URLs themselves keep working — only sitemap inclusion expires)
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    next_30d = now_ts + 30 * 86400
    last_7d  = now_ts - 7 * 86400

    urls: list[str] = []
    seen: set[int] = set()

    # 1. Live games
    try:
        with _state_lock:
            for entry in _live_state.values():
                m = entry.get("match", {})
                mid = m.get("id")
                if mid and m.get("statusType") == "inprogress":
                    slug_part = f"{_slug(m.get('homeTeam', 'home'))}-{_slug(m.get('awayTeam', 'away'))}"
                    urls.append(_xml_url(
                        f"{SITE_URL}/match/{mid}/{slug_part}",
                        now.strftime("%Y-%m-%d"), "always", "0.95"))
                    seen.add(mid)
    except Exception:
        pass

    # 2. Scheduled (next 30d) + finished-with-picks
    try:
        with _db() as conn:
            scheduled = conn.execute("""
                SELECT id, home_team, away_team, start_ts
                FROM games
                WHERE is_finished = 0 AND start_ts > ? AND start_ts <= ?
                ORDER BY start_ts ASC
                LIMIT 5000
            """, (now_ts, next_30d)).fetchall()
            for r in scheduled:
                if r["id"] in seen:
                    continue
                slug_part = f"{_slug(r['home_team'])}-{_slug(r['away_team'])}"
                urls.append(_xml_url(
                    f"{SITE_URL}/match/{r['id']}/{slug_part}",
                    now.strftime("%Y-%m-%d"), "daily", "0.8"))
                seen.add(r["id"])

            finished = conn.execute("""
                SELECT g.id, g.home_team, g.away_team, MAX(t.wall_ts) AS last_ts
                FROM games g JOIN tips t ON t.match_id = g.id
                WHERE g.is_finished = 1 AND g.start_ts >= ?
                GROUP BY g.id
                ORDER BY last_ts DESC
                LIMIT 10000
            """, (last_7d,)).fetchall()
            for r in finished:
                if r["id"] in seen:
                    continue
                slug_part = f"{_slug(r['home_team'])}-{_slug(r['away_team'])}"
                lastmod = (datetime.fromtimestamp(r["last_ts"], tz=timezone.utc).strftime("%Y-%m-%d")
                           if r["last_ts"] else now.strftime("%Y-%m-%d"))
                urls.append(_xml_url(
                    f"{SITE_URL}/match/{r['id']}/{slug_part}",
                    lastmod, "monthly", "0.5"))
                seen.add(r["id"])
    except Exception as e:
        log.warning(f"sitemap-matches: {e}")
    return urls


def _sm_urls_blog() -> list[str]:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    urls: list[str] = []
    try:
        import urllib.request as _ur
        supa_url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?select=slug,published_at&order=published_at.desc&limit=500"
        )
        req = _ur.Request(supa_url, headers={
            "apikey":        SUPABASE_ANON,
            "Authorization": f"Bearer {SUPABASE_ANON}",
        })
        with _ur.urlopen(req, timeout=4) as r:
            posts = json.loads(r.read())
        for post in posts:
            slug = post.get("slug", "")
            pub  = (post.get("published_at") or now.isoformat())[:10]
            if slug:
                urls.append(_xml_url(f"{SITE_URL}/blog/{slug}", pub, "monthly", "0.6"))
    except Exception as e:
        log.debug(f"sitemap-blog Supabase fetch failed: {e}")
    return urls


def _sm_envelope(urls: list[str]) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls) + "\n"
        '</urlset>'
    )


@app.route("/sitemap.xml")
def r_sitemap():
    """
    Single flat sitemap with ALL URLs (~1000) — pages, leagues, teams,
    matches, blog. Sitemap protocol allows up to 50k URLs / 50MB per file,
    we are well below the limit. Flat instead of an index because the CDN
    in front of webpronos.com only routes /sitemap.xml itself to Flask, so
    sub-sitemap URLs would 404. This keeps everything reachable in one shot.
    """
    urls = (
        _sm_urls_pages()
        + _sm_urls_leagues()
        + _sm_urls_teams()
        + _sm_urls_matches()
        + _sm_urls_blog()
    )
    return _xml_response(_sm_envelope(urls), len(urls))


# Sub-sitemap routes kept as direct Flask endpoints in case crawlers fetch
# them or if a future CDN config exposes them. Each one calls the same
# helper used by /sitemap.xml.
@app.route("/sitemap-pages.xml")
def r_sitemap_pages():
    urls = _sm_urls_pages()
    return _xml_response(_sm_envelope(urls), len(urls))


@app.route("/sitemap-teams.xml")
def r_sitemap_teams():
    urls = _sm_urls_teams()
    return _xml_response(_sm_envelope(urls), len(urls))


@app.route("/sitemap-leagues.xml")
def r_sitemap_leagues():
    urls = _sm_urls_leagues()
    return _xml_response(_sm_envelope(urls), len(urls))


@app.route("/sitemap-matches.xml")
def r_sitemap_matches():
    urls = _sm_urls_matches()
    return _xml_response(_sm_envelope(urls), len(urls))


@app.route("/sitemap-blog.xml")
def r_sitemap_blog():
    urls = _sm_urls_blog()
    return _xml_response(_sm_envelope(urls), len(urls))


@app.route("/api/live/picks-stream")
def r_picks_stream():
    """
    Server-Sent Events (SSE) endpoint for real-time pick notifications.
    Clients connect and receive a stream of new picks as they are generated.

    Usage:
      const evtSource = new EventSource('/api/live/picks-stream');
      evtSource.onmessage = (e) => {
        const pick = JSON.parse(e.data);
        console.log('New pick:', pick);
      };

    Returns events with:
      type: "new_pick"
      match_id, flag, tournament, home, away, minute
      market, label, odds, edge, model_p, market_p, timestamp
    """
    def event_stream():
        client_q: Queue = Queue(maxsize=50)
        with _sse_lock:
            _sse_clients.add(client_q)

        try:
            # Send connected message
            yield 'data: {"type":"connected"}\n\n'

            # Stream events until client disconnects
            while True:
                try:
                    msg = client_q.get(timeout=25)  # 25s heartbeat (avoid Cloudflare/proxy timeouts)
                    yield f'data: {msg}\n\n'
                except Empty:
                    # Send heartbeat comment (won't trigger onmessage)
                    yield ': heartbeat\n\n'
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                _sse_clients.discard(client_q)

    return Response(event_stream(),
                    mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache, no-store, must-revalidate',
                        'Pragma': 'no-cache',
                        'Expires': '0',
                        'X-Accel-Buffering': 'no',  # Disable Cloudflare buffering
                    })


@app.route("/api/admin/diag")
def r_admin_diag():
    """Diagnose Sofascore connectivity from the server."""
    import traceback
    out = {"client": _client_type, "tried": []}

    # Try curl_cffi directly
    try:
        from curl_cffi.requests import Session as CffiSession
        s = CffiSession(impersonate="chrome")
        r = s.get(f"{SOFASCORE_API}/sport/football/events/live", timeout=15)
        out["tried"].append({"curl_cffi": r.status_code, "sample": r.text[:200]})
    except ImportError:
        out["tried"].append({"curl_cffi": "NOT_INSTALLED"})
    except Exception as e:
        out["tried"].append({"curl_cffi": f"ERROR: {type(e).__name__}: {e}"})

    # Try cloudscraper directly
    try:
        import cloudscraper
        s = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","desktop":True})
        r = s.get(f"{SOFASCORE_API}/sport/football/events/live", timeout=15)
        out["tried"].append({"cloudscraper": r.status_code, "sample": r.text[:200]})
    except ImportError:
        out["tried"].append({"cloudscraper": "NOT_INSTALLED"})
    except Exception as e:
        out["tried"].append({"cloudscraper": f"ERROR: {type(e).__name__}: {e}"})
    try:
        resp = _session.get(f"{SOFASCORE_API}/sport/football/events/live", timeout=15)
        out["sofascore_live_status"] = resp.status_code
        out["sofascore_live_body_sample"] = resp.text[:300]
        try:
            data = resp.json()
            out["sofascore_live_event_count"] = len(data.get("events", []))
        except Exception:
            out["sofascore_live_event_count"] = "parse_failed"
    except Exception as e:
        out["sofascore_error"] = f"{type(e).__name__}: {e}"
        out["trace"] = traceback.format_exc()[:500]

    try:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        resp2 = _session.get(f"{SOFASCORE_API}/sport/football/scheduled-events/{today_str}", timeout=15)
        out["sofascore_today_status"] = resp2.status_code
        try:
            data2 = resp2.json()
            out["sofascore_today_event_count"] = len(data2.get("events", []))
        except Exception:
            out["sofascore_today_event_count"] = "parse_failed"
    except Exception as e:
        out["sofascore_today_error"] = f"{type(e).__name__}: {e}"

    return jsonify(out)


@app.route("/api/admin/live-debug")
def r_admin_live_debug():
    """Show all live games from Sofascore and filtering results."""
    try:
        live = get_live()
        result = {"total_live": len(live), "games": []}

        for m in live:
            tourn   = m.get("tournament", "")
            country = m.get("country", "")
            home    = m.get("homeTeam", "")
            away    = m.get("awayTeam", "")
            sk      = _resolve_sport_key(tourn, country)
            is_strict = _is_monitored_league_strict(tourn, country) if sk else False
            is_in_keys = sk in MONITORED_SPORT_KEYS if sk else False

            result["games"].append({
                "match": f"{home} vs {away}",
                "tournament": tourn,
                "country": country,
                "sport_key": sk,
                "in_monitored_keys": is_in_keys,
                "passes_strict_check": is_strict,
                "included": is_in_keys and is_strict,
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/resolve", methods=["GET", "POST"])
def r_admin_resolve():
    """
    Force-check all unfinished DB games against Sofascore and resolve pending tips.
    Call this once to fix any tips left pending from games that already finished.
    """
    with _db() as conn:
        pending_ids = [r["id"] for r in conn.execute(
            "SELECT id FROM games WHERE is_finished = 0"
        ).fetchall()]

    fixed_games = []
    for gid in pending_ids:
        try:
            ev = get_event(gid)
            if ev and ev.get("isFinished"):
                hg, ag = ev.get("homeGoals", 0), ev.get("awayGoals", 0)
                now_ts = int(time.time())
                with _db() as conn:
                    conn.execute(
                        "UPDATE games SET is_finished=1, home_goals=?, away_goals=?, "
                        "archived_at=COALESCE(archived_at,?) WHERE id=?",
                        (hg, ag, now_ts, gid)
                    )
                fixed_games.append({"id": gid, "score": f"{hg}-{ag}",
                                    "home": ev["homeTeam"], "away": ev["awayTeam"]})
        except Exception as e:
            log.warning(f"admin/resolve: game {gid} error: {e}")

    _resolve_finished_tips()

    with _db() as conn:
        still_pending_rows = conn.execute("""
            SELECT g.id, g.home_team, g.away_team, g.home_goals, g.away_goals,
                   g.is_finished, t.tip_key, t.market, t.label
            FROM games g JOIN tips t ON t.match_id = g.id
            WHERE t.result IS NULL
        """).fetchall()

    return jsonify({
        "finalized_games": fixed_games,
        "still_pending_tips": len(still_pending_rows),
        "pending_detail": [dict(r) for r in still_pending_rows],
        "ok": True
    })


# ════════════════════════════════════════════════════════════
#  ADMIN PANEL ENDPOINTS — used by Lovable backend sync
# ════════════════════════════════════════════════════════════

@app.route("/api/admin/settings", methods=["GET", "POST"])
def r_admin_settings():
    """
    GET  → returns current settings (merged with defaults)
    POST → upserts settings from admin panel. Body: {key: value, ...}
           Auth: Bearer {ADMIN_SECRET}
    """
    if flask_request.method == "GET":
        merged = dict(_SETTINGS_DEFAULTS)
        merged.update(_load_settings(force=True))
        return jsonify({"ok": True, "settings": merged, "defaults": _SETTINGS_DEFAULTS})

    # POST
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data = flask_request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be JSON object"}), 400

    updated_by = flask_request.headers.get("X-Admin-User", "admin")
    now_ts = int(time.time())
    saved = []
    with _db() as conn:
        for k, v in data.items():
            if k not in _SETTINGS_DEFAULTS:
                continue  # ignore unknown keys
            # blocked_keywords may arrive as list from admin panel → join to string
            if k == "blocked_keywords" and isinstance(v, list):
                v = ",".join(str(x).strip().lower() for x in v if x)
            conn.execute(
                "INSERT INTO settings (key, value, updated_at, updated_by) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at, updated_by=excluded.updated_by",
                (k, str(v), now_ts, updated_by)
            )
            saved.append(k)

    # Force reload of cache
    _load_settings(force=True)
    log.info(f"[admin] settings updated by {updated_by}: {saved}")
    return jsonify({"ok": True, "saved": saved, "settings": _load_settings(force=True)})


@app.route("/api/admin/competitions", methods=["GET", "POST"])
def r_admin_competitions():
    """
    GET  → returns all competitions in DB
    POST → bulk replace. Body: {competitions: [{sport_key, name, country, priority, active_algo, active_frontend}, ...]}
    """
    if flask_request.method == "GET":
        with _db() as conn:
            rows = conn.execute(
                "SELECT sport_key, name, country, priority, active_algo, active_frontend, updated_at "
                "FROM competitions ORDER BY priority ASC, name ASC"
            ).fetchall()
        return jsonify({"ok": True, "competitions": [dict(r) for r in rows]})

    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data = flask_request.get_json(silent=True) or {}
    comps = data.get("competitions", [])
    if not isinstance(comps, list):
        return jsonify({"error": "competitions must be a list"}), 400

    now_ts = int(time.time())
    with _db() as conn:
        # Replace strategy: clear and reinsert
        conn.execute("DELETE FROM competitions")
        for c in comps:
            sk = (c.get("sport_key") or "").strip()
            if not sk:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO competitions "
                "(sport_key, name, country, priority, active_algo, active_frontend, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sk, c.get("name", sk), c.get("country", ""),
                 int(c.get("priority", 99)),
                 1 if c.get("active_algo", True) else 0,
                 1 if c.get("active_frontend", True) else 0,
                 now_ts)
            )
    log.info(f"[admin] competitions replaced: {len(comps)} entries")
    return jsonify({"ok": True, "count": len(comps)})


@app.route("/api/admin/cache/clear", methods=["POST"])
def r_admin_cache_clear():
    """Clear the in-memory odds cache and live state. Body optional: {scope: 'odds'|'state'|'all'}"""
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data = flask_request.get_json(silent=True) or {}
    scope = data.get("scope", "all")
    cleared = []

    try:
        if scope in ("odds", "all"):
            globals().get("_odds_cache", {}).clear()
            cleared.append("odds")
    except Exception as e:
        log.warning(f"clear odds cache error: {e}")

    try:
        if scope in ("state", "all"):
            with _state_lock:
                _live_state.clear()
            cleared.append("state")
    except Exception as e:
        log.warning(f"clear state error: {e}")

    log.info(f"[admin] cache cleared: {cleared}")
    return jsonify({"ok": True, "cleared": cleared})


@app.route("/api/admin/health", methods=["GET"])
def r_admin_health():
    """Quick ping endpoint for the admin panel to verify backend connectivity."""
    return jsonify({
        "ok": True,
        "ts": int(time.time()),
        "settings_loaded": len(_load_settings()),
        "auth_required_for_writes": True,
    })


@app.route("/api/admin/send-daily-summary", methods=["POST"])
def r_admin_send_daily_summary():
    """Send daily summary for a specific day (days_back parameter, default=1 for yesterday)."""
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401

    try:
        # Get days_back from query param (0=today, 1=yesterday, etc.)
        days_back = flask_request.args.get("days_back", 1, type=int)
        force = flask_request.args.get("force", "false").lower() == "true"  # bypass threshold

        _send_daily_summary(days_back=days_back, force_send=force)

        from datetime import datetime, timezone, timedelta
        lisbon_tz = pytz.timezone('Europe/Lisbon')
        now_lisbon = datetime.now(lisbon_tz)
        target_date = (now_lisbon - timedelta(days=days_back)).strftime("%d/%m/%Y")

        return jsonify({
            "ok": True,
            "message": f"Daily summary sent for {target_date} (days_back={days_back})"
        })
    except Exception as e:
        log.error(f"r_admin_send_daily_summary error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/debug-daily-summary", methods=["GET"])
def r_admin_debug_daily_summary():
    """Debug endpoint to see what would be sent for a day."""
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401

    try:
        from datetime import datetime, timezone, timedelta

        # Use Lisbon timezone to determine the target day
        lisbon_tz = pytz.timezone('Europe/Lisbon')
        now_lisbon = datetime.now(lisbon_tz)
        days_back = flask_request.args.get("days_back", 1, type=int)

        target_date = now_lisbon - timedelta(days=days_back)
        target_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=lisbon_tz)
        target_end = target_start + timedelta(days=1)

        # Convert to UTC timestamps
        target_start_ts = int(target_start.timestamp())
        target_end_ts = int(target_end.timestamp())

        STAKE = get_setting("stake_per_bet", 100.0)

        with _db() as conn:
            # Get all settled tips from the target day
            tips = conn.execute(
                "SELECT t.result, t.odd_entry, t.label, t.market, t.match_id, "
                "       g.home_team, g.away_team "
                "FROM tips t "
                "LEFT JOIN games g ON g.id = t.match_id "
                "WHERE t.wall_ts >= ? AND t.wall_ts < ? AND t.result IS NOT NULL "
                "ORDER BY t.odd_entry DESC",
                (target_start_ts, target_end_ts)
            ).fetchall()

            subs_active = conn.execute("SELECT COUNT(*) c FROM tg_subscribers WHERE active = 1").fetchone()["c"]
            subs_list = conn.execute("SELECT chat_id, username, first_name, active FROM tg_subscribers ORDER BY subscribed_at DESC LIMIT 10").fetchall()

        if not tips:
            return jsonify({
                "ok": False,
                "reason": "No settled tips this day",
                "tips_count": 0,
                "subscribers_active": subs_active,
                "subscribers": [{"chat_id": s["chat_id"], "user": s["username"] or s["first_name"], "active": s["active"]} for s in subs_list],
                "day": target_start.strftime("%d/%m/%Y")
            })

        # Calculate stats
        lucro = 0.0
        odds_sum = 0.0
        wins = 0
        losses = 0

        for tip in tips:
            result, odd_entry = tip["result"], tip["odd_entry"]
            if result in ("win", "green") and odd_entry:
                lucro += (odd_entry - 1) * STAKE
                odds_sum += odd_entry
                wins += 1
            elif result in ("loss", "red"):
                lucro -= STAKE
                odds_sum += (odd_entry or 0)
                losses += 1

        settled = wins + losses
        avg_odds = odds_sum / settled if settled > 0 else 0.0
        roi = (lucro / (settled * STAKE) * 100) if settled > 0 else 0.0

        return jsonify({
            "ok": True,
            "day": target_start.strftime("%d/%m/%Y"),
            "tips_count": len(tips),
            "settled": settled,
            "wins": wins,
            "losses": losses,
            "lucro_eur": round(lucro, 2),
            "avg_odds": round(avg_odds, 2),
            "roi_percent": round(roi, 1),
            "will_send": lucro > 25,
            "threshold_eur": 25,
            "subscribers_active": subs_active,
            "subscribers": [{"chat_id": s["chat_id"], "user": s["username"] or s["first_name"], "active": s["active"]} for s in subs_list]
        })
    except Exception as e:
        log.error(f"r_admin_debug_daily_summary error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ════════════════════════════════════════════════════════════
#  SEO PRERENDER — Dynamic meta tags for crawlers & social bots
#  Cloudflare Worker routes bots here; humans go to Lovable SPA
# ════════════════════════════════════════════════════════════

SUPABASE_URL  = os.environ.get("SUPABASE_URL", "https://lcugjwhcmtpdoernjgei.supabase.co")
SUPABASE_ANON = os.environ.get("SUPABASE_ANON_KEY", "")
SITE_URL      = os.environ.get("SITE_URL", "https://webpronos.com")
# Public base URL of THIS Flask backend — used to build links to /api/*
# routes that the Lovable frontend (different origin) consumes. The
# Cloudflare Worker on webpronos.com doesn't proxy /api/* to Flask, so
# we point straight at the fly.dev hostname.
API_BASE_URL  = os.environ.get("API_BASE_URL", "https://livexgmodel-pt.fly.dev")
SITE_NAME     = "WebPronos"

# Cache the base Lovable HTML for 10 min to avoid hammering their CDN
_base_html_cache: dict = {"html": None, "ts": 0}
_BASE_HTML_TTL = 600  # seconds


def _get_base_html() -> str:
    """Fetch and cache the Lovable index.html (the SPA shell)."""
    now = time.time()
    if _base_html_cache["html"] and now - _base_html_cache["ts"] < _BASE_HTML_TTL:
        return _base_html_cache["html"]
    try:
        import urllib.request as _ur
        req = _ur.Request(SITE_URL, headers={"User-Agent": "WebPronosSEO/1.0"})
        with _ur.urlopen(req, timeout=5) as r:
            html = r.read().decode("utf-8")
        _base_html_cache["html"] = html
        _base_html_cache["ts"] = now
        return html
    except Exception as e:
        log.warning(f"[prerender] Could not fetch base HTML: {e}")
        return ""


def _supabase_get_seo_override(match_id: int) -> dict | None:
    """Check Supabase for a manual SEO override for this match. Returns None if not found."""
    if not SUPABASE_ANON:
        return None
    try:
        import urllib.request as _ur, urllib.parse as _up
        url = (
            f"{SUPABASE_URL}/rest/v1/seo_overrides"
            f"?match_id=eq.{match_id}&select=meta_title,meta_description,og_image&limit=1"
        )
        req = _ur.Request(url, headers={
            "apikey":        SUPABASE_ANON,
            "Authorization": f"Bearer {SUPABASE_ANON}",
        })
        with _ur.urlopen(req, timeout=3) as r:
            rows = json.loads(r.read())
            return rows[0] if rows else None
    except Exception as e:
        log.debug(f"[prerender] Supabase seo_overrides lookup failed: {e}")
        return None


def _build_meta_tags(match: dict, odds: dict | None, override: dict | None) -> dict:
    """Generate SEO meta fields for a match page."""
    home   = match.get("homeTeam", "Home")
    away   = match.get("awayTeam", "Away")
    tourn  = match.get("tournament", "")
    status = match.get("statusType", "notstarted")
    h_gls  = match.get("homeGoals", 0) or 0
    a_gls  = match.get("awayGoals", 0) or 0

    # Build title (English — primary language)
    # SEO STABILITY RULE: title/description MUST stay identical from
    # creation (48h pre-kickoff) through the end of the live game.
    # Only `finished` status changes the meta — once the result is set,
    # the title/description rewrite themselves with the final score and
    # never change again. This protects Google's cached snippet.
    if override and override.get("meta_title"):
        title = override["meta_title"]
    elif status == "finished":
        title = f"{home} {h_gls}–{a_gls} {away} – Final Result & xG Analysis | {SITE_NAME}"
    else:
        # Identical title for scheduled AND in-progress (no score, no "LIVE")
        if tourn:
            title = f"{home} vs {away} ({tourn}) – Picks & xG Predictions | {SITE_NAME}"
        else:
            title = f"{home} vs {away} – Picks & xG Predictions | {SITE_NAME}"

    # Build description (English — primary language)
    if override and override.get("meta_description"):
        desc = override["meta_description"]
    elif status == "finished":
        desc = (
            f"Full xG analysis for {home} {h_gls}–{a_gls} {away}. "
            f"Expected Goals, win probabilities and value bets generated by the WebPronos algorithm."
        )
    else:
        # Identical description for scheduled AND in-progress
        desc = (
            f"xG predictions and value bets for {home} vs {away}"
            + (f" – {tourn}" if tourn else "")
            + f". Picks, real-time probabilities and match analysis on {SITE_NAME}."
        )

    # NOTE: live odds are intentionally NOT appended to the description.
    # Odds drift constantly and would change the meta on every crawl,
    # defeating the SEO-stability rule above. Odds belong in the page
    # body, not in <meta>.

    og_image = (override or {}).get("og_image") or "https://webpronos.com/og-default.png"

    return {"title": title, "description": desc, "og_image": og_image}


def _inject_meta(html: str, meta: dict, canonical: str) -> str:
    """Replace/inject meta tags into the Lovable SPA index.html."""
    import re

    title_tag    = f'<title>{meta["title"]}</title>'
    desc_content = meta["description"].replace('"', '&quot;')
    og_image     = meta["og_image"]

    # 1. Replace/strip existing tags in <head>
    html = re.sub(r'<title>[^<]*</title>', title_tag, html)
    html = re.sub(r'<meta\s+name=["\']description["\'][^>]*/?>', '', html)
    html = re.sub(r'<meta\s+(?:property|name)=["\'](?:og:|twitter:)[^"\']*["\'][^>]*/?>', '', html)
    html = re.sub(r'<link\s+rel=["\']canonical["\'][^>]*/?>',  '', html)

    # 2. Build the block of new tags (title already replaced above, so not repeated)
    new_tags = (
        f'<meta name="description" content="{desc_content}">\n'
        f'    <meta property="og:title" content="{meta["title"]}">\n'
        f'    <meta property="og:description" content="{desc_content}">\n'
        f'    <meta property="og:image" content="{og_image}">\n'
        f'    <meta property="og:url" content="{canonical}">\n'
        f'    <meta property="og:type" content="website">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{meta["title"]}">\n'
        f'    <meta name="twitter:description" content="{desc_content}">\n'
        f'    <meta name="twitter:image" content="{og_image}">\n'
        f'    <link rel="canonical" href="{canonical}">'
    )

    # 3. Inject right after <title>...</title>
    html = re.sub(
        r'(<title>[^<]*</title>)',
        r'\1\n    ' + new_tags,
        html,
        count=1,
    )

    return html


def _md_to_html(md: str) -> str:
    """
    Minimal Markdown → HTML converter for blog prerender.
    Handles headings, bold/italic, lists, links, paragraphs.
    No extra dependencies needed.
    """
    import re
    lines = md.replace('\r\n', '\n').split('\n')
    html_lines = []
    in_ul = False
    in_ol = False

    def inline(text):
        # Bold + italic
        text = re.sub(r'\*\*\*(.+?)\*\*\*', r'<strong><em>\1</em></strong>', text)
        text = re.sub(r'\*\*(.+?)\*\*',     r'<strong>\1</strong>', text)
        text = re.sub(r'\*(.+?)\*',         r'<em>\1</em>', text)
        text = re.sub(r'`(.+?)`',           r'<code>\1</code>', text)
        # Links [text](url)
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
        return text

    i = 0
    while i < len(lines):
        line = lines[i]

        # Close open lists if line doesn't match
        if in_ul and not re.match(r'^[\*\-] ', line):
            html_lines.append('</ul>')
            in_ul = False
        if in_ol and not re.match(r'^\d+\. ', line):
            html_lines.append('</ol>')
            in_ol = False

        # ATX headings
        m = re.match(r'^(#{1,6})\s+(.+)', line)
        if m:
            level = len(m.group(1))
            html_lines.append(f'<h{level}>{inline(m.group(2))}</h{level}>')
            i += 1
            continue

        # Horizontal rule
        if re.match(r'^[-*_]{3,}$', line.strip()):
            html_lines.append('<hr>')
            i += 1
            continue

        # Unordered list item
        m = re.match(r'^[\*\-] (.+)', line)
        if m:
            if not in_ul:
                html_lines.append('<ul>')
                in_ul = True
            html_lines.append(f'  <li>{inline(m.group(1))}</li>')
            i += 1
            continue

        # Ordered list item
        m = re.match(r'^\d+\. (.+)', line)
        if m:
            if not in_ol:
                html_lines.append('<ol>')
                in_ol = True
            html_lines.append(f'  <li>{inline(m.group(1))}</li>')
            i += 1
            continue

        # Blockquote
        m = re.match(r'^> (.+)', line)
        if m:
            html_lines.append(f'<blockquote><p>{inline(m.group(1))}</p></blockquote>')
            i += 1
            continue

        # Blank line → paragraph break
        if line.strip() == '':
            html_lines.append('')
            i += 1
            continue

        # Regular paragraph line
        html_lines.append(f'<p>{inline(line)}</p>')
        i += 1

    # Close any open list
    if in_ul:
        html_lines.append('</ul>')
    if in_ol:
        html_lines.append('</ol>')

    return '\n'.join(html_lines)


def _supabase_get_blog_post(slug: str) -> dict | None:
    """Fetch a single blog post from Supabase by slug."""
    if not SUPABASE_ANON:
        return None
    try:
        import urllib.request as _ur, urllib.parse as _up
        url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?slug=eq.{_up.quote(slug)}"
            f"&select=*"
            f"&limit=1"
        )
        req = _ur.Request(url, headers={
            "apikey":        SUPABASE_ANON,
            "Authorization": f"Bearer {SUPABASE_ANON}",
        })
        with _ur.urlopen(req, timeout=5) as r:
            rows = json.loads(r.read())
            return rows[0] if rows else None
    except Exception as e:
        log.warning(f"[prerender/blog] Supabase fetch failed for slug={slug}: {e}")
        return None


def _inject_blog_content(html: str, meta: dict, canonical: str, article_html: str,
                          published_at: str, author: str, jsonld: str) -> str:
    """
    Inject blog meta tags AND full article body into the SPA shell.
    Replaces the <div id="root">...</div> with the rendered article.
    """
    import re

    # ── 1. Inject head meta ──────────────────────────────────────────────────
    desc_content = meta["description"].replace('"', '&quot;')
    og_image     = meta.get("og_image", f"{SITE_URL}/og/default.png")
    title_escaped = meta["title"].replace('<', '&lt;').replace('>', '&gt;')

    # Strip ALL existing dynamic meta (data-rh="true" tags from react-helmet)
    # They come as one long concatenated line — wipe the entire block
    html = re.sub(r'<meta\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<script\s+data-rh=["\']true["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)

    # Replace title tag (may have data-rh attribute)
    html = re.sub(r'<title[^>]*>[^<]*</title>', f'<title>{title_escaped}</title>', html)

    # Strip any remaining og/twitter/canonical tags
    html = re.sub(r'<meta\s+(?:property|name)=["\'](?:og:|twitter:)[^"\']*["\'][^>]*/?>',  '', html)
    html = re.sub(r'<meta\s+name=["\']description["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+rel=["\']canonical["\'][^>]*/?>',  '', html)
    html = re.sub(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)

    new_head = (
        f'<meta name="description" content="{desc_content}">\n'
        f'    <meta property="og:title" content="{title_escaped}">\n'
        f'    <meta property="og:description" content="{desc_content}">\n'
        f'    <meta property="og:image" content="{og_image}">\n'
        f'    <meta property="og:url" content="{canonical}">\n'
        f'    <meta property="og:type" content="article">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{title_escaped}">\n'
        f'    <meta name="twitter:description" content="{desc_content}">\n'
        f'    <meta name="twitter:image" content="{og_image}">\n'
        f'    <link rel="canonical" href="{canonical}">\n'
        f'    <script type="application/ld+json">{jsonld}</script>'
    )
    # Inject right after the (now-clean) title tag
    html = re.sub(
        r'(<title>[^<]*</title>)',
        r'\1\n    ' + new_head,
        html, count=1
    )

    # ── 2. Replace <div id="root">...</div> with full article body ──
    # Keeps the SPA JS loading in background for hydration, but bots read the article
    pub_formatted = published_at[:10] if published_at else ""
    article_body = f"""<div id="root">
<article itemscope itemtype="https://schema.org/BlogPosting"
         style="max-width:800px;margin:0 auto;padding:2rem 1rem;font-family:system-ui,sans-serif;line-height:1.7;color:#e8f0f7">
  <header style="margin-bottom:2rem;padding-bottom:1.5rem;border-bottom:1px solid #2a2f4a">
    <nav style="margin-bottom:1rem;font-size:.85rem">
      <a href="{SITE_URL}" style="color:#10b981;text-decoration:none">WebPronos</a>
      &nbsp;›&nbsp;
      <a href="{SITE_URL}/blog" style="color:#10b981;text-decoration:none">Blog</a>
    </nav>
    <h1 itemprop="headline" style="font-size:2rem;font-weight:800;line-height:1.2;color:#fff;margin:0 0 1rem">{meta["title"]}</h1>
    <p itemprop="description" style="font-size:1.05rem;color:#9ca3af;margin:0 0 1rem">{meta["description"]}</p>
    <div style="font-size:.85rem;color:#6b7280">
      <span itemprop="author" itemscope itemtype="https://schema.org/Person">
        <span itemprop="name">{author}</span>
      </span>
      &nbsp;·&nbsp;
      <time itemprop="datePublished" datetime="{pub_formatted}">{pub_formatted}</time>
    </div>
  </header>
  <div itemprop="articleBody" style="font-size:1rem;color:#cbd5e1">
    {article_html}
  </div>
  <footer style="margin-top:3rem;padding-top:1.5rem;border-top:1px solid #2a2f4a;font-size:.85rem;color:#6b7280">
    <a href="{SITE_URL}/blog" style="color:#10b981;text-decoration:none">← Voltar ao Blog</a>
    &nbsp;&nbsp;|&nbsp;&nbsp;
    <a href="{SITE_URL}" style="color:#10b981;text-decoration:none">WebPronos — Live Football Predictions</a>
  </footer>
</article>
</div>"""

    # Replace the entire #root div — balanced div matching (nested divs)
    start_idx = html.find('<div id="root">')
    if start_idx >= 0:
        i = start_idx + len('<div id="root">')
        depth = 1
        end_idx = -1
        while i < len(html) and depth > 0:
            next_open  = html.find('<div', i)
            next_close = html.find('</div>', i)
            if next_close == -1:
                break
            if next_open != -1 and next_open < next_close:
                depth += 1
                i = next_open + 4
            else:
                depth -= 1
                i = next_close + 6
                if depth == 0:
                    end_idx = i
                    break
        if end_idx > start_idx:
            html = html[:start_idx] + article_body + html[end_idx:]

    return html


@app.route("/prerender/blog/<slug>")
def prerender_blog(slug: str):
    """
    SEO prerender for blog posts.
    Called by the Cloudflare Worker when a bot requests /blog/<slug>.
    Returns the full article HTML — title, meta, JSON-LD BlogPosting + body text.
    Google can read every word of the article without executing JS.
    """
    try:
        post = _supabase_get_blog_post(slug)

        if not post:
            # Return a minimal 404 — don't serve the SPA shell for missing posts
            return (
                f'<!DOCTYPE html><html><head><title>Not Found — WebPronos Blog</title>'
                f'<meta name="robots" content="noindex"></head>'
                f'<body><h1>Article not found</h1>'
                f'<p><a href="{SITE_URL}/blog">Back to Blog</a></p></body></html>',
                404,
                {"Content-Type": "text/html; charset=utf-8"},
            )

        # ── Extract fields ──────────────────────────────────────────────────
        title        = post.get("title")       or post.get("meta_title")      or "WebPronos Blog"
        description  = post.get("description") or post.get("meta_description") or post.get("excerpt") or ""
        content_md   = post.get("content")     or post.get("body")            or ""
        author       = post.get("author")      or "WebPronos"
        published_at = post.get("published_at") or ""
        og_image     = post.get("og_image")    or f"{SITE_URL}/og/default.png"
        canonical    = f"{SITE_URL}/blog/{slug}"

        # ── Markdown → HTML ─────────────────────────────────────────────────
        article_html = _md_to_html(content_md) if content_md else "<p>Article coming soon.</p>"

        # ── JSON-LD BlogPosting ─────────────────────────────────────────────
        jsonld = json.dumps({
            "@context":       "https://schema.org",
            "@type":          "BlogPosting",
            "headline":       title,
            "description":    description,
            "author":         {"@type": "Person", "name": author},
            "publisher":      {"@type": "Organization", "name": "WebPronos", "url": SITE_URL},
            "datePublished":  published_at[:10] if published_at else "",
            "url":            canonical,
            "image":          og_image,
            "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
        }, ensure_ascii=False)

        meta = {"title": title, "description": description, "og_image": og_image}

        # ── Get SPA shell & inject ──────────────────────────────────────────
        base_html = _get_base_html()
        if base_html:
            rendered = _inject_blog_content(base_html, meta, canonical, article_html, published_at, author, jsonld)
            return rendered, 200, {
                "Content-Type":  "text/html; charset=utf-8",
                "Cache-Control": "public, max-age=3600",   # cache 1h — articles don't change often
            }

        # Fallback standalone page (Lovable unreachable)
        fallback = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <meta name="description" content="{description}">
  <meta property="og:title" content="{title}">
  <meta property="og:description" content="{description}">
  <meta property="og:image" content="{og_image}">
  <meta property="og:url" content="{canonical}">
  <meta property="og:type" content="article">
  <link rel="canonical" href="{canonical}">
  <script type="application/ld+json">{jsonld}</script>
</head>
<body style="max-width:800px;margin:0 auto;padding:2rem;font-family:system-ui,sans-serif">
  <nav><a href="{SITE_URL}/blog">← Blog</a></nav>
  <h1>{title}</h1>
  <p><em>{description}</em></p>
  {article_html}
</body>
</html>"""
        return fallback, 200, {"Content-Type": "text/html; charset=utf-8"}

    except Exception as e:
        log.exception(f"[prerender/blog] Error for slug={slug}: {e}")
        return "Internal error", 500


@app.route("/prerender/match/<int:match_id>")
def _event_from_db(match_id: int) -> dict | None:
    """
    Reconstruct an event-shaped dict from the local `games` table.

    Used as a fallback by `prerender_match` when Sofascore is unreachable
    (rate-limited, region-blocked, network error). Output shape matches
    what `_parse_event()` returns so downstream renderers don't care
    whether the data came from Sofascore or the DB.

    Caveats — these are acceptable for SEO bots but not for live UX:
      - `minute` is None (DB has no live clock)
      - `currentPeriodStartTimestamp` is None
      - `statusCode` is a best-guess (100 if finished, 0 if pre-kickoff,
        7 = "second half" if in-progress as a benign default)
      - `tournamentId` is None (not stored in DB)

    Returns None if the match is not in the DB at all (= genuine 404).
    """
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT id, home_team, away_team, home_goals, away_goals, "
                "tournament, country, is_finished, start_ts, "
                "home_team_id, away_team_id "
                "FROM games WHERE id = ?",
                (match_id,)
            ).fetchone()
        if not row:
            return None

        now      = int(time.time())
        is_fin   = bool(row["is_finished"])
        st_ts    = row["start_ts"] or 0

        if is_fin:
            status_type, status_code = "finished", 100
        elif st_ts > now:
            status_type, status_code = "notstarted", 0
        else:
            status_type, status_code = "inprogress", 7

        slug = f"{_slug(row['home_team'])}-vs-{_slug(row['away_team'])}"

        return {
            "id":             row["id"],
            "slug":           slug,
            "homeTeam":       row["home_team"],
            "homeTeamId":     row["home_team_id"],
            "awayTeam":       row["away_team"],
            "awayTeamId":     row["away_team_id"],
            "homeGoals":      row["home_goals"] or 0,
            "awayGoals":      row["away_goals"] or 0,
            "statusCode":     status_code,
            "statusType":     status_type,
            "statusDesc":     "",
            "minute":         None,
            "injuryTime":     0,
            "startTimestamp": st_ts,
            "currentPeriodStartTimestamp": None,
            "tournament":     row["tournament"] or "",
            "tournamentId":   None,
            "country":        row["country"] or "",
            "isLive":         status_type == "inprogress",
            "isFinished":     is_fin,
            "isScheduled":    status_type == "notstarted",
            "_db_fallback":   True,
        }
    except Exception as e:
        log.warning(f"_event_from_db({match_id}) failed: {e}")
        return None


def prerender_match(match_id: int):
    """
    SEO prerender endpoint for match pages.
    Called by the Cloudflare Worker when a bot (Googlebot, Twitterbot, etc.) requests /match/:id.
    Returns fully-rendered HTML with meta tags + body content for the match.

    Strategy:
      1. Try Sofascore live for fresh data (live score, current minute, odds).
      2. If Sofascore is unavailable (region-blocked, network down), fall back
         to the `games` table which has the last-known data — this keeps SEO
         pages indexable even during anti-bot lockouts. Stale data is
         infinitely better than a 404 (which would lead to deindexing).
      3. Only return 404 if the match genuinely doesn't exist in either source.
    """
    try:
        used_fallback = False

        # 1. Try live Sofascore first
        event = get_event(match_id)

        # 2. Fall back to local DB if live fetch failed
        if not event:
            event = _event_from_db(match_id)
            used_fallback = True
            if event:
                log.info(f"[prerender_match {match_id}] using DB fallback (Sofascore unavailable)")

        # 3. Genuine 404 — match not in Sofascore AND not in our DB
        if not event:
            return "Not found", 404

        # 4. Odds: only attempt with fresh event data (live odds require live event context)
        odds = None
        if not used_fallback:
            try:
                odds = get_full_odds_analysis(event, get_shotmap(match_id))
            except Exception:
                odds = None

        # 5. Manual Supabase override (preserves admin-set titles/descriptions)
        override = _supabase_get_seo_override(match_id)

        # 6. Meta + canonical
        meta = _build_meta_tags(event, odds, override)
        slug = event.get("slug", "")
        canonical = f"{SITE_URL}/match/{match_id}/{slug}" if slug else f"{SITE_URL}/match/{match_id}"

        # 7. Body
        body_html = _render_match_body(event, odds, match_id)

        # 8. SportsEvent JSON-LD for rich results
        try:
            from datetime import datetime as _dt, timezone as _tz
            ts = event.get("startTimestamp", 0)
            iso_start = _dt.fromtimestamp(ts, tz=_tz.utc).isoformat() if ts else ""
            jsonld = json.dumps({
                "@context":  "https://schema.org",
                "@type":     "SportsEvent",
                "name":      f"{event.get('homeTeam','')} vs {event.get('awayTeam','')}",
                "startDate": iso_start,
                "sport":     "Soccer",
                "url":       canonical,
                "homeTeam":  {"@type": "SportsTeam", "name": event.get('homeTeam', '')},
                "awayTeam":  {"@type": "SportsTeam", "name": event.get('awayTeam', '')},
                "location":  {"@type": "Place", "name": event.get('tournament', '')},
            }, ensure_ascii=False)
        except Exception:
            jsonld = ""

        # 9. Render
        html = _build_html_page(
            title       = meta["title"],
            description = meta["description"],
            canonical   = canonical,
            body_html   = body_html,
            jsonld      = jsonld,
            og_image    = meta.get("og_image"),
        )
        # Shorter cache on fallback so we retry live data sooner once Sofascore
        # comes back. Live path keeps the original 2-min cache.
        cache_max_age = 600 if used_fallback else 120
        return html, 200, {
            "Content-Type":  "text/html; charset=utf-8",
            "Cache-Control": f"public, max-age={cache_max_age}",
            "X-Source":      "db-fallback" if used_fallback else "live",
        }

    except Exception as e:
        log.exception(f"[prerender] Error for match {match_id}: {e}")
        return "Internal error", 500


# ════════════════════════════════════════════════════════════
#  UNIFIED PRERENDER — single entry point for all bot traffic
#  /prerender?path=/blog → routes to the right renderer
# ════════════════════════════════════════════════════════════

# Shared inline styles for all prerendered pages — dark theme matching the SPA
_PRERENDER_CSS = """
<style>
  body{margin:0;background:#0a0e27;color:#e8f0f7;font-family:system-ui,-apple-system,sans-serif;line-height:1.6}
  .pr-wrap{max-width:1100px;margin:0 auto;padding:1.5rem 1rem}
  .pr-nav{margin-bottom:1.5rem;font-size:.85rem;color:#9ca3af}
  .pr-nav a{color:#10b981;text-decoration:none;margin-right:.5rem}
  .pr-nav a:hover{text-decoration:underline}
  .pr-h1{font-size:2rem;font-weight:800;margin:0 0 .5rem;color:#fff;line-height:1.2}
  .pr-lead{font-size:1.05rem;color:#9ca3af;margin:0 0 2rem}
  .pr-h2{font-size:1.4rem;font-weight:700;color:#fff;margin:2rem 0 1rem}
  .pr-h3{font-size:1.1rem;font-weight:600;color:#fff;margin:1.5rem 0 .5rem}
  .pr-card{background:#1a1f3a;border:1px solid #2a2f4a;border-radius:8px;padding:1rem;margin-bottom:.75rem}
  .pr-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:1rem}
  .pr-stat{display:inline-block;background:#1a1f3a;border:1px solid #2a2f4a;border-radius:6px;padding:.4rem .8rem;margin:0 .4rem .4rem 0;font-size:.85rem}
  .pr-stat strong{color:#10b981;font-weight:700}
  .pr-row{display:flex;justify-content:space-between;align-items:center;padding:.6rem 0;border-bottom:1px solid #2a2f4a}
  .pr-row:last-child{border:0}
  .pr-row a{color:#fff;text-decoration:none;font-weight:600;flex:1}
  .pr-row a:hover{color:#10b981}
  .pr-meta{font-size:.8rem;color:#9ca3af}
  .pr-win{color:#10b981;font-weight:700}
  .pr-lose{color:#ef4444;font-weight:700}
  .pr-pending{color:#fbbf24;font-weight:700}
  .pr-table{width:100%;border-collapse:collapse;margin:1rem 0}
  .pr-table th,.pr-table td{text-align:left;padding:.5rem;border-bottom:1px solid #2a2f4a;font-size:.9rem}
  .pr-table th{color:#9ca3af;font-weight:600;font-size:.75rem;text-transform:uppercase}
  .pr-footer{margin-top:3rem;padding-top:1.5rem;border-top:1px solid #2a2f4a;font-size:.85rem;color:#9ca3af;text-align:center}
  .pr-footer a{color:#10b981;text-decoration:none;margin:0 .5rem}
  .pr-articles a{color:#fff;text-decoration:none}
  .pr-articles h3{margin:0 0 .5rem;font-size:1.05rem}
  .pr-articles p{margin:0;font-size:.9rem;color:#9ca3af}
</style>
"""


def _build_html_page(title: str, description: str, canonical: str,
                      body_html: str, jsonld: str = "",
                      og_image: str | None = None) -> str:
    """
    Build a complete HTML page for prerender.
    Tries to inject into the Lovable SPA shell so visual hydration still works
    for users who somehow hit this endpoint. Falls back to standalone if needed.
    """
    import re
    og_image = og_image or f"{SITE_URL}/og/default.png"
    title_escaped = title.replace('<', '&lt;').replace('>', '&gt;')
    desc_escaped  = description.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')

    base_html = _get_base_html()
    if not base_html:
        # Standalone fallback
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="robots" content="index,follow">
<title>{title_escaped}</title>
<meta name="description" content="{desc_escaped}">
<meta property="og:type" content="website">
<meta property="og:title" content="{title_escaped}">
<meta property="og:description" content="{desc_escaped}">
<meta property="og:image" content="{og_image}">
<meta property="og:url" content="{canonical}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{title_escaped}">
<meta name="twitter:description" content="{desc_escaped}">
<meta name="twitter:image" content="{og_image}">
<link rel="canonical" href="{canonical}">
{('<script type="application/ld+json">' + jsonld + '</script>') if jsonld else ''}
{_PRERENDER_CSS}
</head>
<body>
<div class="pr-wrap">{body_html}</div>
</body>
</html>"""

    # Inject into Lovable SPA shell
    html = base_html

    # Strip react-helmet dynamic tags
    html = re.sub(r'<meta\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<script\s+data-rh=["\']true["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)
    # Replace title (may have data-rh)
    html = re.sub(r'<title[^>]*>[^<]*</title>', f'<title>{title_escaped}</title>', html)
    # Strip existing og/twitter/canonical/jsonld
    html = re.sub(r'<meta\s+(?:property|name)=["\'](?:og:|twitter:)[^"\']*["\'][^>]*/?>', '', html)
    html = re.sub(r'<meta\s+name=["\']description["\'][^>]*/?>', '', html)
    html = re.sub(r'<link\s+rel=["\']canonical["\'][^>]*/?>',  '', html)
    html = re.sub(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)

    new_head = (
        f'<meta name="description" content="{desc_escaped}">\n'
        f'    <meta property="og:title" content="{title_escaped}">\n'
        f'    <meta property="og:description" content="{desc_escaped}">\n'
        f'    <meta property="og:image" content="{og_image}">\n'
        f'    <meta property="og:url" content="{canonical}">\n'
        f'    <meta property="og:type" content="website">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{title_escaped}">\n'
        f'    <meta name="twitter:description" content="{desc_escaped}">\n'
        f'    <meta name="twitter:image" content="{og_image}">\n'
        f'    <link rel="canonical" href="{canonical}">\n'
        + (f'    <script type="application/ld+json">{jsonld}</script>\n' if jsonld else '')
        + f'    {_PRERENDER_CSS}'
    )
    html = re.sub(r'(<title>[^<]*</title>)', r'\1\n    ' + new_head, html, count=1)

    # Strip the aria-hidden cloaking div from Lovable shell (SEO risk + redundant now)
    html = re.sub(
        r'<div\s+aria-hidden=["\']true["\'][^>]*>.*?</div>\s*</body>',
        '</body>',
        html, flags=re.DOTALL
    )

    # Replace #root with the body — need balanced div matching since #root has nested divs
    new_root = f'<div id="root"><div class="pr-wrap">{body_html}</div></div>'
    start_idx = html.find('<div id="root">')
    if start_idx >= 0:
        # Walk forward counting <div ... and </div> to find the matching close tag
        i = start_idx + len('<div id="root">')
        depth = 1
        end_idx = -1
        while i < len(html) and depth > 0:
            next_open  = html.find('<div', i)
            next_close = html.find('</div>', i)
            if next_close == -1:
                break
            if next_open != -1 and next_open < next_close:
                depth += 1
                i = next_open + 4
            else:
                depth -= 1
                i = next_close + 6
                if depth == 0:
                    end_idx = i
                    break
        if end_idx > start_idx:
            html = html[:start_idx] + new_root + html[end_idx:]

    return html


def _render_pr_footer() -> str:
    return f"""
    <div class="pr-footer">
      <a href="{SITE_URL}/">Home</a> ·
      <a href="{SITE_URL}/blog">Blog</a> ·
      <a href="{SITE_URL}/history">History</a> ·
      <a href="{SITE_URL}/tomorrow">Tomorrow</a> ·
      <a href="{SITE_URL}/about">About</a>
      <p style="margin-top:1rem;font-size:.75rem">WebPronos provides statistical predictions for informational purposes only. 18+ — please gamble responsibly.</p>
    </div>
    """


# ── Blog listing ──────────────────────────────────────────────────────────
def _supabase_get_all_blog_posts(limit: int = 50) -> list:
    """Fetch all published blog posts (lightweight: title, slug, excerpt, date)."""
    if not SUPABASE_ANON:
        return []
    try:
        import urllib.request as _ur
        url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?select=*"
            f"&order=published_at.desc"
            f"&limit={limit}"
        )
        req = _ur.Request(url, headers={
            "apikey":        SUPABASE_ANON,
            "Authorization": f"Bearer {SUPABASE_ANON}",
        })
        with _ur.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning(f"[prerender/blog-list] Supabase fetch failed: {e}")
        return []


def _render_blog_listing() -> str:
    """SSR for /blog — list of all articles."""
    posts = _supabase_get_all_blog_posts(limit=50)

    if not posts:
        articles_html = '<p>No articles published yet. Check back soon.</p>'
    else:
        items = []
        for p in posts:
            slug   = p.get("slug", "")
            title  = p.get("title", "Untitled")
            excerpt = p.get("description") or p.get("excerpt") or ""
            pub    = (p.get("published_at") or "")[:10]
            author = p.get("author", "WebPronos")
            items.append(f"""
            <article class="pr-card pr-articles" itemscope itemtype="https://schema.org/BlogPosting">
              <a href="{SITE_URL}/blog/{slug}" itemprop="url">
                <h3 itemprop="headline">{title}</h3>
                <p itemprop="description">{excerpt}</p>
                <p class="pr-meta" style="margin-top:.5rem">
                  <span itemprop="author">{author}</span> · <time itemprop="datePublished" datetime="{pub}">{pub}</time>
                </p>
              </a>
            </article>""")
        articles_html = "\n".join(items)

    body = f"""
    <nav class="pr-nav">
      <a href="{SITE_URL}/">WebPronos</a> › Blog
    </nav>
    <h1 class="pr-h1">WebPronos Blog</h1>
    <p class="pr-lead">In-depth guides on xG, live betting strategy, value detection and how AI improves football predictions. Updated regularly.</p>
    <div class="pr-grid">
      {articles_html}
    </div>
    {_render_pr_footer()}
    """

    # JSON-LD: ItemList of articles
    items_jsonld = []
    for i, p in enumerate(posts[:20]):
        items_jsonld.append({
            "@type": "ListItem",
            "position": i + 1,
            "url": f"{SITE_URL}/blog/{p.get('slug','')}",
            "name": p.get("title", ""),
        })
    jsonld = json.dumps({
        "@context": "https://schema.org",
        "@type":    "Blog",
        "name":     "WebPronos Blog",
        "url":      f"{SITE_URL}/blog",
        "description": "Guides on xG, live betting strategy and AI football predictions.",
        "blogPost": items_jsonld,
    }, ensure_ascii=False)

    return _build_html_page(
        title       = "Blog — Live Betting Strategy, xG & AI Predictions | WebPronos",
        description = "Free in-depth guides on xG, live betting timing, value detection and edge calculation. Learn how the WebPronos AI model finds positive-EV bets.",
        canonical   = f"{SITE_URL}/blog",
        body_html   = body,
        jsonld      = jsonld,
    )


# ── History ───────────────────────────────────────────────────────────────
def _render_history() -> str:
    """SSR for /history — last settled picks with results."""
    try:
        STAKE = get_setting("stake_per_bet", 100.0)
        with _db() as conn:
            rows = conn.execute("""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.country, g.tournament
                FROM tips t
                LEFT JOIN games g ON g.id = t.match_id
                WHERE t.result IS NOT NULL
                ORDER BY t.wall_ts DESC
                LIMIT 50
            """).fetchall()

            stats_row = conn.execute("""
                SELECT COUNT(*) total,
                       SUM(CASE WHEN result IN ('win','green') THEN 1 ELSE 0 END) wins,
                       SUM(CASE WHEN result IN ('loss','red') THEN 1 ELSE 0 END) losses
                FROM tips WHERE result IS NOT NULL
            """).fetchone()

        # Compute aggregate stats
        total = stats_row["total"] or 0
        wins  = stats_row["wins"] or 0
        losses = stats_row["losses"] or 0
        pnl   = 0.0
        for r in rows:
            if r["result"] in ("win","green") and r["odd_entry"]:
                pnl += (r["odd_entry"] - 1) * STAKE
            elif r["result"] in ("loss","red"):
                pnl -= STAKE

        winrate = (wins / total * 100) if total else 0
        roi     = (pnl  / (total * STAKE) * 100) if total else 0

        # Build table
        table_rows = []
        for r in rows[:30]:
            from datetime import datetime, timezone
            date_str = datetime.fromtimestamp(r["wall_ts"], tz=timezone.utc).strftime("%b %d")
            won = r["result"] in ("win","green")
            badge = '<span class="pr-win">✓ Won</span>' if won else '<span class="pr-lose">✗ Lost</span>'
            score = f"{r['home_goals']}-{r['away_goals']}" if r["home_goals"] is not None else "—"
            match = f"{r['home_team']} vs {r['away_team']}" if r["home_team"] else "—"
            league = r["tournament"] or r["country"] or "—"
            profit = ((r["odd_entry"] - 1) * STAKE) if won and r["odd_entry"] else (-STAKE if not won else 0)
            profit_class = "pr-win" if profit > 0 else "pr-lose"

            table_rows.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td class="pr-meta">{league}</td>
                <td><a href="{_match_url(r['match_id'], r['home_team'], r['away_team'])}" style="color:#fff">{match}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{r['market']} — {r['label']}</td>
                <td class="pr-meta">@{r['odd_entry']:.2f}</td>
                <td>{badge}</td>
                <td class="{profit_class}">{'+' if profit > 0 else ''}{profit:.0f}€</td>
              </tr>
            """)

        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}/">WebPronos</a> › History
        </nav>
        <h1 class="pr-h1">Historical Performance — Track Record</h1>
        <p class="pr-lead">Every settled prediction by the WebPronos AI model is published openly. Audit the full track record below — no cherry-picking.</p>

        <div style="margin:1.5rem 0">
          <span class="pr-stat">Total picks: <strong>{total}</strong></span>
          <span class="pr-stat">Wins / Losses: <strong>{wins} / {losses}</strong></span>
          <span class="pr-stat">Win rate: <strong>{winrate:.1f}%</strong></span>
          <span class="pr-stat">P&amp;L (€{STAKE:.0f}/pick): <strong>{'+' if pnl > 0 else ''}{pnl:.0f}€</strong></span>
          <span class="pr-stat">ROI: <strong>{'+' if roi > 0 else ''}{roi:.1f}%</strong></span>
        </div>

        <h2 class="pr-h2">Last 30 settled picks</h2>
        <table class="pr-table">
          <thead>
            <tr>
              <th>Date</th><th>League</th><th>Match</th><th>Score</th>
              <th>Pick</th><th>Odds</th><th>Result</th><th>P&amp;L</th>
            </tr>
          </thead>
          <tbody>
            {''.join(table_rows) if table_rows else '<tr><td colspan="8" style="text-align:center;padding:2rem">No settled picks yet.</td></tr>'}
          </tbody>
        </table>

        <h2 class="pr-h2">How we measure performance</h2>
        <p>Every pick is logged the moment our live model identifies a positive-EV bet. The recorded entry odds are the live bookmaker price at that exact second — never inflated post-result. P&amp;L assumes a flat €{STAKE:.0f} stake on every recommendation. ROI is calculated as total profit divided by total staked, expressed as a percentage.</p>

        <h2 class="pr-h2">Why a public track record matters</h2>
        <p>Most tipsters cherry-pick wins and hide losses. By publishing every single settled pick — including the bad ones — we let anyone audit our edge. If the long-term ROI stays positive, the model is genuinely beating the market. If it drops, we owe you transparency about why.</p>

        {_render_pr_footer()}
        """

        jsonld = json.dumps({
            "@context": "https://schema.org",
            "@type":    "Dataset",
            "name":     "WebPronos prediction track record",
            "description": f"Public history of {total} AI-generated football predictions with results and P&L.",
            "url":      f"{SITE_URL}/history",
            "creator":  {"@type": "Organization", "name": "WebPronos", "url": SITE_URL},
            # Required by Google's Dataset structured data spec — flagged in
            # Search Console as "Missing field 'license'". Points to the site
            # terms which describe permitted reuse of the prediction history.
            "license":  f"{SITE_URL}/terms",
            "isAccessibleForFree": True,
            "keywords": ["football predictions", "AI tips", "track record", "betting analytics", "xG model"],
        }, ensure_ascii=False)

        return _build_html_page(
            title       = f"History — Track Record of {total} AI Football Predictions | WebPronos",
            description = f"Public audit log of every prediction by the WebPronos AI model. {wins} wins, {losses} losses, {roi:+.1f}% ROI across {total} settled picks. No cherry-picking.",
            canonical   = f"{SITE_URL}/history",
            body_html   = body,
            jsonld      = jsonld,
        )
    except Exception as e:
        log.exception(f"[prerender/history] Error: {e}")
        return _build_html_page(
            title="History | WebPronos",
            description="Public track record of AI football predictions.",
            canonical=f"{SITE_URL}/history",
            body_html=f'<h1>History</h1><p>Loading… {_render_pr_footer()}</p>',
        )


# ── Tomorrow's matches ────────────────────────────────────────────────────
def _render_tomorrow() -> str:
    """SSR for /tomorrow — list of matches scheduled for tomorrow."""
    try:
        from datetime import datetime, timezone, timedelta
        # Reuse the upcoming endpoint logic — fetch tomorrow specifically
        import urllib.request as _ur
        try:
            req = _ur.Request("http://127.0.0.1:8080/api/upcoming?days=2")
            with _ur.urlopen(req, timeout=5) as r:
                data = json.loads(r.read())
        except Exception:
            # Fallback to public URL
            req = _ur.Request("https://livexgmodel-pt.fly.dev/api/upcoming?days=2")
            with _ur.urlopen(req, timeout=8) as r:
                data = json.loads(r.read())

        # Get tomorrow's matches
        tomorrow_day = next((d for d in data.get("days", []) if d.get("label", "").lower() == "tomorrow"), None)
        if not tomorrow_day:
            tomorrow_day = data["days"][1] if len(data.get("days", [])) > 1 else {"matches": [], "date": ""}

        matches = tomorrow_day.get("matches", [])
        date_str = tomorrow_day.get("date", "")

        # Group by tournament
        groups: dict = {}
        for m in matches:
            league = m.get("tournament") or m.get("country") or "Other"
            groups.setdefault(league, []).append(m)

        # Render groups
        groups_html = []
        for league, ms in sorted(groups.items()):
            rows = []
            for m in sorted(ms, key=lambda x: x.get("startTimestamp", 0)):
                ts = m.get("startTimestamp", 0)
                kickoff = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M") if ts else "—"
                rows.append(f"""
                  <div class="pr-row">
                    <a href="{_match_url(m['id'], m['homeTeam'], m['awayTeam'])}">{m['homeTeam']} <span class="pr-meta">vs</span> {m['awayTeam']}</a>
                    <span class="pr-meta">{kickoff} UTC</span>
                  </div>""")
            groups_html.append(f"""
            <div class="pr-card">
              <h3 class="pr-h3" style="margin-top:0">{league}</h3>
              {''.join(rows)}
            </div>""")

        # Format human date
        try:
            d_obj = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.now(timezone.utc) + timedelta(days=1)
            date_human = d_obj.strftime("%A, %B %-d")
        except Exception:
            date_human = "Tomorrow"

        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}/">WebPronos</a> › Tomorrow
        </nav>
        <h1 class="pr-h1">Tomorrow's Football Matches — {date_human}</h1>
        <p class="pr-lead">Every match scheduled for tomorrow across the {len(groups)} competitions we cover. Click any fixture to open its dedicated live page — once kickoff happens, the AI model starts publishing in-play tips, value bets and updated odds.</p>

        <div class="pr-grid">
          {''.join(groups_html) if groups_html else '<p>No matches scheduled tomorrow.</p>'}
        </div>

        <h2 class="pr-h2">How tomorrow's preview works</h2>
        <p>This is a preview of fixtures only — pre-match tips are deliberately not published. WebPronos only generates live tips, after kickoff, when the AI model can react to actual lineups, red cards, momentum swings and tactical decisions. Bookmark a fixture you care about and check back during the match.</p>

        <h2 class="pr-h2">Why we don't bet pre-match</h2>
        <p>Pre-match models guess. They don't know who is on the pitch, who got injured warming up, or which referee is calling cards generously today. Our model waits — when a match is live, every shot, every card and every substitution updates the win probabilities in real time. That's where the edge lives.</p>

        {_render_pr_footer()}
        """

        # JSON-LD: SportsEvent list
        events_jsonld = []
        for m in matches[:25]:
            ts = m.get("startTimestamp", 0)
            iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else ""
            events_jsonld.append({
                "@type":     "SportsEvent",
                "name":      f"{m['homeTeam']} vs {m['awayTeam']}",
                "startDate": iso,
                "sport":     "Soccer",
                "url":       f"{SITE_URL}/match/{m['id']}",
                "homeTeam":  {"@type": "SportsTeam", "name": m['homeTeam']},
                "awayTeam":  {"@type": "SportsTeam", "name": m['awayTeam']},
            })
        jsonld = json.dumps({
            "@context": "https://schema.org",
            "@type":    "ItemList",
            "name":     f"Tomorrow's football matches — {date_human}",
            "numberOfItems": len(matches),
            "itemListElement": [
                {"@type": "ListItem", "position": i+1, "item": e}
                for i, e in enumerate(events_jsonld)
            ],
        }, ensure_ascii=False)

        return _build_html_page(
            title       = f"Tomorrow's Football Matches — {date_human} | WebPronos",
            description = f"Full preview of {len(matches)} football matches scheduled for {date_human}. Live AI tips will be published once kickoff happens.",
            canonical   = f"{SITE_URL}/tomorrow",
            body_html   = body,
            jsonld      = jsonld,
        )
    except Exception as e:
        log.exception(f"[prerender/tomorrow] Error: {e}")
        return _build_html_page(
            title="Tomorrow's Matches | WebPronos",
            description="Preview of football matches scheduled for tomorrow.",
            canonical=f"{SITE_URL}/tomorrow",
            body_html=f'<h1>Tomorrow\'s matches</h1><p>Loading…</p>{_render_pr_footer()}',
        )


# ── Enhanced match prerender (with body content) ───────────────────────────
def _render_match_body(event: dict, odds: dict | None, match_id: int) -> str:
    """Build the visible body content for a match prerender."""
    home   = event.get("homeTeam", "Home")
    away   = event.get("awayTeam", "Away")
    tourn  = event.get("tournament", "")
    country = event.get("country", "")
    status = event.get("statusType", "notstarted")
    h_gls  = event.get("homeGoals", 0) or 0
    a_gls  = event.get("awayGoals", 0) or 0
    minute = event.get("minute", 0) or 0

    # Status pill
    if status == "inprogress":
        status_pill = f'<span class="pr-stat">🔴 LIVE — {minute}\'</span>'
        score_html = f'<div style="font-size:2rem;font-weight:800;margin:1rem 0;color:#fff">{home} {h_gls} — {a_gls} {away}</div>'
    elif status == "finished":
        status_pill = f'<span class="pr-stat">✓ Finished</span>'
        score_html = f'<div style="font-size:2rem;font-weight:800;margin:1rem 0;color:#fff">{home} {h_gls} — {a_gls} {away}</div>'
    else:
        status_pill = f'<span class="pr-stat">⏰ Scheduled</span>'
        score_html = f'<div style="font-size:2rem;font-weight:800;margin:1rem 0;color:#fff">{home} vs {away}</div>'

    # Odds section (if available)
    odds_html = ""
    if odds:
        h2h = odds.get("h2h") if isinstance(odds, dict) else None
        if h2h and isinstance(h2h, dict):
            home_odd = h2h.get("home_odd") or h2h.get("1") or "—"
            draw_odd = h2h.get("draw_odd") or h2h.get("X") or "—"
            away_odd = h2h.get("away_odd") or h2h.get("2") or "—"
            odds_html = f"""
            <h2 class="pr-h2">Live Odds Comparison</h2>
            <div class="pr-card">
              <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:.5rem;text-align:center">
                <div><div class="pr-meta">Home Win</div><div style="font-size:1.4rem;font-weight:700">{home_odd}</div></div>
                <div><div class="pr-meta">Draw</div><div style="font-size:1.4rem;font-weight:700">{draw_odd}</div></div>
                <div><div class="pr-meta">Away Win</div><div style="font-size:1.4rem;font-weight:700">{away_odd}</div></div>
              </div>
            </div>
            """

    body = f"""
    <nav class="pr-nav">
      <a href="{SITE_URL}/">WebPronos</a> ›
      <a href="{SITE_URL}/history">Matches</a> › {home} vs {away}
    </nav>

    <p class="pr-meta">{country} · {tourn}</p>
    <h1 class="pr-h1">{home} vs {away} — Live Football Predictions</h1>
    {status_pill}
    {score_html}

    <p class="pr-lead">Real-time AI predictions for {home} vs {away}, recalculated every 15 seconds based on shots, expected goals (xG), possession, cards and momentum. Compare live odds across top bookmakers and spot value bets the moment they appear.</p>

    {odds_html}

    <h2 class="pr-h2">What you'll find on this page</h2>
    <ul>
      <li><strong>Live momentum bar</strong> — visual indicator of who is dominating the match right now.</li>
      <li><strong>Prediction pill</strong> — the AI model's current best pick for the next goal / final result.</li>
      <li><strong>Value tip badge</strong> — highlighted when bookmaker odds are higher than our fair price (positive EV).</li>
      <li><strong>Full odds comparison</strong> — best live price across licensed operators.</li>
      <li><strong>Shot-by-shot xG breakdown</strong> — every chance plotted with xG value.</li>
    </ul>

    <h2 class="pr-h2">About this match</h2>
    <p>{home} take on {away} in the {tourn}. WebPronos publishes live in-play tips for this fixture — every prediction is generated after kickoff, when the model can react to the actual flow of the game. Open the live page during kickoff to see real-time win probabilities, value bets and the full xG shot map.</p>

    {_render_pr_footer()}
    """

    return body


# ── SEO renderers: team / league / tips-market / today ────────────────────

# Common pretty-name lookup for tip markets
# market slug → (display title, list of SQL LIKE patterns matched against tips.market)
# Only markets we actually bet on. Goal totals collapsed into one bucket
# because comparing by line (1.5, 2.5, 3.5) doesn't tell a coherent story.
_TIP_MARKET_LABELS = {
    "total-goals": ("Total Goals",        ["O/U%", "Over/Under%", "Goals%", "Total%"]),
    "handicap":    ("Asian Handicap",     ["Handicap%", "AH%"]),
    "1x2":         ("Match Winner (1X2)", ["1X2", "Match Winner%", "Match Result%"]),
}


def _render_team(slug: str) -> tuple:
    """SSR for /team/{slug} — recent picks + stats for one team."""
    cache_key = f"team:{slug}"
    cached = _seo_cache_get(cache_key)
    if cached:
        return cached, 200, {"Content-Type": "text/html; charset=utf-8",
                              "Cache-Control": "public, max-age=600",
                              "X-Prerender": "webpronos-team"}

    name = _team_by_slug(slug)
    if not name:
        # Trigger lazy refresh in case the bg loop hasn't run yet
        _refresh_slug_index()
        name = _team_by_slug(slug)
    if not name:
        return _render_passthrough(f"/team/{slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}

    STAKE = get_setting("stake_per_bet", 100.0)
    try:
        with _db() as conn:
            picks = conn.execute("""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.tournament, g.country, g.start_ts, g.is_finished
                FROM tips t
                JOIN games g ON g.id = t.match_id
                WHERE g.home_team = ? OR g.away_team = ?
                ORDER BY t.wall_ts DESC
                LIMIT 30
            """, (name, name)).fetchall()

        # Next fixture from in-memory cache (today + next 2 days)
        next_fixture = _next_fixture_for_team(name)
        # Recent on-pitch performance (form, xG, streaks) — drives the SEO copy
        perf = _team_performance(name, recent_n=5)

        # Stats
        settled = [p for p in picks if p["result"] is not None]
        wins   = sum(1 for p in settled if p["result"] in ("win", "green"))
        losses = sum(1 for p in settled if p["result"] in ("loss", "red"))
        voids  = sum(1 for p in settled if p["result"] == "void")
        total  = len(settled)
        winrate = (wins / total * 100) if total else 0
        pnl = 0.0
        odd_sum = 0.0
        odd_n = 0
        for p in settled:
            if p["odd_entry"]:
                odd_sum += p["odd_entry"]; odd_n += 1
            if p["result"] in ("win", "green") and p["odd_entry"]:
                pnl += (p["odd_entry"] - 1) * STAKE
            elif p["result"] in ("loss", "red"):
                pnl -= STAKE
        avg_odd = (odd_sum / odd_n) if odd_n else 0.0

        # Logo
        logo_url = _quick_logo(name) or ""
        logo_img = (f'<img src="{logo_url}" alt="{name} logo" '
                    f'style="width:64px;height:64px;vertical-align:middle;margin-right:.75rem">'
                    if logo_url else "")

        # Picks table
        rows_html = []
        for p in picks[:20]:
            from datetime import datetime as _dt, timezone as _tz
            date_str = _dt.fromtimestamp(p["wall_ts"], tz=_tz.utc).strftime("%b %d")
            opp_is_home = p["away_team"] == name
            opponent = p["home_team"] if opp_is_home else p["away_team"]
            score = (f'{p["home_goals"]}-{p["away_goals"]}'
                     if p["home_goals"] is not None and p["is_finished"] else "—")
            if p["result"] in ("win", "green"):
                badge = '<span class="pr-win">✓ Won</span>'
            elif p["result"] in ("loss", "red"):
                badge = '<span class="pr-lose">✗ Lost</span>'
            else:
                badge = '<span class="pr-meta">Pending</span>'
            rows_html.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td><a href="{_match_url(p['match_id'], p['home_team'], p['away_team'])}" style="color:#fff">{p['home_team']} vs {p['away_team']}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{p['market']} — {p['label']}</td>
                <td class="pr-meta">@{(p['odd_entry'] or 0):.2f}</td>
                <td>{badge}</td>
              </tr>""")

        # Next fixture block
        if next_fixture:
            from datetime import datetime as _dt, timezone as _tz
            kickoff = _dt.fromtimestamp(next_fixture["kickoff_ts"], tz=_tz.utc).strftime("%b %d, %H:%M UTC")
            next_html = f"""
            <h2 class="pr-h2">Next fixture</h2>
            <p><a href="{_match_url(next_fixture['match_id'], next_fixture['home_team'], next_fixture['away_team'])}" style="color:#22d3ee">
              {next_fixture['home_team']} vs {next_fixture['away_team']}
            </a> — {next_fixture['tournament'] or ''} · {kickoff}</p>"""
        else:
            next_html = ""

        # ── Build rich SEO intro paragraph from real DB data ────────────────
        # We assemble multiple sentences referring to actual observed metrics
        # (no made-up "season" stats — only what we tracked). Each sentence is
        # only included when the underlying data is present.
        intro_bits = []
        n_obs = perf.get("sample_size") or 0
        xg = perf.get("xg_summary")
        streak = perf.get("streak") or {}
        form_letters = perf.get("form_letters") or ""

        if n_obs == 0:
            intro_bits.append(
                f"This page tracks every AI-generated football pick "
                f"that involves {name}, alongside live xG analysis when the team plays."
            )
        else:
            if xg:
                intro_bits.append(
                    f"Across the last {xg['sample_size']} tracked match"
                    + ("es" if xg['sample_size'] != 1 else "")
                    + f", {name} have averaged {xg['avg_for']} xG created and "
                    f"{xg['avg_against']} xG conceded per game, scoring "
                    f"{xg['avg_goals_for']:.1f} and conceding {xg['avg_goals_against']:.1f} on average."
                )
                if xg.get("overperforming") is True:
                    intro_bits.append(
                        f"Their goal output has been outpacing the underlying chance quality — "
                        f"a sign the finishing has been sharp, but expected to regress over time."
                    )
                elif xg.get("overperforming") is False:
                    intro_bits.append(
                        f"They've been generating more chances than the scoreboard reflects — "
                        f"underperforming xG suggests positive regression is likely."
                    )
            if streak.get("text"):
                intro_bits.append(f"{name} arrive {streak['text']}, with a recent form line of {form_letters}.")
            else:
                intro_bits.append(f"Recent form line: {form_letters}.")
            intro_bits.append(
                f"Below you'll find every AI pick our model has issued on {name} — "
                "with entry odds, live results, and the running profit/loss audit."
            )

        intro_html = " ".join(intro_bits)

        # ── Recent form table (compact, with xG when we have it)
        if perf["recent_games"]:
            form_rows = []
            for g in perf["recent_games"]:
                badge = ('<span class="pr-win">W</span>' if g["result"] == "W"
                         else '<span class="pr-lose">L</span>' if g["result"] == "L"
                         else '<span class="pr-meta">D</span>')
                xg_cell = (f'{g["xg_for"]} – {g["xg_against"]}'
                           if g["xg_for"] is not None and g["xg_against"] is not None
                           else '<span class="pr-meta">—</span>')
                ha   = "(H)" if g["was_home"] else "(A)"
                form_rows.append(f"""
                  <tr>
                    <td class="pr-meta">{g['date']}</td>
                    <td><a href="{_match_url(g['match_id'], (name if g['was_home'] else g['opponent']), (g['opponent'] if g['was_home'] else name))}" style="color:#fff">{name if g['was_home'] else g['opponent']} vs {g['opponent'] if g['was_home'] else name} <span class="pr-meta">{ha}</span></a></td>
                    <td class="pr-meta">{g['score']}</td>
                    <td class="pr-meta">{xg_cell}</td>
                    <td>{badge}</td>
                  </tr>""")
            recent_form_html = f"""
            <h2 class="pr-h2">Recent form (last {len(perf['recent_games'])} matches)</h2>
            <table class="pr-table">
              <thead><tr><th>Date</th><th>Match</th><th>Score</th><th>xG (for–against)</th><th>Result</th></tr></thead>
              <tbody>{''.join(form_rows)}</tbody>
            </table>"""
        else:
            recent_form_html = ""

        # ── Track-record stat strip (no win-rate; use avg odds + P&L)
        stat_strip = (
            f'<span class="pr-stat">Settled picks: <strong>{total}</strong></span>'
            f'<span class="pr-stat">Wins: <strong>{wins}</strong></span>'
            f'<span class="pr-stat">Losses: <strong>{losses}</strong></span>'
            + (f'<span class="pr-stat">Push: <strong>{voids}</strong></span>' if voids else '')
            + f'<span class="pr-stat">Avg odds: <strong>@{avg_odd:.2f}</strong></span>'
            + f'<span class="pr-stat">P&amp;L: <strong>{"+" if pnl > 0 else ""}{pnl:.0f}€</strong></span>'
        )

        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}/">WebPronos</a> › {name}
        </nav>
        <h1 class="pr-h1">{logo_img}{name} — AI Football Predictions, xG Analysis & Track Record</h1>
        <p class="pr-lead">{intro_html}</p>

        <div style="margin:1.5rem 0">{stat_strip}</div>

        {next_html}

        {recent_form_html}

        <h2 class="pr-h2">Last picks involving {name}</h2>
        <table class="pr-table">
          <thead><tr>
            <th>Date</th><th>Match</th><th>Score</th><th>Pick</th><th>Odds</th><th>Result</th>
          </tr></thead>
          <tbody>
            {''.join(rows_html) if rows_html else '<tr><td colspan="6" style="text-align:center;padding:2rem">No picks logged yet.</td></tr>'}
          </tbody>
        </table>

        {_render_pr_footer()}
        """

        # JSON-LD: SportsTeam + BreadcrumbList (helps SERP breadcrumb display)
        jsonld_team = {
            "@context": "https://schema.org",
            "@type": "SportsTeam",
            "name": name,
            "sport": "Soccer",
            "url": f"{SITE_URL}/team/{slug}",
            "logo": logo_url or f"{SITE_URL}/og/default.png",
        }
        jsonld_breadcrumbs = {
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "WebPronos", "item": SITE_URL},
                {"@type": "ListItem", "position": 2, "name": name,        "item": f"{SITE_URL}/team/{slug}"},
            ],
        }
        jsonld = json.dumps([jsonld_team, jsonld_breadcrumbs], ensure_ascii=False)

        # Meta description: include the strongest factual signal we have
        # (xG averages or recent streak) so the SERP snippet is informative.
        if perf.get("xg_summary"):
            xs = perf["xg_summary"]
            meta_desc = (
                f"AI football predictions and xG analysis for {name}. "
                f"Last {xs['sample_size']} matches: {xs['avg_for']} xG created, "
                f"{xs['avg_against']} xG conceded per game. {total} tracked picks with full audit trail."
            )
        elif perf.get("streak", {}).get("text"):
            meta_desc = (
                f"AI football predictions for {name} — {perf['streak']['text']} "
                f"(form: {perf['form_letters']}). {total} tracked picks with entry odds and live results."
            )
        else:
            meta_desc = (
                f"Live and historical AI football predictions for {name}. "
                f"{total} tracked picks with entry odds, live results, and a public profit/loss audit."
            )

        html = _build_html_page(
            title=f"{name} — AI Football Predictions & xG Analysis | WebPronos",
            description=meta_desc,
            canonical=f"{SITE_URL}/team/{slug}",
            body_html=body,
            jsonld=jsonld,
            og_image=logo_url or None,
        )
        _seo_cache_put(cache_key, html)
        return html, 200, {"Content-Type": "text/html; charset=utf-8",
                            "Cache-Control": "public, max-age=600",
                            "X-Prerender": "webpronos-team"}
    except Exception as e:
        log.exception(f"[prerender/team] Error for slug={slug}: {e}")
        return _render_passthrough(f"/team/{slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}


def _render_league(slug: str) -> tuple:
    """SSR for /league/{slug} — upcoming fixtures + recent picks for one competition."""
    cache_key = f"league:{slug}"
    cached = _seo_cache_get(cache_key)
    if cached:
        return cached, 200, {"Content-Type": "text/html; charset=utf-8",
                              "Cache-Control": "public, max-age=600",
                              "X-Prerender": "webpronos-league"}

    name = _league_by_slug(slug)
    if not name:
        _refresh_slug_index()
        name = _league_by_slug(slug)
    if not name:
        return _render_passthrough(f"/league/{slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}

    try:
        variants = _league_variants_for(name)
        placeholders = ",".join("?" * len(variants))
        with _db() as conn:
            recent_picks = conn.execute(f"""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals
                FROM tips t
                JOIN games g ON g.id = t.match_id
                WHERE g.tournament IN ({placeholders})
                ORDER BY t.wall_ts DESC
                LIMIT 25
            """, tuple(variants)).fetchall()
            country_row = conn.execute(
                f"SELECT country FROM games WHERE tournament IN ({placeholders}) "
                f"AND country IS NOT NULL LIMIT 1",
                tuple(variants)
            ).fetchone()
        country = country_row["country"] if country_row else ""

        # Upcoming fixtures from in-memory cache (today + next 2 days)
        upcoming = _upcoming_for_league(name, limit=20)
        from datetime import datetime as _dt, timezone as _tz
        fixt_rows = []
        for r in upcoming:
            ko = _dt.fromtimestamp(r["kickoff_ts"], tz=_tz.utc).strftime("%b %d, %H:%M UTC")
            fixt_rows.append(f"""
              <tr>
                <td class="pr-meta">{ko}</td>
                <td><a href="{_match_url(r['match_id'], r['home_team'], r['away_team'])}" style="color:#fff">{r['home_team']} vs {r['away_team']}</a></td>
              </tr>""")

        # Recent picks table
        pick_rows = []
        for p in recent_picks:
            date_str = _dt.fromtimestamp(p["wall_ts"], tz=_tz.utc).strftime("%b %d")
            score = (f'{p["home_goals"]}-{p["away_goals"]}'
                     if p["home_goals"] is not None else "—")
            if p["result"] in ("win", "green"):
                badge = '<span class="pr-win">✓ Won</span>'
            elif p["result"] in ("loss", "red"):
                badge = '<span class="pr-lose">✗ Lost</span>'
            else:
                badge = '<span class="pr-meta">Pending</span>'
            match = f"{p['home_team']} vs {p['away_team']}"
            pick_rows.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td><a href="{_match_url(p['match_id'], p['home_team'], p['away_team'])}" style="color:#fff">{match}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{p['market']} — {p['label']}</td>
                <td class="pr-meta">@{(p['odd_entry'] or 0):.2f}</td>
                <td>{badge}</td>
              </tr>""")

        logo_url = _league_logo(name) or ""
        logo_img = (f'<img src="{logo_url}" alt="{name} logo" '
                    f'style="width:64px;height:64px;vertical-align:middle;margin-right:.75rem">'
                    if logo_url else "")
        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}/">WebPronos</a> › <a href="{SITE_URL}/leagues" style="color:#22d3ee">Leagues</a> › {name}
        </nav>
        <h1 class="pr-h1">{logo_img}{name}</h1>
        <p class="pr-lead">AI-driven live and pre-match football tips for {name}{f" ({country})" if country else ""}. Upcoming fixtures and the latest recorded picks below.</p>

        <h2 class="pr-h2">Upcoming fixtures</h2>
        <table class="pr-table">
          <thead><tr><th>Kickoff</th><th>Match</th></tr></thead>
          <tbody>
            {''.join(fixt_rows) if fixt_rows else '<tr><td colspan="2" style="text-align:center;padding:2rem">No upcoming fixtures.</td></tr>'}
          </tbody>
        </table>

        <h2 class="pr-h2">Recent picks in {name}</h2>
        <table class="pr-table">
          <thead><tr>
            <th>Date</th><th>Match</th><th>Score</th><th>Pick</th><th>Odds</th><th>Result</th>
          </tr></thead>
          <tbody>
            {''.join(pick_rows) if pick_rows else '<tr><td colspan="6" style="text-align:center;padding:2rem">No picks logged yet for this league.</td></tr>'}
          </tbody>
        </table>

        {_render_pr_footer()}
        """

        jsonld = json.dumps({
            "@context": "https://schema.org",
            "@type": "SportsLeague",
            "name": name,
            "sport": "Soccer",
            "url": f"{SITE_URL}/league/{slug}",
            **({"location": {"@type": "Country", "name": country}} if country else {}),
        }, ensure_ascii=False)

        html = _build_html_page(
            title=f"{name} — Live Football Tips & Predictions | WebPronos",
            description=f"AI-generated tips and predictions for {name}. {len(upcoming)} upcoming fixtures and {len(recent_picks)} recent picks with full results.",
            canonical=f"{SITE_URL}/league/{slug}",
            body_html=body,
            jsonld=jsonld,
        )
        _seo_cache_put(cache_key, html)
        return html, 200, {"Content-Type": "text/html; charset=utf-8",
                            "Cache-Control": "public, max-age=600",
                            "X-Prerender": "webpronos-league"}
    except Exception as e:
        log.exception(f"[prerender/league] Error for slug={slug}: {e}")
        return _render_passthrough(f"/league/{slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}


def _render_tips_market(market_slug: str) -> tuple:
    """SSR for /tips/{market} — last picks of one market type."""
    cache_key = f"tips:{market_slug}"
    cached = _seo_cache_get(cache_key)
    if cached:
        return cached, 200, {"Content-Type": "text/html; charset=utf-8",
                              "Cache-Control": "public, max-age=600",
                              "X-Prerender": "webpronos-tips-market"}

    market_info = _TIP_MARKET_LABELS.get(market_slug)
    if not market_info:
        return _render_passthrough(f"/tips/{market_slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}
    pretty_name, like_patterns = market_info

    STAKE = get_setting("stake_per_bet", 100.0)
    try:
        # Match by tips.market against any of the configured LIKE patterns
        where_clause = " OR ".join("t.market LIKE ?" for _ in like_patterns)
        with _db() as conn:
            rows = conn.execute(f"""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.tournament
                FROM tips t
                JOIN games g ON g.id = t.match_id
                WHERE {where_clause}
                ORDER BY t.wall_ts DESC
                LIMIT 30
            """, tuple(like_patterns)).fetchall()

        wins = sum(1 for r in rows if r["result"] in ("win", "green"))
        losses = sum(1 for r in rows if r["result"] in ("loss", "red"))
        total_settled = wins + losses
        winrate = (wins / total_settled * 100) if total_settled else 0
        pnl = 0.0
        for r in rows:
            if r["result"] in ("win", "green") and r["odd_entry"]:
                pnl += (r["odd_entry"] - 1) * STAKE
            elif r["result"] in ("loss", "red"):
                pnl -= STAKE

        from datetime import datetime as _dt, timezone as _tz
        pick_rows = []
        for r in rows:
            date_str = _dt.fromtimestamp(r["wall_ts"], tz=_tz.utc).strftime("%b %d")
            score = (f'{r["home_goals"]}-{r["away_goals"]}'
                     if r["home_goals"] is not None else "—")
            if r["result"] in ("win", "green"):
                badge = '<span class="pr-win">✓ Won</span>'
            elif r["result"] in ("loss", "red"):
                badge = '<span class="pr-lose">✗ Lost</span>'
            else:
                badge = '<span class="pr-meta">Pending</span>'
            match = f"{r['home_team']} vs {r['away_team']}"
            pick_rows.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td class="pr-meta">{r['tournament'] or '—'}</td>
                <td><a href="{_match_url(r['match_id'], r['home_team'], r['away_team'])}" style="color:#fff">{match}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{r['label']}</td>
                <td class="pr-meta">@{(r['odd_entry'] or 0):.2f}</td>
                <td>{badge}</td>
              </tr>""")

        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}/">WebPronos</a> › Tips › {pretty_name}
        </nav>
        <h1 class="pr-h1">{pretty_name} — Live Football Tips</h1>
        <p class="pr-lead">All AI-detected {pretty_name} bets logged by the WebPronos live model. Each pick includes entry odds, result and running track record.</p>

        <div style="margin:1.5rem 0">
          <span class="pr-stat">Settled: <strong>{total_settled}</strong></span>
          <span class="pr-stat">Win rate: <strong>{winrate:.1f}%</strong></span>
          <span class="pr-stat">P&amp;L: <strong>{'+' if pnl > 0 else ''}{pnl:.0f}€</strong></span>
        </div>

        <h2 class="pr-h2">Latest {pretty_name} picks</h2>
        <table class="pr-table">
          <thead><tr>
            <th>Date</th><th>League</th><th>Match</th><th>Score</th><th>Pick</th><th>Odds</th><th>Result</th>
          </tr></thead>
          <tbody>
            {''.join(pick_rows) if pick_rows else '<tr><td colspan="7" style="text-align:center;padding:2rem">No picks logged yet for this market.</td></tr>'}
          </tbody>
        </table>

        {_render_pr_footer()}
        """

        jsonld = json.dumps({
            "@context": "https://schema.org",
            "@type": "CollectionPage",
            "name": f"{pretty_name} football tips — WebPronos",
            "url": f"{SITE_URL}/tips/{market_slug}",
            "about": pretty_name,
        }, ensure_ascii=False)

        html = _build_html_page(
            title=f"{pretty_name} Tips — Live AI Predictions | WebPronos",
            description=f"All {pretty_name} football tips logged by the WebPronos AI model with entry odds, results and ROI. {total_settled} settled picks tracked.",
            canonical=f"{SITE_URL}/tips/{market_slug}",
            body_html=body,
            jsonld=jsonld,
        )
        _seo_cache_put(cache_key, html)
        return html, 200, {"Content-Type": "text/html; charset=utf-8",
                            "Cache-Control": "public, max-age=600",
                            "X-Prerender": "webpronos-tips-market"}
    except Exception as e:
        log.exception(f"[prerender/tips-market] Error for {market_slug}: {e}")
        return _render_passthrough(f"/tips/{market_slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}


def _render_today() -> str:
    """SSR for /today — today's monitored fixtures."""
    cache_key = "page:today"
    cached = _seo_cache_get(cache_key)
    if cached:
        return cached
    try:
        from datetime import datetime as _dt, timezone as _tz
        date_str = _dt.now(_tz.utc).strftime("%Y-%m-%d")
        cached_day = _upcoming_cache.get(date_str)
        matches = cached_day["matches"] if cached_day else []

        rows = []
        for m in matches:
            ko_ts = m.get("startTimestamp") or 0
            ko = _dt.fromtimestamp(ko_ts, tz=_tz.utc).strftime("%H:%M UTC") if ko_ts else "—"
            rows.append(f"""
              <tr>
                <td class="pr-meta">{ko}</td>
                <td class="pr-meta">{m.get('tournament','')}</td>
                <td><a href="{_match_url(m['id'], m['homeTeam'], m['awayTeam'])}" style="color:#fff">{m['homeTeam']} vs {m['awayTeam']}</a></td>
              </tr>""")

        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}/">WebPronos</a> › Today
        </nav>
        <h1 class="pr-h1">Today's Football Matches — Live AI Tips</h1>
        <p class="pr-lead">Every monitored match scheduled for today. Click any fixture to follow the live xG model and AI tips in real time.</p>

        <table class="pr-table">
          <thead><tr><th>Kickoff</th><th>League</th><th>Match</th></tr></thead>
          <tbody>
            {''.join(rows) if rows else '<tr><td colspan="3" style="text-align:center;padding:2rem">No monitored matches today.</td></tr>'}
          </tbody>
        </table>

        {_render_pr_footer()}
        """

        jsonld = json.dumps({
            "@context": "https://schema.org",
            "@type": "ItemList",
            "name": f"Football matches today — {date_str}",
            "numberOfItems": len(matches),
            "itemListElement": [{
                "@type": "ListItem",
                "position": i + 1,
                "url": f"{SITE_URL}/match/{m['id']}",
                "name": f"{m['homeTeam']} vs {m['awayTeam']}",
            } for i, m in enumerate(matches[:50])],
        }, ensure_ascii=False)

        html = _build_html_page(
            title=f"Today's Football Tips & Live Predictions | WebPronos",
            description=f"All {len(matches)} monitored football matches scheduled for today with live AI tips, xG model and value detection.",
            canonical=f"{SITE_URL}/today",
            body_html=body,
            jsonld=jsonld,
        )
        _seo_cache_put(cache_key, html)
        return html
    except Exception as e:
        log.exception(f"[prerender/today] Error: {e}")
        return _build_html_page(
            title="Today's Football Tips | WebPronos",
            description="Today's monitored football matches with live AI tips.",
            canonical=f"{SITE_URL}/today",
            body_html=f'<h1>Today</h1><p>Loading…</p>{_render_pr_footer()}',
        )


# ── Homepage ──────────────────────────────────────────────────────────────
def _render_homepage() -> str:
    """SSR for / — builds a semantically correct homepage with exactly ONE H1."""
    body_html = f"""
<section style="max-width:800px;margin:0 auto;padding:2rem 1rem">
  <h1 style="font-size:2.2rem;font-weight:900;color:#fff;line-height:1.2;margin:0 0 .75rem">
    Live Football Tips &amp; In-Play Predictions — Updated Every 15 Seconds
  </h1>
  <p style="color:#94a3b8;font-size:1.1rem;margin:0 0 2rem">
    AI-powered football picks across 25+ competitions. Our algorithm tracks xG, momentum shifts
    and live odds to fire tips during the match — not before it.
  </p>
  <a href="/history" style="display:inline-block;background:#22d3ee;color:#0f172a;font-weight:700;padding:.75rem 1.5rem;border-radius:.5rem;text-decoration:none;margin-bottom:2rem;transition:background 200ms">
    View Historical Results →
  </a>

  <h2 style="font-size:1.4rem;font-weight:700;color:#e2e8f0;margin:2rem 0 .5rem">
    How it works
  </h2>
  <p style="color:#94a3b8;margin:0 0 1rem">
    Every match in our database is monitored minute-by-minute. When xG diverges from the
    scoreline and live odds offer value, the algorithm fires a pick — visible instantly on
    the live dashboard.
  </p>

  <h2 style="font-size:1.4rem;font-weight:700;color:#e2e8f0;margin:2rem 0 .5rem">
    Why in-play betting?
  </h2>
  <p style="color:#94a3b8;margin:0 0 1rem">
    Pre-match odds are heavily efficient. In-play markets move fast and are often mis-priced
    for 2–3 minutes after a key event — that's the window our model exploits.
  </p>

  <h2 style="font-size:1.4rem;font-weight:700;color:#e2e8f0;margin:2rem 0 .5rem">
    Track record
  </h2>
  <p style="color:#94a3b8;margin:0 0 2rem">
    All picks are logged with entry time, odds and result. Check the
    <a href="/history" style="color:#22d3ee;text-decoration:none">historical performance page</a>
    for full transparency.
  </p>
</section>
{_render_pr_footer()}"""

    return _build_html_page(
        title="WebPronos — Live Football Tips & In-Play Predictions",
        description="AI-powered in-play football tips updated every 15 seconds. xG-based picks across 25+ competitions with full track record.",
        canonical=f"{SITE_URL}/",
        body_html=body_html,
        jsonld=json.dumps({
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": "WebPronos",
            "url": SITE_URL,
            "description": "Live football tips and in-play predictions powered by xG analytics.",
            "potentialAction": {
                "@type": "SearchAction",
                "target": f"{SITE_URL}/history",
                "query-input": "required name=search_term_string"
            }
        }, ensure_ascii=False),
    )


# ── Static-content pages (about, terms, etc.) ─────────────────────────────
def _render_passthrough(canonical_path: str = "/") -> str:
    """For static pages — serve Lovable shell unchanged (it has good content already)."""
    base_html = _get_base_html()
    if base_html:
        return base_html
    return _build_html_page(
        title="WebPronos — Live Football Predictions",
        description="AI-powered football tips across 25 competitions.",
        canonical=f"{SITE_URL}{canonical_path}",
        body_html=f"<h1>WebPronos</h1>{_render_pr_footer()}",
    )


# ── Unified dispatcher ────────────────────────────────────────────────────
# ───────────────────── SEO JSON endpoints ──────────────────────
# These endpoints expose the same data the prerender HTML pages use,
# but as raw JSON so the Lovable frontend can render React equivalents
# at /team/{slug}, /league/{slug}, /tips/{market}, /today. Real users
# coming from Google search results land on the React page, which calls
# these endpoints. Bots still get the prerender HTML (no JS dependency).

def _format_pick_row(row, *, perspective_team: str | None = None) -> dict:
    """Normalize a tips+games joined row into the shape Lovable expects."""
    home = row["home_team"]
    away = row["away_team"]
    is_finished = bool(row["is_finished"]) if "is_finished" in row.keys() else (row["home_goals"] is not None)
    score = (f'{row["home_goals"]}-{row["away_goals"]}'
             if row["home_goals"] is not None and is_finished else None)

    # Result normalization: "win"/"green" → "win", "loss"/"red" → "loss"
    raw_result = row["result"]
    if raw_result in ("win", "green"):
        result = "win"
    elif raw_result in ("loss", "red"):
        result = "loss"
    else:
        result = None

    out = {
        "match_id":   row["match_id"],
        "wall_ts":    row["wall_ts"],
        "home_team":  home,
        "away_team":  away,
        "score":      score,
        "is_finished": is_finished,
        "market":     row["market"],
        "label":      row["label"],
        "odds":       row["odd_entry"],
        "result":     result,
    }
    if "tournament" in row.keys() and row["tournament"]:
        out["tournament"] = row["tournament"]
    if perspective_team:
        out["opponent"] = away if perspective_team == home else home
        out["is_home"]  = (perspective_team == home)
    return out


def _calc_stats(picks: list[dict]) -> dict:
    """Standard stats block: settled count, wins, losses, win rate, P&L."""
    STAKE = get_setting("stake_per_bet", 100.0)
    settled = [p for p in picks if p["result"] in ("win", "loss")]
    wins   = sum(1 for p in settled if p["result"] == "win")
    losses = sum(1 for p in settled if p["result"] == "loss")
    pnl    = 0.0
    for p in settled:
        if p["result"] == "win" and p["odds"]:
            pnl += (p["odds"] - 1) * STAKE
        elif p["result"] == "loss":
            pnl -= STAKE
    return {
        "settled":  len(settled),
        "wins":     wins,
        "losses":   losses,
        "win_rate": (wins / len(settled) * 100) if settled else 0.0,
        "pnl":      round(pnl, 2),
    }


@app.route("/api/seo/team/<slug>")
def r_seo_team(slug: str):
    name = _team_by_slug(slug)
    if not name:
        _refresh_slug_index()
        name = _team_by_slug(slug)
    if not name:
        return jsonify({"error": "team_not_found", "slug": slug}), 404
    cache_key = f"jsonteam:{slug}"
    cached_body = _seo_cache_get(cache_key)
    if cached_body:
        return cached_body, 200, {"Content-Type": "application/json",
                                   "Cache-Control": "public, max-age=300"}
    try:
        with _db() as conn:
            picks_rows = conn.execute("""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.tournament, g.country, g.start_ts, g.is_finished
                FROM tips t
                JOIN games g ON g.id = t.match_id
                WHERE g.home_team = ? OR g.away_team = ?
                ORDER BY t.wall_ts DESC
                LIMIT 30
            """, (name, name)).fetchall()
        # Same reasoning as league: next fixture comes from the live cache, not
        # the local `games` table.
        next_fix = _next_fixture_for_team(name)

        picks = [_format_pick_row(r, perspective_team=name) for r in picks_rows]
        # On-pitch performance (form, xG averages, streak) — same data the
        # prerender uses to build its SEO copy. Exposed here so the Lovable
        # React app can render the same enriched view to human users.
        performance = _team_performance(name, recent_n=5)
        payload = {
            "team_name":    name,
            "slug":         slug,
            "logo_url":     _quick_logo(name) or None,
            "stats":        _calc_stats(picks),
            "next_fixture": next_fix,
            "picks":        picks,
            "performance":  performance,
        }
        body = json.dumps(payload, ensure_ascii=False)
        _seo_cache_put(cache_key, body)
        return body, 200, {"Content-Type": "application/json",
                            "Cache-Control": "public, max-age=300"}
    except Exception as e:
        log.exception(f"[api/seo/team] {slug}: {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/api/seo/league/<slug>")
def r_seo_league(slug: str):
    name = _league_by_slug(slug)
    if not name:
        _refresh_slug_index()
        name = _league_by_slug(slug)
    if not name:
        return jsonify({"error": "league_not_found", "slug": slug}), 404

    # JSON cache — same TTL pattern as the prerender HTML cache (15 min).
    # Cuts cold-page load from ~1-2s (Sofascore + DB) to <50ms.
    cache_key = f"jsonleague:{slug}"
    cached_body = _seo_cache_get(cache_key)
    if cached_body:
        return cached_body, 200, {"Content-Type": "application/json",
                                   "Cache-Control": "public, max-age=300"}
    try:
        variants = _league_variants_for(name)
        placeholders = ",".join("?" * len(variants))
        with _db() as conn:
            picks_rows = conn.execute(f"""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.is_finished, g.tournament
                FROM tips t
                JOIN games g ON g.id = t.match_id
                WHERE g.tournament IN ({placeholders})
                ORDER BY t.wall_ts DESC LIMIT 25
            """, tuple(variants)).fetchall()
            country_row = conn.execute(
                f"SELECT country FROM games WHERE tournament IN ({placeholders}) "
                f"AND country IS NOT NULL LIMIT 1",
                tuple(variants)).fetchone()

        upcoming = _upcoming_for_league(name, limit=20)
        picks = [_format_pick_row(r) for r in picks_rows]
        payload = {
            "league_name":  name,
            "slug":         slug,
            "country":      country_row["country"] if country_row else None,
            "logo_url":     _league_logo(name),
            "upcoming":     upcoming,
            "recent_picks": picks,
            "stats":        _calc_stats(picks),
        }
        body = json.dumps(payload, ensure_ascii=False)
        _seo_cache_put(cache_key, body)
        return body, 200, {"Content-Type": "application/json",
                            "Cache-Control": "public, max-age=300"}
    except Exception as e:
        log.exception(f"[api/seo/league] {slug}: {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/api/seo/tips/<market_slug>")
def r_seo_tips(market_slug: str):
    market_info = _TIP_MARKET_LABELS.get(market_slug)
    if not market_info:
        return jsonify({
            "error": "market_not_found",
            "slug":  market_slug,
            "available_slugs": list(_TIP_MARKET_LABELS.keys()),
        }), 404
    pretty_name, like_patterns = market_info
    try:
        where_clause = " OR ".join("t.market LIKE ?" for _ in like_patterns)
        with _db() as conn:
            rows = conn.execute(f"""
                SELECT t.match_id, t.market, t.label, t.odd_entry, t.result, t.wall_ts,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.is_finished, g.tournament
                FROM tips t
                JOIN games g ON g.id = t.match_id
                WHERE {where_clause}
                ORDER BY t.wall_ts DESC LIMIT 30
            """, tuple(like_patterns)).fetchall()
        picks = [_format_pick_row(r) for r in rows]
        return jsonify({
            "market_slug": market_slug,
            "market_name": pretty_name,
            "stats":       _calc_stats(picks),
            "picks":       picks,
        })
    except Exception as e:
        log.exception(f"[api/seo/tips] {market_slug}: {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/api/seo/today")
def r_seo_today():
    """Today's fixtures — only leagues we actively monitor (≥1 pick in DB)."""
    try:
        matches = _filter_monitored(get_scheduled())
        return jsonify({
            "date":    datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "count":   len(matches),
            "matches": matches,
        })
    except Exception as e:
        log.exception(f"[api/seo/today] {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/api/seo/tomorrow")
def r_seo_tomorrow():
    """Tomorrow's fixtures — monitored leagues only."""
    try:
        date_str = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        matches  = _filter_monitored(get_scheduled(date_str))
        return jsonify({
            "date":    date_str,
            "count":   len(matches),
            "matches": matches,
        })
    except Exception as e:
        log.exception(f"[api/seo/tomorrow] {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/api/seo/schedule/<date_str>")
def r_seo_schedule(date_str: str):
    """Fixtures for any date (YYYY-MM-DD) — monitored leagues only."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        matches = _filter_monitored(get_scheduled(date_str))
        return jsonify({
            "date":    date_str,
            "count":   len(matches),
            "matches": matches,
        })
    except ValueError:
        return jsonify({"error": "bad_date", "expected": "YYYY-MM-DD"}), 400
    except Exception as e:
        log.exception(f"[api/seo/schedule] {date_str}: {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/api/league-logo/<slug>")
def r_league_logo_proxy(slug: str):
    """
    Image proxy for competition badges. Lovable / browsers can't fetch
    Sofascore's image CDN directly (returns 403 to non-Sofascore origins).
    This route fetches server-side via curl_cffi + Chrome TLS impersonation
    and serves the bytes with a long cache header.
    """
    # Strip optional .png/.svg/.webp suffix for nicer URLs
    raw_slug = slug
    if "." in slug:
        slug = slug.rsplit(".", 1)[0]

    cached = _league_logo_bytes.get(slug)
    if cached:
        body, ct = cached
        return body, 200, {
            "Content-Type":  ct,
            "Cache-Control": "public, max-age=86400, immutable",
            "X-Cache":       "HIT",
        }

    name = _league_by_slug(slug)
    if not name:
        _refresh_slug_index()
        name = _league_by_slug(slug)
    if not name:
        # Short cache: don't poison the browser if the slug becomes
        # valid after slug-index refresh.
        return ("Not found", 404, {"Cache-Control": "max-age=60"})

    tid = _resolve_league_tid(name)
    if not tid:
        # Memo cold — try a quick 3-day deep resolve here so we don't
        # serve the user a permanent monogram while warmup catches up.
        tid = _resolve_league_tid_quick(name)
    if not tid:
        # Don't long-cache the miss — warmup will populate within 30s.
        return ("Not found", 404, {"Cache-Control": "max-age=60"})

    fetched = _get_bytes(f"https://api.sofascore.com/api/v1/unique-tournament/{tid}/image")
    if not fetched:
        return ("Upstream fetch failed", 502, {"Cache-Control": "max-age=60"})
    body, ct = fetched
    _league_logo_bytes[slug] = (body, ct)
    return body, 200, {
        "Content-Type":  ct,
        "Cache-Control": "public, max-age=86400, immutable",
        "X-Cache":       "MISS",
    }


# Useful for the Lovable frontend footer / discovery
@app.route("/api/seo/index")
def r_seo_index():
    """Top-N teams, leagues, and the available tip markets — for footer links."""
    try:
        if not _slug_index_cache["teams"] or not _slug_index_cache["leagues"]:
            _refresh_slug_index()
        with _db() as conn:
            top_teams = conn.execute("""
                SELECT g.home_team AS name, COUNT(*) AS n
                FROM tips t JOIN games g ON g.id = t.match_id
                GROUP BY g.home_team ORDER BY n DESC LIMIT 12
            """).fetchall()
            top_leagues = conn.execute("""
                SELECT g.tournament AS name, COUNT(*) AS n
                FROM tips t JOIN games g ON g.id = t.match_id
                WHERE g.tournament IS NOT NULL
                GROUP BY g.tournament ORDER BY n DESC LIMIT 8
            """).fetchall()
        # Resolve canonical slugs
        teams_idx = {v: k for k, v in _slug_index_cache["teams"].items()}
        leagues_idx = {v: k for k, v in _slug_index_cache["leagues"].items()}
        return jsonify({
            "top_teams": [{"name": r["name"], "slug": teams_idx.get(r["name"], _slug(r["name"]))}
                          for r in top_teams if r["name"]],
            "top_leagues": [{"name": r["name"], "slug": leagues_idx.get(r["name"], _slug(r["name"]))}
                            for r in top_leagues if r["name"]],
            "tip_markets": [{"slug": s, "name": v[0]} for s, v in _TIP_MARKET_LABELS.items()],
        })
    except Exception as e:
        log.exception(f"[api/seo/index] {e}")
        return jsonify({"error": "internal", "detail": str(e)}), 500


@app.route("/prerender")
def prerender_dispatch():
    """
    Single bot-facing entry point. The Cloudflare Worker forwards every bot
    request here with ?path=<original_path>. This dispatcher returns the
    correct fully-rendered HTML for that path.
    """
    import re as _re
    try:
        path = (flask_request.args.get("path") or "/").strip()
        # Normalize: strip query string from path, ensure leading slash
        if "?" in path:
            path = path.split("?")[0]
        if not path.startswith("/"):
            path = "/" + path
        # Strip trailing slash (except root)
        if len(path) > 1 and path.endswith("/"):
            path = path.rstrip("/")

        # Route patterns
        if path == "/" or path == "":
            html = _render_homepage()
        elif path == "/blog":
            html = _render_blog_listing()
        elif _re.match(r'^/blog/[^/]+$', path):
            slug = path[len("/blog/"):]
            return prerender_blog(slug)
        elif _re.match(r'^/match/\d+', path):
            mid = int(_re.match(r'^/match/(\d+)', path).group(1))
            return prerender_match(mid)
        elif _re.match(r'^/team/[^/]+$', path):
            return _render_team(path[len("/team/"):])
        elif _re.match(r'^/league/[^/]+$', path):
            return _render_league(path[len("/league/"):])
        elif _re.match(r'^/tips/[^/]+$', path):
            return _render_tips_market(path[len("/tips/"):])
        elif path == "/today":
            html = _render_today()
        elif path == "/history":
            html = _render_history()
        elif path == "/tomorrow" or path == "/upcoming":
            html = _render_tomorrow()
        elif path in ("/about", "/terms", "/privacy", "/responsible-gambling"):
            html = _render_passthrough(path)
        else:
            # Unknown path — pass through Lovable shell
            html = _render_passthrough(path)

        return html, 200, {
            "Content-Type":  "text/html; charset=utf-8",
            "Cache-Control": "public, max-age=300",   # 5min cache
            "X-Prerender":   "webpronos-v1",
        }
    except Exception as e:
        log.exception(f"[prerender] dispatcher error for path={flask_request.args.get('path')}: {e}")
        return _render_passthrough("/"), 200, {"Content-Type": "text/html; charset=utf-8"}


# ════════════════════════════════════════════════════════════
#  SCHEDULED TASKS — Daily summary at 23:00 Lisbon time
# ════════════════════════════════════════════════════════════

def _init_scheduler():
    """
    Initialize APScheduler for daily summary messages.

    Hardening (2026-05-08): previous version had three issues that caused
    the 07/05 summary to be skipped:
      1. With gunicorn --workers 2, each worker spawned its own scheduler,
         making behaviour racy (could double-send or skip).
      2. APScheduler's default misfire_grace_time is 1 second, so any
         deploy/restart within seconds of the trigger silently dropped it.
      3. Trigger at 23:00 + days_back=0 summarised "today so far",
         missing tips that settled between 23:00–23:59.

    Fix: cross-worker DB lock (only one instance fires per minute per day),
    1h grace window, trigger moved to 23:55 with days_back=0 still
    summarising the current Lisbon day (now nearly complete).
    """
    scheduler = BackgroundScheduler()
    # 23:55 Lisbon — late enough to capture nearly all settled tips,
    # before Lisbon midnight rolls the date.
    trigger = CronTrigger(hour=23, minute=55, timezone='Europe/Lisbon')
    scheduler.add_job(
        _send_daily_summary_locked,
        trigger=trigger,
        id='daily_summary',
        replace_existing=True,
        misfire_grace_time=3600,  # 1h: still fires if process restarted late
        coalesce=True,            # collapse multiple pending fires into one
        max_instances=1,
    )
    scheduler.start()
    log.info("Scheduler started: daily summary at 23:55 Lisbon time (1h grace, DB-locked)")
    return scheduler


def _send_daily_summary_locked():
    """
    Wrapper around _send_daily_summary that takes a DB-backed lock.
    Prevents duplicate sends when multiple gunicorn workers each spawn
    their own scheduler — the first worker to insert the lock row wins;
    others see UNIQUE violation and bail out silently.
    """
    from datetime import datetime
    lisbon_tz = pytz.timezone('Europe/Lisbon')
    today_str = datetime.now(lisbon_tz).strftime("%Y-%m-%d")
    try:
        with _db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary_locks (
                    day TEXT PRIMARY KEY,
                    sent_at INTEGER NOT NULL
                )
            """)
            try:
                conn.execute(
                    "INSERT INTO daily_summary_locks (day, sent_at) VALUES (?, ?)",
                    (today_str, int(datetime.utcnow().timestamp()))
                )
            except sqlite3.IntegrityError:
                log.info(f"_send_daily_summary_locked: another worker already sent for {today_str}")
                return
        # Lock acquired → send (days_back=0 = today, called near end of Lisbon day)
        _send_daily_summary(days_back=0, force_send=False)
    except Exception as e:
        log.error(f"_send_daily_summary_locked error: {e}", exc_info=True)


_scheduler = None


if __name__ == "__main__":
    # CLI: prewarm subcommand runs in a clean OS process (no gevent)
    if len(sys.argv) > 1 and sys.argv[1] == "prewarm-logos":
        _run_prewarm_cli()
        sys.exit(0)

    _load_aliases()

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        cli_test()
    else:
        print("=" * 60)
        print("  Sofascore xG Scraper v4 — http://localhost:5050")
        print("  + Live Odds & Benter Value Engine")
        print("=" * 60)
        _init_client()
        _init_db()
        _scheduler = _init_scheduler()
        threading.Thread(target=_background_loop, daemon=True).start()
        print(f"  Client: {_client_type}")
        print(f"  Odds API: enabled")
        print(f"  Background engine: every {BG_INTERVAL}s")
        print(f"  Team aliases: {len(_team_aliases)} loaded")
        print(f"  Scheduler: daily summary at 23:55 Lisbon time\n")
        app.run(host="0.0.0.0", port=5050, debug=True)
else:
    # Running under gunicorn — __main__ block is skipped, so initialize here
    _load_aliases()
    _init_db()
    _scheduler = _init_scheduler()
    threading.Thread(target=_init_client, daemon=True).start()
    threading.Thread(target=_background_loop, daemon=True).start()
    # Pre-warm logos cache so the first API response already has logos inline
    threading.Thread(target=_load_logos, daemon=True).start()
    # Cheap: load fuzzy memo from disk if it exists (instant ~1ms read)
    _load_fuzzy_memo_from_disk()
    # Spawn fuzzy resolver as a SUBPROCESS — runs in a clean OS process so it
    # NEVER blocks gunicorn's gevent event loop. Subprocess writes memo to disk;
    # we reload it when subprocess exits. Skipped if memo already populated.
    if not _fuzzy_logo_memo:
        threading.Thread(target=_prewarm_fuzzy_logos, daemon=True).start()
