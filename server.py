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
from datetime import datetime, timezone
from difflib import SequenceMatcher

from flask import Flask, jsonify, request as flask_request, Response

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
    "australia": "🇦🇺", "china": "🇨🇳", "international": "🌍",
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

def _tg_subscribe(chat_id: int, username: str = None, first_name: str = None):
    with _db() as conn:
        conn.execute("""
            INSERT INTO tg_subscribers (chat_id, username, first_name, subscribed_at, active)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(chat_id) DO UPDATE SET active = 1, subscribed_at = excluded.subscribed_at
        """, (chat_id, username, first_name, int(time.time())))

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
            recent = conn.execute("""
                SELECT chat_id, username, first_name, start_param, started_at
                FROM tg_starts
                ORDER BY started_at DESC
                LIMIT 5
            """).fetchall()

        lines = [
            "📊 <b>Admin Stats</b>",
            "",
            f"👥 <b>Users:</b> {unique_users} unique  ({active_subs} active subs · {inactive_subs} unsubscribed)",
            f"🚀 <b>/start events:</b> {total_starts} total",
            "",
            f"📅 <b>Last 24h:</b>  {starts_24h} starts · {new_users_24h} new users",
            f"📆 <b>Last 7d:</b>   {starts_7d} starts · {new_users_7d} new users",
            "",
            "🔗 <b>Top sources (start params):</b>",
        ]
        if top_params:
            for r in top_params:
                src = (r["src"] or "(none)")[:30]
                lines.append(f"  • <code>{src}</code> — {r['c']} starts ({r['u']} users)")
        else:
            lines.append("  <i>(no /start events yet)</i>")

        lines.append("")
        lines.append("🆕 <b>Latest 5 /start events:</b>")
        if recent:
            for r in recent:
                from datetime import datetime as _dt, timezone as _tz
                ts = _dt.fromtimestamp(r["started_at"], tz=_tz.utc).strftime("%m-%d %H:%M")
                uname = f"@{r['username']}" if r["username"] else (r["first_name"] or f"id:{r['chat_id']}")
                param = f" [{r['start_param']}]" if r["start_param"] else ""
                lines.append(f"  • {ts} — {uname}{param}")
        else:
            lines.append("  <i>(none)</i>")

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
        _tg_subscribe(chat_id, username=username, first_name=first_name)
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

    # Fallback: any bookmaker
    for bm in bookmakers:
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

    # 1) curl_cffi
    try:
        from curl_cffi.requests import Session as CffiSession
        _session = CffiSession(impersonate="chrome")
        resp = _session.get(SOFASCORE_WEB, timeout=15)
        if resp.status_code == 200:
            _client_type = "curl_cffi"
            log.info("Using curl_cffi (Chrome TLS impersonation)")
            return True
    except ImportError:
        log.info("curl_cffi not available")
    except Exception as e:
        log.warning(f"curl_cffi failed: {e}")

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


def _get(url, retries=3):
    global _last_req
    if _session is None:
        _init_client()

    for attempt in range(retries):
        wait = REQ_GAP - (time.time() - _last_req)
        if wait > 0:
            time.sleep(wait)
        _last_req = time.time()

        try:
            resp = _session.get(url, timeout=15)

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
        "tournament": tourn.get("name", ""),
        "country": tourn.get("category", {}).get("name", ""),
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
            id          INTEGER PRIMARY KEY,
            home_team   TEXT NOT NULL,
            away_team   TEXT NOT NULL,
            home_goals  INTEGER DEFAULT 0,
            away_goals  INTEGER DEFAULT 0,
            tournament  TEXT,
            country     TEXT,
            is_finished INTEGER DEFAULT 0,
            archived_at INTEGER,
            start_ts    INTEGER
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
        """)
    # Migration: add edge_entry column to existing DBs
    with _db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tips)").fetchall()]
        if "edge_entry" not in cols:
            conn.execute("ALTER TABLE tips ADD COLUMN edge_entry REAL")
            log.info("DB migration: added edge_entry column to tips")

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
        conn.execute("""
            INSERT INTO games (id, home_team, away_team, home_goals, away_goals,
                               tournament, country, is_finished, start_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                home_goals  = excluded.home_goals,
                away_goals  = excluded.away_goals,
                is_finished = excluded.is_finished
        """, (
            match["id"], match["homeTeam"], match["awayTeam"],
            match["homeGoals"], match["awayGoals"],
            match.get("tournament"), match.get("country"),
            1 if match.get("isFinished") else 0,
            match.get("startTimestamp"),
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
                conn.execute("""
                    INSERT INTO tips (tip_key, match_id, market, label,
                                      odd_entry, odd_now, edge_entry, minute_entry, wall_ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (key, match_id, p["market"], p["label"],
                      p["odds"], p["odds"], p.get("edge"), minute, now_ts))
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
            live_picks = []
            for p in picks:
                live_picks.append({
                    "market":     p.get("market"),
                    "label":      p.get("label"),
                    "odds":       p.get("odds"),       # current live bookmaker odds
                    "edge":       p.get("edge", 0),    # current edge (positive by definition)
                    "blend":      p.get("blend", 0),
                    "model":      p.get("model", 0),
                    "minute":     minute,
                })

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
    with _db() as conn:
        pending = conn.execute(
            "SELECT id FROM games WHERE is_finished = 0"
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
    # Spain
    "la liga": None,
    "laliga": None,
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
            result.append(gd)
    return jsonify({"games": result, "count": len(result)})


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
    if time.time() - _logos_ts > _LOGOS_TTL or not _logos_cache:
        _load_logos()
    return _logos_cache

def _fuzzy_logo(name: str, threshold: float = 0.72) -> str | None:
    """
    Return a logo URL for *name* using a tiered lookup:
      1. Exact match  (original keys)
      2. Normalized exact match
      3. Best fuzzy match via SequenceMatcher (above threshold)
    Returns None when no acceptable match is found.
    """
    logos = _get_logos()

    # 1. exact
    if name in logos:
        return logos[name]

    # 2. normalized exact
    norm_cache = _logos_norm_cache
    nkey = _normalize_team_for_logo(name)
    if nkey in norm_cache:
        return norm_cache[nkey]

    # 3. fuzzy over normalized keys
    best_score = 0.0
    best_url   = None
    for stored_norm, url in norm_cache.items():
        score = SequenceMatcher(None, nkey, stored_norm).ratio()
        if score > best_score:
            best_score = score
            best_url   = url
    if best_score >= threshold:
        log.debug(f"Fuzzy logo match '{name}' → score={best_score:.2f}")
        return best_url

    return None


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


@app.route("/sitemap.xml")
def r_sitemap():
    """
    Dynamic XML sitemap for webpronos.com.
    Includes ONLY accessible pages:
      - Homepage
      - Live matches (in progress right now)
      - Scheduled matches (not yet started, start_ts in the next 48h)
    Finished matches are excluded — their pages are no longer accessible.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    next_48h = now_ts + 48 * 3600

    # ── 1. Live games from _live_state ─────────────────────────────────────────
    live_ids: dict[int, dict] = {}
    try:
        with _state_lock:
            for entry in _live_state.values():
                m = entry.get("match", {})
                mid = m.get("id")
                if mid and m.get("statusType") == "inprogress":
                    live_ids[mid] = m
    except Exception:
        pass

    # ── 2. Scheduled matches from DB (not yet started, within next 48h) ────────
    try:
        with _db() as conn:
            scheduled_rows = conn.execute(
                """
                SELECT id, home_team, away_team, start_ts
                FROM games
                WHERE is_finished = 0
                  AND start_ts > ?
                  AND start_ts <= ?
                ORDER BY start_ts ASC
                LIMIT 200
                """,
                (now_ts, next_48h)
            ).fetchall()
    except Exception:
        scheduled_rows = []

    # ── 3. Build match URL list ────────────────────────────────────────────────
    match_urls: list[tuple] = []

    # Live games (highest priority)
    for mid, m in live_ids.items():
        slug_part = f"{_slug(m.get('homeTeam', 'home'))}-{_slug(m.get('awayTeam', 'away'))}"
        match_urls.append((
            f"{SITE_URL}/match/{mid}/{slug_part}",
            now.strftime("%Y-%m-%d"),
            "always",
            "0.9",
        ))

    # Scheduled games
    seen_live = set(live_ids.keys())
    for r in scheduled_rows:
        if r["id"] in seen_live:
            continue
        slug_part = f"{_slug(r['home_team'])}-{_slug(r['away_team'])}"
        match_urls.append((
            f"{SITE_URL}/match/{r['id']}/{slug_part}",
            now.strftime("%Y-%m-%d"),
            "daily",
            "0.7",
        ))

    # ── 3. Build XML ───────────────────────────────────────────────────────────
    static_pages = [
        (SITE_URL + "/",           now.strftime("%Y-%m-%d"), "daily",  "1.0"),
    ]

    def url_block(loc, lastmod, changefreq, priority):
        return (
            f"  <url>\n"
            f"    <loc>{loc}</loc>\n"
            f"    <lastmod>{lastmod}</lastmod>\n"
            f"    <changefreq>{changefreq}</changefreq>\n"
            f"    <priority>{priority}</priority>\n"
            f"  </url>"
        )

    all_urls = static_pages + match_urls
    url_blocks = "\n".join(url_block(*u) for u in all_urls)

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{url_blocks}\n"
        "</urlset>"
    )

    return Response(
        xml,
        mimetype="application/xml",
        headers={
            "Cache-Control": "public, max-age=600, s-maxage=600",
            "X-Sitemap-Urls": str(len(all_urls)),
        },
    )


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


# ════════════════════════════════════════════════════════════
#  SEO PRERENDER — Dynamic meta tags for crawlers & social bots
#  Cloudflare Worker routes bots here; humans go to Lovable SPA
# ════════════════════════════════════════════════════════════

SUPABASE_URL  = os.environ.get("SUPABASE_URL", "https://lcugjwhcmtpdoernjgei.supabase.co")
SUPABASE_ANON = os.environ.get("SUPABASE_ANON_KEY", "")
SITE_URL      = os.environ.get("SITE_URL", "https://webpronos.com")
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
    if override and override.get("meta_title"):
        title = override["meta_title"]
    elif status == "inprogress":
        title = f"{home} {h_gls}–{a_gls} {away} LIVE – xG Predictions | {SITE_NAME}"
    elif status == "finished":
        title = f"{home} {h_gls}–{a_gls} {away} – Final xG Analysis | {SITE_NAME}"
    else:
        if tourn:
            title = f"{home} vs {away} ({tourn}) – Live xG Predictions | {SITE_NAME}"
        else:
            title = f"{home} vs {away} – Live xG Predictions | {SITE_NAME}"

    # Build description (English — primary language)
    if override and override.get("meta_description"):
        desc = override["meta_description"]
    else:
        if status == "inprogress":
            desc = (
                f"Follow {home} vs {away} live. "
                f"Current score: {h_gls}–{a_gls}. xG probabilities and predictions updated every minute."
            )
        elif status == "finished":
            desc = (
                f"Full xG analysis for {home} {h_gls}–{a_gls} {away}. "
                f"Expected Goals, win probabilities and value bets generated by the WebPronos algorithm."
            )
        else:
            desc = (
                f"Live xG predictions for {home} vs {away}"
                + (f" – {tourn}" if tourn else "")
                + f". Real-time probabilities, value bets and match analysis on {SITE_NAME}."
            )

    # Append live odds snippet if available
    if odds and odds.get("h2h") and odds["h2h"].get("outcomes") and not (override and override.get("meta_description")):
        try:
            oc = {o["name"]: o["price"] for o in odds["h2h"]["outcomes"]}
            h_price = next((v for k, v in oc.items() if home.split()[0].lower() in k.lower()), None)
            a_price = next((v for k, v in oc.items() if away.split()[0].lower() in k.lower()), None)
            if h_price and a_price:
                desc += f" Odds: {home} @{h_price} | {away} @{a_price}."
        except Exception:
            pass

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


@app.route("/prerender/match/<int:match_id>")
def prerender_match(match_id: int):
    """
    SEO prerender endpoint for match pages.
    Called by the Cloudflare Worker when a bot (Googlebot, Twitterbot, etc.) requests /match/:id.
    Returns the Lovable SPA shell with dynamic meta tags injected for the specific match.
    """
    try:
        # 1. Fetch match data
        event = get_event(match_id)
        if not event:
            return "Not found", 404

        # 2. Fetch odds (best-effort, don't block on failure)
        try:
            from flask import g as _g
            odds = get_full_odds_analysis(event, get_shotmap(match_id))
        except Exception:
            odds = None

        # 3. Check for manual Supabase override
        override = _supabase_get_seo_override(match_id)

        # 4. Build meta
        meta = _build_meta_tags(event, odds, override)
        # Use slug in canonical URL if available (better SEO)
        slug = event.get("slug", "")
        canonical = f"{SITE_URL}/match/{match_id}/{slug}" if slug else f"{SITE_URL}/match/{match_id}"

        # 5. Fetch base HTML and inject meta
        base_html = _get_base_html()
        if base_html:
            rendered = _inject_meta(base_html, meta, canonical)
            return rendered, 200, {"Content-Type": "text/html; charset=utf-8"}

        # Fallback: minimal HTML if Lovable is unreachable
        fallback = f"""<!DOCTYPE html>
<html lang="pt">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{meta['title']}</title>
  <meta name="description" content="{meta['description']}">
  <meta property="og:title" content="{meta['title']}">
  <meta property="og:description" content="{meta['description']}">
  <meta property="og:image" content="{meta['og_image']}">
  <meta property="og:url" content="{canonical}">
  <meta property="og:type" content="website">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{meta['title']}">
  <meta name="twitter:description" content="{meta['description']}">
  <link rel="canonical" href="{canonical}">
  <meta http-equiv="refresh" content="0;url={canonical}">
</head>
<body>
  <h1>{meta['title']}</h1>
  <p>{meta['description']}</p>
</body>
</html>"""
        return fallback, 200, {"Content-Type": "text/html; charset=utf-8"}

    except Exception as e:
        log.exception(f"[prerender] Error for match {match_id}: {e}")
        return "Internal error", 500


if __name__ == "__main__":
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
        threading.Thread(target=_background_loop, daemon=True).start()
        print(f"  Client: {_client_type}")
        print(f"  Odds API: enabled")
        print(f"  Background engine: every {BG_INTERVAL}s")
        print(f"  Team aliases: {len(_team_aliases)} loaded\n")
        app.run(host="0.0.0.0", port=5050, debug=True)
else:
    # Running under gunicorn — __main__ block is skipped, so initialize here
    _load_aliases()
    _init_db()
    threading.Thread(target=_init_client, daemon=True).start()
    threading.Thread(target=_background_loop, daemon=True).start()
