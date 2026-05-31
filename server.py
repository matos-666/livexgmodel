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

from flask import Flask, jsonify, request as flask_request, Response, redirect
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
    "fb426ae753559bf14d44cdc4f8cb68f7",  # fresh — added 2026-05-22
    "1bfe41fcc930a851627b2ac9ab19656a",  # fresh — added 2026-05-22
    "33b4d68e6b059cf32c1d8e661330e23f",  # fresh — added 2026-05-21
    "23b0d9f3cc94088301a62a55c7f51294",  # fresh — added 2026-05-21
    "e4b971151c23a4af2cf685fc2e22834a",  # fresh — added 2026-05-21
    "1c35b43e758d4646338141dc0205ce6d",  # fresh — added 2026-05-21
    "549fadd572fc1d53a57555a020fba953",  # fresh — added 2026-05-20
    "173199657160c9c0823710ddc47529e7",  # fresh — added 2026-05-20
    "1347b523a46f1acc1d06841cd5124f06",  # fresh — added 2026-05-19
    "c7587f4db8b8429c106dc400df29d314",  # fresh — added 2026-05-19
    "aa5ada68df5e9dcd732b3496cc39bb40",  # fresh — added 2026-05-18
    "a4efb0259b6eb7efb0899cd2ecdc7456",  # fresh — added 2026-05-18
    "c5b879c935c70f05499945763bfa683c",  # fresh — added 2026-05-17
    "79e8cf289ad94878e2dd3290ffd6c246",  # fresh — added 2026-05-17
    "a6f9fe4bcc03446252da3b6b4b8dde42",  # fresh — added 2026-05-17
    "54a9e36a8665a23e48b8914c50d67529",  # fresh — added 2026-05-17
    "cd61aac1e29abe01674694115e31212c",  # fresh — added 2026-05-17
    "3d1d3a38c606943f6814759d4b093deb",  # fresh — added 2026-05-17
    "5e99f2387c71d01a09ca59ba2aa87e95",  # fresh — added 2026-05-17
    "64ad973751ea7bd8d367f3301ace0fa8",  # fresh — added 2026-05-17
    "4de0149bd0bc9956eb1418abe35b6cfa",  # fresh — added 2026-05-16
    "d6e71a0d9b18968ffc6c89247dfd4c58",  # fresh — added 2026-05-16
    "17ea05e226230e41723bf61690406714",  # fresh — added 2026-05-16
    "6b193b6456c28bbaf36a933ac0b51140",  # fresh — added 2026-05-16
    "92fc15c6c973dbaf138b936a118633bb",  # fresh — added 2026-05-16
    "53b70360a662ce3ae547b54f6885f1a8",  # fresh — added 2026-05-15
    "f5c6a681c73b318f3315d0be2b7dcc69",  # fresh — added 2026-05-15
    "b249fc6d0ffff3957555804937c827a6",  # fresh — added 2026-05-15
    "fbb9cbb2c7b57bd2c7249194cb8820a3",  # fresh — added 2026-05-15
    "a1b39abfff67accdfaf3c2adb167e685",  # fresh — added 2026-05-14
    "e85703a4e55ec785b5d1af5491e58025",  # fresh — added 2026-05-14
    "5094ec093e7ba2417b13f97b28c8c31e",  # fresh — added 2026-05-13
    "47584ba207a1c72c88c1c8ab031c1679",  # fresh — added 2026-05-13
    "482e6787f85ae37796b502840e057623",  # fresh — added 2026-05-13
    "e0b7fcc3feab9018c6e0b3fd2442ed6e",  # fresh — added 2026-05-13
    "54c7797847ee5b03f71be6f1da9c537f",  # fresh — added 2026-05-12
    "53ac35f5683fbe127c281805a898ae10",  # fresh — added 2026-05-12
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

# ── Telegram CTAs — rotated on every pick alert ───────────────────────────
# Each CTA links to /go/bet on webpronos, which renders the smart-loader
# interstitial (PT-friendly bookmaker comparison) and auto-redirects to
# the rotating LEON/TWIN affiliate. URL is built per-call so we can
# inject match_id/market/label/odd/aff dynamically.
#
# 8 phrasings, all framed around "compare odds → find best price" so the
# CTA matches what the interstitial actually does (no bait & switch).
_TG_CTA_PHRASES = [
    "🔍 Comparar e abrir na melhor casa",
    "💰 Ver onde a odd está mais alta",
    "📊 Comparar 5 casas e abrir a melhor",
    "🏆 Apostar ao melhor preço",
    "🎯 Encontrar a odd mais alta agora",
    "💎 Ver melhor cotação para esta pick",
    "⚡ Comparar odds — abrir a vencedora",
    "🚀 Abrir na casa com melhor preço",
]
# Affiliate rotation — alternates between LEON and TWIN per pick so both
# campaigns get coverage. Kept as a list (not a generator) so the lock-
# based round-robin below stays simple.
_TG_AFFILIATE_ROTATION = ["leon", "twin"]

# Public base URL for the smart-loader interstitial. Worker on
# webpronos.com proxies /go/* straight to Flask (no Lovable cache).
_GO_BET_BASE = "https://webpronos.com/go/bet"


# ════════════════════════════════════════════════════════════
#  X (Twitter) AUTO-POSTING BOT
# ════════════════════════════════════════════════════════════
#
# Posts to @WebPronosAI when our algorithm fires a value pick.
# Goals:
#   - Build a public, auditable track record on X (every pick visible,
#     every result replied in-thread within 24h of settlement).
#   - Drive qualified traffic to webpronos.com via the link in each tweet.
#   - Volume: ~5-15 tweets/day at edge ≥ 8% threshold.
#
# Disabled gracefully if X_API_KEY is not set (lets dev/staging deploys
# run without the secret). All errors are caught and logged — Twitter
# downtime never blocks pick generation, SSE fan-out, or Telegram.
#
# Secrets (all 5 required for posting; the 5th is the bearer token used
# only for read endpoints we don't currently use):
#   X_API_KEY              — Consumer Key (OAuth 1.0a)
#   X_API_SECRET           — Consumer Secret
#   X_ACCESS_TOKEN         — Access Token for @WebPronosAI account
#   X_ACCESS_TOKEN_SECRET  — Access Token Secret
#   X_BEARER_TOKEN         — App-only auth (unused in v1)
#
# Tuning knobs:
#   X_EDGE_THRESHOLD       — float, default 8.0 (only tweet picks ≥ this %)
#   X_BOT_ENABLED          — "1" to enable, anything else disables (default "1")
#   X_DRY_RUN              — "1" logs the tweet instead of posting (test mode)
X_API_KEY              = os.environ.get("X_API_KEY", "")
X_API_SECRET           = os.environ.get("X_API_SECRET", "")
X_ACCESS_TOKEN         = os.environ.get("X_ACCESS_TOKEN", "")
X_ACCESS_TOKEN_SECRET  = os.environ.get("X_ACCESS_TOKEN_SECRET", "")
X_BEARER_TOKEN         = os.environ.get("X_BEARER_TOKEN", "")
X_EDGE_THRESHOLD       = float(os.environ.get("X_EDGE_THRESHOLD", "8.0"))
X_BOT_ENABLED          = os.environ.get("X_BOT_ENABLED", "1") == "1"
X_DRY_RUN              = os.environ.get("X_DRY_RUN", "0") == "1"

# Rate-limit guards — hard caps to prevent runaway tweeting on bad days
# (model glitches, mass-pick events) and to keep cost predictable
# (each tweet ~$0.015 plain / $0.20 with URL — we keep tweets URL-free).
# All reads count rows from x_tweets DB inside a process-wide lock so the
# atomic slot reservation can't be raced.
#
# Tier-of-defense:
#   1. EDGE_THRESHOLD     — only highest-conviction picks make it through (45%)
#   2. MAX_PER_MATCH      — avoid spamming one game (2)
#   3. MAX_PER_MARKET_PM  — same market on same match never tweets twice (1)
#   4. MAX_PER_HOUR       — rate-spreading: 1 tweet per hour max so the
#                            timeline reads as steady drumbeat, not bursts
#   5. MAX_PER_DAY        — absolute daily ceiling: 5 tweets/24h
#                            ⇒ cost ceiling = 5 picks × 2 (reply) × $0.015 = ~$0.15/day = $4.50/month
X_MAX_PER_HOUR             = int(os.environ.get("X_MAX_PER_HOUR", "1"))
X_MAX_PER_DAY              = int(os.environ.get("X_MAX_PER_DAY", "5"))
X_MAX_PER_MATCH            = int(os.environ.get("X_MAX_PER_MATCH", "2"))
X_MAX_PER_MARKET_PER_MATCH = int(os.environ.get("X_MAX_PER_MARKET_PER_MATCH", "1"))
X_DEDUP_WINDOW_SEC         = int(os.environ.get("X_DEDUP_WINDOW_SEC", "60"))


def _x_configured() -> bool:
    """True iff all 4 OAuth 1.0a secrets are set. Bearer token is optional."""
    return bool(X_API_KEY and X_API_SECRET and X_ACCESS_TOKEN and X_ACCESS_TOKEN_SECRET)


_x_client_singleton = None
_x_client_lock = threading.Lock()


def _x_client():
    """Lazy-init the Tweepy v2 client for the @WebPronosAI account.

    Returns None if secrets are missing or tweepy fails to import — the
    caller treats that as 'X disabled' and skips silently.
    """
    global _x_client_singleton
    if _x_client_singleton is not None:
        return _x_client_singleton
    if not (_x_configured() and X_BOT_ENABLED):
        return None
    try:
        import tweepy  # type: ignore
    except ImportError:
        log.warning("tweepy not installed — X bot disabled")
        return None
    with _x_client_lock:
        if _x_client_singleton is not None:
            return _x_client_singleton
        try:
            client = tweepy.Client(
                consumer_key        = X_API_KEY,
                consumer_secret     = X_API_SECRET,
                access_token        = X_ACCESS_TOKEN,
                access_token_secret = X_ACCESS_TOKEN_SECRET,
                bearer_token        = X_BEARER_TOKEN or None,
                wait_on_rate_limit  = False,  # we handle our own limits via DB
            )
            _x_client_singleton = client
            log.info("X bot client initialised for @WebPronosAI")
            return client
        except Exception as e:
            log.error(f"X bot init failed: {e}")
            return None

_tg_cta_counter = 0
_tg_cta_lock = threading.Lock()


def _create_short_link(target_url: str, source: str = "",
                        path: str = "r") -> str:
    """Insert a row into short_links and return the public short URL.

    Args:
        target_url — the long URL we want to wrap
        source     — free-form tag, stored for analytics
        path       — URL path component, defaults to "r" (generic).
                      Pass "betradar" for Telegram BetRadar AI CTAs so the
                      "Open link?" confirmation modal shows
                      webpronos.com/betradar/<code> — brand-aligned with
                      the bot name without buying a new domain.
    """
    import secrets
    for _ in range(8):   # retry on collision (cosmically unlikely with 6 chars)
        code = secrets.token_urlsafe(5)[:6]
        try:
            with _db() as conn:
                conn.execute(
                    "INSERT INTO short_links (code, target_url, created_at, source) "
                    "VALUES (?, ?, ?, ?)",
                    (code, target_url, int(time.time()), source or None)
                )
            return f"https://webpronos.com/{path}/{code}"
        except sqlite3.IntegrityError:
            continue
        except Exception as e:
            log.warning(f"_create_short_link insert failed: {e}; falling back to long URL")
            return target_url
    log.warning("_create_short_link: 8 collisions in a row, returning long URL")
    return target_url


def _next_cta(odds: float = None, stake: float = 100.0,
              match_id: int = 0, market: str = "",
              label: str = "") -> tuple[str, str]:
    """
    Return (phrase, short_url) for the next Telegram CTA. Rotates both
    the phrasing (8 variations) and the affiliate destination (LEON ↔
    TWIN). The short_url points to /r/<code> which 302s to the real
    /go/bet URL — keeps the Telegram "Open this link?" confirmation
    modal looking clean.

    Args:
        odds, stake — unused now (kept for backward-compat with callers)
        match_id, market, label — interpolated into the /go/bet URL so the
          interstitial knows which pick to display in its sub-line.
    """
    from urllib.parse import urlencode
    global _tg_cta_counter
    with _tg_cta_lock:
        n = _tg_cta_counter
        _tg_cta_counter += 1

    phrase = _TG_CTA_PHRASES[n % len(_TG_CTA_PHRASES)]
    aff    = _TG_AFFILIATE_ROTATION[n % len(_TG_AFFILIATE_ROTATION)]

    qs = {
        "aff":      aff,
        "match_id": int(match_id or 0),
        "market":   market or "",
        "label":    label or "",
        "odd":      f"{(odds or 0):.2f}",
        "lang":     "pt-pt",
        "source":   "telegram-betradar",
    }
    full_url  = f"{_GO_BET_BASE}?{urlencode(qs)}"
    # Use the branded /betradar/<code> path so the Telegram "Open link?"
    # confirmation modal reads webpronos.com/betradar/... — much more
    # on-brand for the BetRadar AI bot than a generic /r/<code>.
    short_url = _create_short_link(full_url, source="telegram-betradar",
                                     path="betradar")
    return (phrase, short_url)

_COUNTRY_FLAGS = {
    "england": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "spain": "🇪🇸", "italy": "🇮🇹", "germany": "🇩🇪",
    "france": "🇫🇷", "portugal": "🇵🇹", "netherlands": "🇳🇱", "belgium": "🇧🇪",
    "turkey": "🇹🇷", "scotland": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "austria": "🇦🇹", "switzerland": "🇨🇭",
    "russia": "🇷🇺", "ukraine": "🇺🇦", "greece": "🇬🇷", "poland": "🇵🇱",
    "czech republic": "🇨🇿", "denmark": "🇩🇰", "sweden": "🇸🇪", "norway": "🇳🇴",
    "usa": "🇺🇸", "brazil": "🇧🇷", "argentina": "🇦🇷", "mexico": "🇲🇽",
    "colombia": "🇨🇴", "chile": "🇨🇱", "japan": "🇯🇵", "south korea": "🇰🇷",
    "australia": "🇦🇺", "china": "🇨🇳",
    "saudi arabia": "🇸🇦", "uae": "🇦🇪", "qatar": "🇶🇦", "iran": "🇮🇷",
    "ireland": "🇮🇪", "wales": "🏴󠁧󠁢󠁷󠁬󠁳󠁿", "northern ireland": "🇬🇧",
    "finland": "🇫🇮", "iceland": "🇮🇸", "romania": "🇷🇴", "hungary": "🇭🇺",
    "croatia": "🇭🇷", "serbia": "🇷🇸", "slovenia": "🇸🇮", "slovakia": "🇸🇰",
    "bulgaria": "🇧🇬", "israel": "🇮🇱", "cyprus": "🇨🇾",
    "uruguay": "🇺🇾", "paraguay": "🇵🇾", "peru": "🇵🇪", "ecuador": "🇪🇨",
    "venezuela": "🇻🇪", "bolivia": "🇧🇴", "canada": "🇨🇦",
    "south africa": "🇿🇦", "morocco": "🇲🇦", "egypt": "🇪🇬", "nigeria": "🇳🇬",
    "india": "🇮🇳", "thailand": "🇹🇭", "vietnam": "🇻🇳", "indonesia": "🇮🇩",
    "malaysia": "🇲🇾",
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

def _send_telegram(text: str, chat_id=None, buttons=None):
    """Send a message via Telegram Bot API. If chat_id is None, sends to
    all subscribers. Optional `buttons` is an inline_keyboard (list of
    rows of {text, url} or {text, callback_data} dicts) attached as
    reply_markup."""
    if not TELEGRAM_BOT_TOKEN:
        return
    import urllib.request as _urllib
    ids = [str(chat_id)] if chat_id else _tg_subscribers()
    for cid in ids:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            body = {
                "chat_id": cid,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if buttons:
                body["reply_markup"] = {"inline_keyboard": buttons}
            payload = json.dumps(body).encode()
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

        # Only send the DAILY recap if lucro > €25. Otherwise (day was
        # flat or losing) try the monthly fallback — same time slot, same
        # animation style, but the window is the month-to-date and we
        # only send if THAT total is in profit. Keeps the bot useful on
        # quiet/down days without spamming losses.
        if not force_send and lucro <= 25:
            log.info(f"_send_daily_summary: lucro €{lucro:.2f} below threshold (€25) for {days_back} day(s) ago — trying monthly fallback")
            try:
                _send_monthly_summary_if_profitable(now_lisbon=now_lisbon)
            except Exception as e:
                log.error(f"_send_monthly_summary_if_profitable failed: {e}")
            return

        # Calculate average odds and ROI
        avg_odds = odds_sum / settled if settled > 0 else 0.0
        roi = (lucro / (settled * STAKE) * 100) if settled > 0 else 0.0

        # Find the WIN with the highest odd — the day's biggest hit.
        # User clarification (2026-05-11): "Maior Odd do Dia" must be the
        # biggest GREEN of the day, not just the highest odds overall (a
        # losing bet with 3.85 odds isn't a "maior odd" highlight, it's
        # a -€100 line). If there are no wins, skip this section entirely.
        # NOTE: tips are sqlite3.Row objects — no .get(), use indexing
        def _row_get(row, key, default=""):
            try:
                v = row[key]
                return v if v is not None else default
            except (IndexError, KeyError):
                return default

        winning_tips = [t for t in tips if (t["result"] or "").lower() in ("green", "win")]
        biggest_win = max(winning_tips, key=lambda t: t["odd_entry"] or 0) if winning_tips else None

        biggest_win_block = []
        if biggest_win:
            bw_odd    = biggest_win["odd_entry"] or 0
            bw_label  = _row_get(biggest_win, 'label', '?')
            bw_market = _row_get(biggest_win, 'market', '?')
            bw_home   = _row_get(biggest_win, 'home_team', '')
            bw_away   = _row_get(biggest_win, 'away_team', '')
            bw_match  = f"{bw_home} vs {bw_away}" if bw_home and bw_away else "Match desconhecido"
            bw_profit = (bw_odd - 1) * STAKE
            biggest_win_block = [
                "",
                f"🎯 <b>Maior Odd do Dia:</b> {bw_odd:.2f}",
                f"   <i>{_localize_pick_label(bw_label)} ({bw_market})</i>",
                f"   <i>{bw_match}</i>",
                f"   💰 Lucro gerado: <b>+€{bw_profit:.2f}</b>",
            ]

        # Format the message with match context and encouragement
        date_str = target_start.strftime("%d/%m/%Y")
        day_label = "Ontem" if days_back == 1 else ("Hoje" if days_back == 0 else f"{days_back}d atrás")

        msg_lines = [
            f"<b>Resumo Diário — {day_label} ({date_str})</b>",
            "",
            f"💶 <b>Lucro:</b> €{lucro:.2f}",
            f"📊 <b>Odds Médias:</b> {avg_odds:.2f}",
            f"📈 <b>ROI:</b> {roi:.1f}%",
            *biggest_win_block,
            "",
            f"<i>Mantém a vigilância nas entradas de amanhã — o edge está lá! 🚀</i>",
        ]

        msg = "\n".join(msg_lines)

        log.info(f"_send_daily_summary: sending message for {day_label} ({date_str}), lucro €{lucro:.2f}")

        # Try to attach the animated P&L recap. Falls back to plain text if
        # the MP4 build fails for any reason (ffmpeg missing, no settled
        # tips for the day in the recap loader, etc.) — delivery must
        # never be blocked by the visual.
        anim_bytes = None
        try:
            sys.path.insert(0, os.path.dirname(__file__))
            from tools.build_daily_recap import build_daily_recap  # type: ignore
            out_path = f"/tmp/daily_recap_{target_date.strftime('%Y-%m-%d')}.mp4"
            result = build_daily_recap(
                target_start_ts=target_start_ts,
                target_end_ts=target_end_ts,
                date_label=date_str,
                out_path=out_path,
                db_path=str(DB_PATH),
            )
            actual_path = result.split(" (", 1)[0] if isinstance(result, str) else out_path
            with open(actual_path, "rb") as fh:
                anim_bytes = fh.read()
            filename = os.path.basename(actual_path)
        except Exception as e:
            log.warning(f"_send_daily_summary: animation build failed ({e}); falling back to text-only")
            anim_bytes = None

        subscribers = _tg_subscribers() or []
        if anim_bytes:
            chat_ids = [int(c) for c in subscribers if str(c).lstrip("-").isdigit()]
            stats = _broadcast_telegram_animation(
                chat_ids, anim_bytes, caption=msg,
                buttons=_betradar_share_buttons(),
                filename=filename,
            )
            log.info(
                f"_send_daily_summary: animation broadcast — "
                f"upload {stats['sent_with_upload']}, "
                f"by_id {stats['sent_with_id']}, "
                f"failed {stats['failed']} (file_id={stats['file_id']})"
            )
        else:
            _send_telegram(msg)

    except Exception as e:
        log.error(f"_send_daily_summary error: {e}", exc_info=True)


def _send_monthly_summary_if_profitable(now_lisbon=None, force_send: bool = False):
    """Public wrapper — kicks off the real work in a daemon thread so the
    caller (HTTP request / APScheduler tick) returns immediately. The
    monthly animation build takes ~3 minutes (~1000 picks vs ~60 for the
    daily) and would otherwise blow past gunicorn's 30s worker timeout.
    """
    t = threading.Thread(
        target=_send_monthly_summary_if_profitable_blocking,
        kwargs={"now_lisbon": now_lisbon, "force_send": force_send},
        daemon=True,
        name="monthly-recap-worker",
    )
    t.start()


def _send_monthly_summary_if_profitable_blocking(now_lisbon=None, force_send: bool = False):
    """Fallback for flat/losing days: send a month-to-date recap, but ONLY
    when the month is in profit. Same Telegram animation style as the
    daily recap, just with a 'RESUMO MENSAL' header and the window set
    to [1st of current month 00:00 .. now].

    Dedup'd via the monthly_summary_locks table so a month can only be
    broadcast once per day even if _send_daily_summary fires multiple
    times (e.g. retries). The same month CAN re-broadcast on a later
    day if it's still profitable and that day is also flat — by design,
    keeps the channel alive on quiet stretches.
    """
    try:
        from datetime import datetime, timedelta
        lisbon_tz = pytz.timezone('Europe/Lisbon')
        if now_lisbon is None:
            now_lisbon = datetime.now(lisbon_tz)

        month_start = datetime(now_lisbon.year, now_lisbon.month, 1, 0, 0, 0, tzinfo=lisbon_tz)
        month_start_ts = int(month_start.timestamp())
        # End window = end of TODAY (so the animation includes anything
        # settled today). Picks settled tomorrow won't be in here.
        today_end = datetime(now_lisbon.year, now_lisbon.month, now_lisbon.day,
                              23, 59, 59, tzinfo=lisbon_tz)
        month_end_ts = int(today_end.timestamp())

        STAKE = get_setting("stake_per_bet", 100.0)

        # Per-day dedup so we don't re-broadcast the monthly recap if
        # _send_daily_summary fires more than once on the same day.
        today_key = now_lisbon.strftime("%Y-%m-%d")
        if not force_send:
            try:
                with _db() as conn:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS monthly_summary_locks (
                            day TEXT PRIMARY KEY,
                            sent_at INTEGER NOT NULL,
                            month_profit REAL
                        )
                    """)
                    try:
                        conn.execute(
                            "INSERT INTO monthly_summary_locks (day, sent_at) VALUES (?, ?)",
                            (today_key, int(time.time()))
                        )
                    except sqlite3.IntegrityError:
                        log.info(f"_send_monthly_summary: already sent for {today_key}, skipping")
                        return
            except Exception as e:
                log.warning(f"_send_monthly_summary: lock init failed: {e}")

        with _db() as conn:
            tips = conn.execute(
                "SELECT t.result, t.odd_entry, t.label, t.market, t.match_id, "
                "       g.home_team, g.away_team "
                "FROM tips t "
                "LEFT JOIN games g ON g.id = t.match_id "
                "WHERE t.wall_ts >= ? AND t.wall_ts <= ? AND t.result IS NOT NULL "
                "ORDER BY t.odd_entry DESC",
                (month_start_ts, month_end_ts)
            ).fetchall()

        if not tips:
            log.info(f"_send_monthly_summary: no settled tips for {now_lisbon.strftime('%Y-%m')}")
            # Roll back the lock so we can retry tomorrow.
            try:
                with _db() as conn:
                    conn.execute("DELETE FROM monthly_summary_locks WHERE day = ?", (today_key,))
            except Exception:
                pass
            return

        lucro = 0.0
        odds_sum = 0.0
        wins = losses = 0
        for tip in tips:
            r = (tip["result"] or "").lower()
            oe = tip["odd_entry"] or 0
            if r in ("green", "win") and oe:
                lucro += (oe - 1) * STAKE
                odds_sum += oe; wins += 1
            elif r in ("red", "loss"):
                lucro -= STAKE
                odds_sum += oe; losses += 1
        settled = wins + losses

        # Gate: only fire when the month is genuinely profitable. €25
        # mirrors the daily threshold — a month barely positive isn't
        # newsworthy.
        if not force_send and lucro <= 25:
            log.info(f"_send_monthly_summary: month profit €{lucro:.2f} below €25 — skipping")
            try:
                with _db() as conn:
                    conn.execute("DELETE FROM monthly_summary_locks WHERE day = ?", (today_key,))
            except Exception:
                pass
            return

        avg_odds = odds_sum / settled if settled else 0.0
        roi = (lucro / (settled * STAKE) * 100) if settled else 0.0

        # Biggest win of the month (same logic as daily).
        def _row_get(row, key, default=""):
            try:
                v = row[key]; return v if v is not None else default
            except (IndexError, KeyError):
                return default

        winning_tips = [t for t in tips if (t["result"] or "").lower() in ("green", "win")]
        biggest_win = max(winning_tips, key=lambda t: t["odd_entry"] or 0) if winning_tips else None
        biggest_win_block = []
        if biggest_win:
            bw_odd    = biggest_win["odd_entry"] or 0
            bw_label  = _row_get(biggest_win, 'label', '?')
            bw_market = _row_get(biggest_win, 'market', '?')
            bw_home   = _row_get(biggest_win, 'home_team', '')
            bw_away   = _row_get(biggest_win, 'away_team', '')
            bw_match  = f"{bw_home} vs {bw_away}" if bw_home and bw_away else "Match desconhecido"
            bw_profit = (bw_odd - 1) * STAKE
            biggest_win_block = [
                "",
                f"🎯 <b>Maior Odd do Mês:</b> {bw_odd:.2f}",
                f"   <i>{_localize_pick_label(bw_label)} ({bw_market})</i>",
                f"   <i>{bw_match}</i>",
                f"   💰 Lucro gerado: <b>+€{bw_profit:.2f}</b>",
            ]

        # Header chip uses month name in Portuguese.
        pt_months = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                     "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
        month_label_chip = f"{pt_months[now_lisbon.month-1]} {now_lisbon.year}"
        msg_lines = [
            f"<b>Resumo do Mês — {month_label_chip}</b>",
            "",
            f"💶 <b>Lucro acumulado:</b> €{lucro:.2f}",
            f"📊 <b>Odds Médias:</b> {avg_odds:.2f}",
            f"📈 <b>ROI:</b> {roi:.1f}%",
            f"✅ <b>Picks:</b> {settled} · {wins}V — {losses}P",
            *biggest_win_block,
            "",
            f"<i>Mês positivo até hoje — o edge continua a entregar. 🚀</i>",
        ]
        msg = "\n".join(msg_lines)

        log.info(f"_send_monthly_summary: sending for {month_label_chip}, lucro €{lucro:.2f}")

        # Render a STATIC PNG (not the animated MP4 used by the daily).
        # The monthly window has ~1000 picks vs ~60 daily, so FuncAnimation
        # blows past Fly's 1 GB memory cap AND gunicorn's 30s timeout.
        # The chart shape and stats are identical to the daily's final
        # frame — informationally equivalent, just not animated.
        png_bytes = None
        try:
            sys.path.insert(0, os.path.dirname(__file__))
            from tools.build_daily_recap import build_static_recap  # type: ignore
            out_path = f"/tmp/monthly_recap_{now_lisbon.strftime('%Y-%m')}.png"
            result = build_static_recap(
                target_start_ts=month_start_ts,
                target_end_ts=month_end_ts,
                date_label=month_label_chip,
                out_path=out_path,
                db_path=str(DB_PATH),
                header_label="RESUMO MENSAL",
            )
            actual_path = result.split(" (", 1)[0] if isinstance(result, str) else out_path
            with open(actual_path, "rb") as fh:
                png_bytes = fh.read()
        except Exception as e:
            log.warning(f"_send_monthly_summary: chart build failed ({e}); falling back to text-only")
            png_bytes = None

        subscribers = _tg_subscribers() or []
        chat_ids = [int(c) for c in subscribers if str(c).lstrip("-").isdigit()]
        if png_bytes and chat_ids:
            sent = 0
            failed = 0
            buttons = _betradar_share_buttons()
            for cid in chat_ids:
                try:
                    _send_telegram_photo(cid, png_bytes, caption=msg, buttons=buttons)
                    sent += 1
                except Exception as e:
                    failed += 1
                    log.warning(f"_send_monthly_summary: photo to {cid} failed: {e}")
            log.info(f"_send_monthly_summary: photo broadcast — sent {sent}, failed {failed}")
        elif chat_ids:
            # Photo build failed → text-only fallback to every subscriber.
            for cid in chat_ids:
                try:
                    _send_telegram(msg, chat_id=cid)
                except Exception:
                    pass
            log.info(f"_send_monthly_summary: text-only fallback sent to {len(chat_ids)} chat(s)")
        else:
            log.warning("_send_monthly_summary: no subscribers configured")

    except Exception as e:
        log.error(f"_send_monthly_summary_if_profitable error: {e}", exc_info=True)


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


def _send_telegram_animation(chat_id: int, animation_bytes: bytes,
                              caption: str = "", buttons: list | None = None,
                              filename: str = "recap.mp4") -> str | None:
    """Envia animação via Telegram sendAnimation. Aceita GIF ou MP4 (H.264).
    Content-Type derivado da extensão do filename — MP4 dá melhor qualidade
    pelo mesmo file size.

    Returns the Telegram-assigned file_id on success (str) or None on
    failure. The file_id can be reused with _send_telegram_animation_by_id
    to fan out the same animation to many chats without re-uploading.
    """
    if not TELEGRAM_BOT_TOKEN:
        return None
    import urllib.request as _urllib
    content_type = "video/mp4" if filename.lower().endswith(".mp4") else "image/gif"
    try:
        boundary = "TGBotBoundary7x3k"
        CRLF = b"\r\n"

        def field(name: str, value: str) -> bytes:
            return (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}"
            ).encode() + CRLF

        body = field("chat_id", str(chat_id)) + field("parse_mode", "HTML")
        if caption:
            body += field("caption", caption)
        if buttons:
            body += field("reply_markup", json.dumps({"inline_keyboard": buttons}))
        body += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="animation"; filename="{filename}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode() + animation_bytes + CRLF
        body += f"--{boundary}--\r\n".encode()

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendAnimation"
        req = _urllib.Request(url, data=body,
                              headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        with _urllib.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read())
        return ((payload.get("result") or {}).get("animation") or {}).get("file_id")
    except Exception as e:
        log.error(f"Telegram send_animation failed to {chat_id}: {e}")
        return None


def _send_telegram_animation_by_id(chat_id: int, file_id: str,
                                    caption: str = "",
                                    buttons: list | None = None) -> bool:
    """Forward a previously-uploaded animation by its Telegram file_id.

    Much cheaper than _send_telegram_animation: no file upload, just a
    small JSON POST. Use this for fan-out after the first chat has been
    served via the full multipart upload."""
    if not TELEGRAM_BOT_TOKEN or not file_id:
        return False
    import urllib.request as _urllib
    try:
        payload = {
            "chat_id":    str(chat_id),
            "animation":  file_id,
            "parse_mode": "HTML",
        }
        if caption:
            payload["caption"] = caption
        if buttons:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendAnimation"
        req = _urllib.Request(url, data=json.dumps(payload).encode(),
                              headers={"Content-Type": "application/json"})
        _urllib.urlopen(req, timeout=10)
        return True
    except Exception as e:
        log.error(f"Telegram send_animation_by_id failed to {chat_id}: {e}")
        return False


def _broadcast_telegram_animation(chat_ids: list, animation_bytes: bytes,
                                   caption: str = "",
                                   buttons: list | None = None,
                                   filename: str = "recap.mp4") -> dict:
    """Fan out one animation to many chats. Uploads the file ONCE to the
    first chat, then forwards the rest via Telegram's file_id reuse —
    O(1) bandwidth instead of O(N).
    Returns {sent_with_upload, sent_with_id, failed, file_id}."""
    if not chat_ids:
        return {"sent_with_upload": 0, "sent_with_id": 0, "failed": 0, "file_id": None}
    first, rest = chat_ids[0], chat_ids[1:]
    file_id = _send_telegram_animation(first, animation_bytes, caption=caption,
                                        buttons=buttons, filename=filename)
    sent_with_upload = 1 if file_id is not None else 0
    sent_with_id = 0
    failed = 0
    for cid in rest:
        if file_id and _send_telegram_animation_by_id(cid, file_id,
                                                       caption=caption,
                                                       buttons=buttons):
            sent_with_id += 1
        else:
            # First-send failed OR file_id wasn't returned → fall back to
            # full upload per chat. Rare path; just keep delivery working.
            if _send_telegram_animation(cid, animation_bytes, caption=caption,
                                         buttons=buttons, filename=filename):
                sent_with_upload += 1
            else:
                failed += 1
    return {
        "sent_with_upload": sent_with_upload,
        "sent_with_id":     sent_with_id,
        "failed":           failed,
        "file_id":          file_id,
    }


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

def _pick_narrative(market, label, home, away, hg, ag,
                    home_xg, away_xg, minute, model_p, market_p, edge):
    """Context-aware "value explanation" line for a pick.

    Rewritten 2026-05-21 after boss feedback that:
      1. "domina em xG" appeared when xG was tied or inferior (logic bug)
      2. "Falta(m) 1 golo(s)" — ugly placeholder pluralisation
      3. "Cobertura com colchão" used in every handicap regardless of fit
      4. Same exact phrasing every pick → readers get blind to the copy
    Fix: compute the actual scoreline + xG state, pick the scenario that
    matches it, then sample one variant from a pool of ≥3 phrasings (hash-
    seeded so the same pick is stable across re-fires but different picks
    vary). Falls back to the generic phrasing for unknown market types.
    """
    import re as _re_pick

    try:
        minute_int = int(str(minute).split('+')[0])
    except Exception:
        minute_int = 45
    remaining = max(1, 90 - minute_int)
    home_xg = float(home_xg or 0)
    away_xg = float(away_xg or 0)
    hg = int(hg or 0); ag = int(ag or 0)
    total_xg = home_xg + away_xg
    total_g  = hg + ag

    mk = (market or "").strip()
    lb = (label or "").strip()
    lb_low = lb.lower()
    edge_tag = f" (+<b>{edge:.1f}%</b> de valor)"

    # Deterministic-but-varied variant selection: hash combines the pick
    # identity (teams, market, label) and a *coarse* minute bucket. Same
    # pick re-emitted within ~5 min keeps the same phrasing (no flicker
    # in the same Telegram alert) but a new pick on a new game gets a
    # different one. Different scenarios (e.g. dominant vs tied xG) have
    # their own pools so the choice is always semantically correct.
    _variant_seed = abs(hash(f"{home}|{away}|{mk}|{lb}|{minute_int // 5}"))
    def _pick_variant(pool: list[str]) -> str:
        return pool[_variant_seed % len(pool)]

    # Smart PT-BR pluralisation. n=1 → singular, otherwise plural.
    def _plur_pt(n: int, sing: str, plur: str) -> str:
        return sing if abs(n) == 1 else plur

    # xG-difference state machine. Thresholds chosen empirically so
    # "tied" feels intuitive (≤ 0.2 swing) and "dominant" is a clear
    # gap (≥ 0.5). Used by every market branch below.
    def _xg_state(t_xg: float, o_xg: float) -> str:
        diff = t_xg - o_xg
        if diff >  0.5:  return "dominant"
        if diff >  0.2:  return "edging"
        if diff > -0.2:  return "tied"
        if diff > -0.5:  return "edged_against"
        return "outclassed"

    # ───────── Totals: Over X.5 / Under X.5 ─────────
    if mk in ("Totals", "Over/Under") or mk.startswith("O/U"):
        m = _re_pick.search(r"(\d+\.?\d*)", lb)
        line = float(m.group(1)) if m else 2.5
        is_over = lb_low.startswith("over") or lb_low.startswith("mais")
        projected = total_xg * (90 / minute_int) if minute_int > 0 else total_xg

        if is_over:
            need = line - total_g
            if need <= 0:
                pool = [
                    f"💡 <b>Linha já partida:</b> {total_g} golos no marcador, só falta "
                    f"chegar ao apito final ({remaining}' por jogar).{edge_tag}",
                    f"💡 <b>Over já cumprido:</b> jogo com {total_g} golos antes do {minute_int}', "
                    f"o resto é só esperar pelo árbitro.{edge_tag}",
                    f"💡 <b>Conta arrumada:</b> {total_g} golos já metidos com {remaining}' "
                    f"ainda no relógio — Over {line:g} está fechado.{edge_tag}",
                ]
                return _pick_variant(pool)

            need_int = int(need) if need == int(need) else int(need) + 1
            golo_pt   = _plur_pt(need_int, "golo",   "golos")
            falta_pt  = _plur_pt(need_int, "Falta",  "Faltam")
            pool = [
                f"💡 <b>Ritmo de Over:</b> {total_xg:.1f} xG combinado em {minute_int}' "
                f"projeta {projected:.1f} no final da partida. {falta_pt} apenas "
                f"{need_int} {golo_pt} em {remaining}' para bater a linha de {line:g} "
                f"da casa.{edge_tag}",
                f"💡 <b>Jogo aberto:</b> {home} e {away} já produziram {total_xg:.1f} xG "
                f"aos {minute_int}' — passo médio para terminarem em ~{projected:.1f}. "
                f"{falta_pt} {need_int} {golo_pt} para a linha {line:g} cair.{edge_tag}",
                f"💡 <b>Projeção acima da linha:</b> {total_xg:.1f} xG em {minute_int}' "
                f"sugerem {projected:.1f} ao apito final — bem acima dos {line:g} pedidos "
                f"pela casa. {falta_pt} {need_int} {golo_pt} em {remaining}'.{edge_tag}",
            ]
            return _pick_variant(pool)

        # ── Under: pick scenario by game state ──────────────────────
        # Variables that distinguish scenarios:
        #   - goals_to_break = line - total_g  (golos que ainda partem o Under)
        #   - projected vs line gap
        #   - minute (late vs mid)
        #   - xG asymmetry (one side dominating but unable to convert)
        goals_to_break = line - total_g
        gtb_int = int(goals_to_break) if goals_to_break == int(goals_to_break) else int(goals_to_break) + 1
        xg_diff_abs = abs(home_xg - away_xg)
        dominant_side = home if home_xg > away_xg else away
        dom_xg, weak_xg = max(home_xg, away_xg), min(home_xg, away_xg)

        # CENÁRIO A — "Tempo a esgotar" (minuto alto, golos no marcador
        # próximos da linha mas com ritmo a abrandar). Foca-se no relógio.
        if minute_int >= 70:
            golo_pt  = _plur_pt(gtb_int, "golo",  "golos")
            falta_pt = _plur_pt(gtb_int, "falta", "faltam")
            pool = [
                f"💡 <b>Cronómetro a fechar Under:</b> {hg}-{ag} aos {minute_int}', "
                f"{falta_pt} mais {gtb_int} {golo_pt} para Under {line:g} cair. "
                f"xG total {total_xg:.1f} mostra que o ritmo já não dá em {remaining}'."
                f"{edge_tag}",
                f"💡 <b>Tempo a favor:</b> restam {remaining}' e a linha {line:g} ainda "
                f"precisa de {gtb_int} {golo_pt} para partir. Com apenas {total_xg:.1f} xG "
                f"em {minute_int}', dificilmente acontece.{edge_tag}",
                f"💡 <b>Jogo a esmorecer:</b> {hg}-{ag} aos {minute_int}', {falta_pt} "
                f"{gtb_int} {golo_pt} em {remaining}' para Under {line:g} cair — projeção "
                f"final ~{projected:.1f}, abaixo da linha da casa.{edge_tag}",
            ]
            return _pick_variant(pool)

        # CENÁRIO B — "Domínio estéril" (uma equipa cria xG significativo
        # mas sem rasgar). Score baixo apesar de uma equipa pressionar.
        # Sinaliza defesa sólida vs ataque ineficaz.
        if xg_diff_abs >= 0.8 and total_g <= line - 1:
            pool = [
                f"💡 <b>Pressão estéril:</b> {dominant_side} cria muito ({dom_xg:.1f} xG) "
                f"mas a defesa adversária aguenta — só {total_g} golos aos {minute_int}'. "
                f"Linha {line:g} parece longe para o ritmo real do jogo.{edge_tag}",
                f"💡 <b>Domínio sem rasgar:</b> {dominant_side} já produziu {dom_xg:.1f} xG "
                f"vs {weak_xg:.1f} do adversário e mesmo assim o marcador está em {hg}-{ag}. "
                f"Defesa montada para segurar Under {line:g}.{edge_tag}",
                f"💡 <b>Asfixia mas sem prémio:</b> jogo unilateral ({dom_xg:.1f}–"
                f"{weak_xg:.1f} xG) mas finalização inexistente. Projeção {projected:.1f} "
                f"insuficiente para passar {line:g}.{edge_tag}",
            ]
            return _pick_variant(pool)

        # CENÁRIO C — "Jogo travado / defensivo" (xG total baixo, sem
        # ritmo de nenhum lado). Cenário clássico de jogos pesados.
        pool = [
            f"💡 <b>Jogo travado:</b> apenas {total_xg:.1f} xG combinado em {minute_int}' "
            f"(projeção {projected:.1f}). {home} e {away} sem ritmo para passar a linha "
            f"{line:g} da casa nos {remaining}' restantes.{edge_tag}",
            f"💡 <b>Defesas no controlo:</b> {total_xg:.1f} xG total aos {minute_int}' — "
            f"poucos remates de qualidade, projeção {projected:.1f} fica abaixo de {line:g}."
            f"{edge_tag}",
            f"💡 <b>Ritmo lento:</b> {total_xg:.1f} xG até ao {minute_int}' aponta para "
            f"~{projected:.1f} no final. Linha {line:g} da casa parece exagerada para "
            f"o que se vê em campo.{edge_tag}",
        ]
        return _pick_variant(pool)

    # ───────── 1X2 ─────────
    if mk == "1X2":
        if lb_low in ("draw", "empate", "x"):
            xg_diff = abs(home_xg - away_xg)
            tied_score = (hg == ag)
            # Identify trailing team for "reaction expected" scenarios
            if hg < ag:
                trail, lead = home, away
                trail_xg, lead_xg = home_xg, away_xg
                trail_g, lead_g   = hg, ag
            elif ag < hg:
                trail, lead = away, home
                trail_xg, lead_xg = away_xg, home_xg
                trail_g, lead_g   = ag, hg
            else:
                trail = lead = ""
                trail_xg = lead_xg = 0.0
                trail_g  = lead_g  = 0

            # CENÁRIO A — "Cronómetro fecha empate" (score empatado +
            # minuto alto). Quanto menos tempo resta, mais provável o
            # empate ficar como está. Foca-se no relógio.
            if tied_score and minute_int >= 70:
                pool = [
                    f"💡 <b>Cronómetro a fechar o empate:</b> {hg}-{ag} aos {minute_int}' "
                    f"e nem {home} nem {away} estão a empurrar com força. {remaining}' "
                    f"não chegam para virar o jogo.{edge_tag}",
                    f"💡 <b>Cada um para o seu lado:</b> aos {minute_int}', {home} e "
                    f"{away} aceitam o empate ({hg}-{ag}). xG dos últimos minutos não "
                    f"justifica subida de risco no final.{edge_tag}",
                    f"💡 <b>Empate a desenhar-se:</b> {remaining}' por jogar e ambas as "
                    f"equipas a controlar — {total_xg:.1f} xG combinado em {minute_int}' "
                    f"diz que ninguém quer arriscar mais.{edge_tag}",
                ]
                return _pick_variant(pool)

            # CENÁRIO B — "Reação esperada" (score NÃO empatado, mas
            # equipa atrás está a criar mais → modelo aposta no golo
            # que iguala o jogo).
            if (not tied_score) and trail and trail_xg > lead_xg + 0.3:
                pool = [
                    f"💡 <b>Resposta no horizonte:</b> {trail} a perder ({trail_g}-{lead_g}) "
                    f"mas com {trail_xg:.1f} xG vs {lead_xg:.1f} de {lead}. Golo do empate "
                    f"é o desfecho mais provável nos {remaining}'.{edge_tag}",
                    f"💡 <b>Pressão para igualar:</b> {trail} produz ({trail_xg:.1f} xG vs "
                    f"{lead_xg:.1f}) mas continua atrás no marcador. Modelo a {model_p:.0f}% "
                    f"que o jogo acaba empatado.{edge_tag}",
                    f"💡 <b>Modelo vê viragem-mas-só-até-empate:</b> {trail} domina "
                    f"({trail_xg:.1f} xG) mas {lead} segura — golo da equipa atrás vem, "
                    f"o segundo dificilmente.{edge_tag}",
                ]
                return _pick_variant(pool)

            # CENÁRIO C — "Equilíbrio total" (score empatado + xG
            # equilibrado). Cenário clássico: jogo controlado, nenhuma
            # equipa cria distância. Empate pago acima do valor.
            pool = [
                f"💡 <b>Sem dono do jogo:</b> {home} {home_xg:.1f} xG vs {away} "
                f"{away_xg:.1f} xG aos {minute_int}' — equilíbrio que as casas estão "
                f"a subvalorizar.{edge_tag}",
                f"💡 <b>Equilíbrio confirmado:</b> aos {minute_int}' nenhuma equipa se "
                f"impõe ({home_xg:.1f}–{away_xg:.1f} em xG). Empate pago acima do "
                f"valor justo.{edge_tag}",
                f"💡 <b>Ninguém quer arriscar:</b> {home} e {away} aos {minute_int}' "
                f"com {total_xg:.1f} xG combinado, sem nenhum lado a empurrar — empate "
                f"é o desfecho mais provável.{edge_tag}",
            ]
            return _pick_variant(pool)

        if lb == home or lb_low in ("home", "1"):
            team, opp, t_xg, o_xg, t_g, o_g = home, away, home_xg, away_xg, hg, ag
        elif lb == away or lb_low in ("away", "2"):
            team, opp, t_xg, o_xg, t_g, o_g = away, home, away_xg, home_xg, ag, hg
        else:
            team, opp = lb, ""
            t_xg = o_xg = 0.0; t_g = o_g = 0

        state = _xg_state(t_xg, o_xg)

        # Team is LOSING the scoreline
        if t_g < o_g:
            if state in ("dominant", "edging"):
                pool = [
                    f"💡 <b>Reviravolta no horizonte:</b> {team} a perder ({t_g}-{o_g}) "
                    f"mas com {t_xg:.1f} xG vs {o_xg:.1f} de {opp} — a pressão está "
                    f"a montar nos {remaining}' que faltam.{edge_tag}",
                    f"💡 <b>Domínio sem prémio:</b> {team} já produziu mais "
                    f"({t_xg:.1f} xG a {o_xg:.1f}) mas continua atrás no marcador "
                    f"({t_g}-{o_g}). Tendência para corrigir no resto do jogo.{edge_tag}",
                    f"💡 <b>Pressão por prémio:</b> apesar do {t_g}-{o_g} contra, "
                    f"{team} é quem cria ({t_xg:.1f} vs {o_xg:.1f} xG) — modelo "
                    f"confia na resposta antes do 90'.{edge_tag}",
                ]
            else:  # tied or behind on xG — model still picks them, likely momentum/odds value
                pool = [
                    f"💡 <b>Ainda no jogo:</b> {team} atrás ({t_g}-{o_g}) e em xG "
                    f"({t_xg:.1f} vs {o_xg:.1f}), mas as odds caíram demais. "
                    f"Modelo vê valor mesmo sem domínio claro.{edge_tag}",
                    f"💡 <b>Odds esticadas:</b> mercado descartou {team} ao {minute_int}' "
                    f"({t_g}-{o_g}), mas faltam {remaining}' e o preço já não respeita "
                    f"a possibilidade de viragem.{edge_tag}",
                ]
            return _pick_variant(pool)

        # Team is LEADING the scoreline
        if t_g > o_g:
            if state in ("dominant", "edging"):
                pool = [
                    f"💡 <b>Vantagem merecida:</b> {team} à frente ({t_g}-{o_g}) e por "
                    f"cima em xG ({t_xg:.1f} a {o_xg:.1f}) — modelo dá {model_p:.0f}% "
                    f"de segurar até ao final.{edge_tag}",
                    f"💡 <b>Resultado a refletir o jogo:</b> {team} {t_g}-{o_g} com "
                    f"{t_xg:.1f} xG vs {o_xg:.1f} de {opp}. Pressão controlada, "
                    f"vantagem a confirmar nos {remaining}'.{edge_tag}",
                    f"💡 <b>No comando:</b> {team} lidera por {t_g}-{o_g} e domina "
                    f"o relatório de remates ({t_xg:.1f}–{o_xg:.1f} xG). Casas a "
                    f"subestimar a estabilidade.{edge_tag}",
                ]
            elif state == "tied":
                pool = [
                    f"💡 <b>Vantagem a defender:</b> {team} à frente ({t_g}-{o_g}) "
                    f"mas xG equilibrado ({t_xg:.1f}–{o_xg:.1f}) — modelo confia "
                    f"que o resultado aguenta os últimos {remaining}'.{edge_tag}",
                    f"💡 <b>Pequena janela ainda aberta:</b> {team} segura o {t_g}-{o_g} "
                    f"sem domínio claro, mas {opp} também não está a empurrar — "
                    f"casas pagam acima do risco real.{edge_tag}",
                    f"💡 <b>Resultado vulnerável que se mantém:</b> aos {minute_int}', "
                    f"{team} à frente sem distância em xG ({t_xg:.1f}–{o_xg:.1f}). "
                    f"Modelo vê valor em segurar.{edge_tag}",
                ]
            else:  # edged_against / outclassed but still leading
                pool = [
                    f"💡 <b>À frente sem merecer:</b> {team} segura o {t_g}-{o_g} apesar "
                    f"de {opp} produzir mais ({o_xg:.1f} vs {t_xg:.1f} xG). Modelo "
                    f"acredita que a defesa aguenta os {remaining}' restantes.{edge_tag}",
                    f"💡 <b>Vantagem por defender:</b> {team} ganha por {t_g}-{o_g} "
                    f"mas em xG está atrás ({t_xg:.1f}–{o_xg:.1f}). Casas pagam "
                    f"como se a queda fosse certa — modelo discorda.{edge_tag}",
                ]
            return _pick_variant(pool)

        # Tied score
        if state == "dominant":
            pool = [
                f"💡 <b>Empate enganador:</b> {team} já com {t_xg:.1f} xG vs "
                f"{o_xg:.1f} de {opp} sem premiar o domínio — o golo está perto."
                f"{edge_tag}",
                f"💡 <b>Pressão sem prémio:</b> aos {minute_int}', {team} produz "
                f"({t_xg:.1f} xG) bem mais que {opp} ({o_xg:.1f}) mas o marcador "
                f"continua igual. Tendência clara para o golo.{edge_tag}",
                f"💡 <b>Manda quem ainda não marcou:</b> {team} a empurrar com "
                f"{t_xg:.1f} xG (vs {o_xg:.1f}) — falta finalizar mas as casas já "
                f"deviam ter ajustado.{edge_tag}",
            ]
        elif state == "edging":
            pool = [
                f"💡 <b>Ligeira ascendência:</b> {team} aos {minute_int}' já com "
                f"{t_xg:.1f} xG, um pouco acima de {opp} ({o_xg:.1f}) — modelo "
                f"vê valor antes do mercado ajustar.{edge_tag}",
                f"💡 <b>Ritmo a favor:</b> {team} a tomar conta do jogo ({t_xg:.1f}–"
                f"{o_xg:.1f} xG) mesmo empatado no marcador. Casas ainda não ajustaram."
                f"{edge_tag}",
            ]
        else:  # tied xG and tied score
            pool = [
                f"💡 <b>Modelo aposta no impulso:</b> aos {minute_int}' o ritmo "
                f"favorece {team} e as casas não ajustaram o preço.{edge_tag}",
                f"💡 <b>Valor escondido em {team}:</b> jogo equilibrado, mas as odds "
                f"sobrevalorizam {opp} — modelo prefere o lado contrário.{edge_tag}",
            ]
        return _pick_variant(pool)

    # ───────── Handicap (Asian / European) ─────────
    if "handicap" in mk.lower():
        if home and home in lb:
            team, opp, t_xg, o_xg, t_g, o_g = home, away, home_xg, away_xg, hg, ag
        elif away and away in lb:
            team, opp, t_xg, o_xg, t_g, o_g = away, home, away_xg, home_xg, ag, hg
        else:
            team, opp = lb, ""
            t_xg = o_xg = 0.0; t_g = o_g = 0

        # Parse handicap value (e.g. "Team +1.5" or "Team -2")
        m = _re_pick.search(r"([+-]?\d+\.?\d*)", lb)
        hcp = float(m.group(1)) if m else 0.0
        is_defensive = hcp > 0  # +X protects the team
        state = _xg_state(t_xg, o_xg)

        if is_defensive:
            # Team has +X handicap — covering against losing by too much
            margin = (o_g - t_g) - hcp  # negative = currently covered
            if margin < 0 and state in ("dominant", "edging"):
                pool = [
                    f"💡 <b>Pressão para reduzir distância:</b> {team} com "
                    f"{t_xg:.1f} xG vs {o_xg:.1f} aos {minute_int}' — está a "
                    f"criar para fechar a diferença e cobrir {lb}.{edge_tag}",
                    f"💡 <b>Reação no radar:</b> {team} produz ({t_xg:.1f} xG, "
                    f"{o_xg:.1f} de {opp}) e o handicap {lb} dá margem confortável."
                    f"{edge_tag}",
                ]
            elif margin < 0:
                pool = [
                    f"💡 <b>Cobertura intacta:</b> {team} aguenta o resultado e tem "
                    f"folga no handicap {lb}. {t_xg:.1f}–{o_xg:.1f} xG sem sinais "
                    f"de cedência nos {remaining}'.{edge_tag}",
                    f"💡 <b>Distância sob controlo:</b> aos {minute_int}', "
                    f"{team} mantém o jogo perto ({t_g}-{o_g}) e o +{hcp:g} cobre "
                    f"sem stress.{edge_tag}",
                ]
            else:
                pool = [
                    f"💡 <b>Muralha defensiva:</b> {team} com {t_xg:.1f} xG sofrido "
                    f"vs {o_xg:.1f} de {opp} — defesa sólida, handicap {lb} parece "
                    f"abaixo do valor real.{edge_tag}",
                    f"💡 <b>Resistência montada:</b> aos {minute_int}', {team} "
                    f"limita {opp} a {o_xg:.1f} xG e protege o spread {lb}.{edge_tag}",
                ]
        else:
            # Team has -X handicap — needs to win by more
            needed = abs(hcp) - (t_g - o_g)  # positive = goals still needed
            if needed > 0 and state in ("dominant", "edging"):
                falta_n = int(needed) if needed == int(needed) else int(needed) + 1
                pool = [
                    f"💡 <b>Ritmo para alargar:</b> {team} domina ({t_xg:.1f}–"
                    f"{o_xg:.1f} xG) e precisa de mais {_plur_pt(falta_n, 'golo','golos')} "
                    f"em {remaining}' para fechar o handicap {lb}.{edge_tag}",
                    f"💡 <b>Domínio para concretizar:</b> {team} com {t_xg:.1f} xG "
                    f"vs {o_xg:.1f}, falta finalizar para cobrir {lb}. Modelo a "
                    f"{model_p:.0f}%.{edge_tag}",
                ]
            elif needed > 0:
                pool = [
                    f"💡 <b>Diferença ainda por fazer:</b> {team} {t_g}-{o_g} em "
                    f"{opp} mas precisa de alargar para cobrir {lb}. xG aos "
                    f"{minute_int}': {t_xg:.1f}–{o_xg:.1f}.{edge_tag}",
                ]
            else:
                # Already covering -X
                pool = [
                    f"💡 <b>Avanço suficiente:</b> {team} já cobre {lb} no marcador. "
                    f"xG aos {minute_int}': {t_xg:.1f}–{o_xg:.1f} — só tem de "
                    f"manter o controlo nos {remaining}'.{edge_tag}",
                    f"💡 <b>Spread garantido em xG:</b> {team} produziu {t_xg:.1f} "
                    f"vs {o_xg:.1f} de {opp} e segura o handicap {lb} confortavelmente."
                    f"{edge_tag}",
                ]
        return _pick_variant(pool)

    # ───────── BTTS ─────────
    if "btts" in mk.lower() or "both teams" in mk.lower():
        yes = "yes" in lb_low or "sim" in lb_low
        if yes:
            need = []
            if hg == 0: need.append(f"{home} ({home_xg:.1f} xG ainda sem marcar)")
            if ag == 0: need.append(f"{away} ({away_xg:.1f} xG ainda sem marcar)")
            if not need:
                pool = [
                    f"💡 <b>Garantido:</b> ambas equipas já marcaram.{edge_tag}",
                    f"💡 <b>Conta fechada:</b> {hg}-{ag} já tem ambas no marcador."
                    f"{edge_tag}",
                ]
                return _pick_variant(pool)
            pool = [
                f"💡 <b>Falta finalizar:</b> {' e '.join(need)} — pressão sem prémio, "
                f"modelo a {model_p:.0f}% que cai antes do 90'.{edge_tag}",
                f"💡 <b>Pressão por marcar:</b> {' e '.join(need)}. xG acumulado "
                f"sugere golo nos {remaining}'.{edge_tag}",
            ]
            return _pick_variant(pool)

        pool = [
            f"💡 <b>Defesas a controlar:</b> apenas {total_xg:.1f} xG total em "
            f"{minute_int}' — sem ritmo para ambas marcarem nos {remaining}'.{edge_tag}",
            f"💡 <b>Marcador travado:</b> {total_xg:.1f} xG combinado aos {minute_int}' "
            f"não convence — modelo a {model_p:.0f}% que pelo menos uma fica em branco."
            f"{edge_tag}",
        ]
        return _pick_variant(pool)

    # ───────── Fallback (mercado desconhecido) ─────────
    pool = [
        f"💡 <b>Vale a pena:</b> modelo vê <b>{model_p:.0f}%</b> de hipótese, "
        f"casa paga como se fossem <b>{market_p:.0f}%</b>{edge_tag}.",
        f"💡 <b>Desalinhamento de preço:</b> probabilidade real ~{model_p:.0f}%, "
        f"mas mercado a {market_p:.0f}%{edge_tag}.",
    ]
    return _pick_variant(pool)


def _format_pick_alert(match: dict, pick: dict, minute, shots: dict = None):
    """Build the Telegram message for a new pick.

    Returns (text, inline_keyboard) where inline_keyboard is a list of
    rows of buttons suitable for the `reply_markup` of sendMessage. The
    CTA is rendered as a proper Telegram button (not an inline <a>) so
    it stands out as the call-to-action and the URL is a short link.
    """
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
    match_id    = match.get("id") or match.get("matchId") or 0

    market_icons = {"1X2": "🎯", "Handicap": "⚖️"}
    mkt_icon = market_icons.get(market, "📊")

    stake = get_setting("stake_per_bet", 100.0)
    cta_phrase, cta_url = _next_cta(odds=odds, stake=stake, match_id=match_id,
                                     market=market, label=label)

    # Context-aware narrative: references teams, live xG, market dynamic.
    # Falls back to the generic "modelo X% vs casa Y%" only for unknown
    # market types.
    value_line = _pick_narrative(
        market=market, label=label, home=home, away=away,
        hg=hg, ag=ag, home_xg=home_xg, away_xg=away_xg,
        minute=minute, model_p=model_p, market_p=market_p, edge=edge,
    )

    text = (
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
        f"{value_line}"
    )
    # CTA rendered as a real Telegram inline button. URL is a short link
    # so the "Open this link?" confirmation modal stays clean.
    inline_keyboard = [[{"text": cta_phrase, "url": cta_url}]]
    return (text, inline_keyboard)

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


# ════════════════════════════════════════════════════════════
#  X (Twitter) — formatting, posting, resolution helpers
# ════════════════════════════════════════════════════════════
# Lives in this module (not split) because everything it touches is
# already here: _broadcast_pick, the DB connection helper, _country_flag,
# the tip-result resolution polling loop.

# Country code 2-letter ISO → flag emoji (lightweight subset; falls back
# to no flag if missing). Mirrors the larger _country_flag map elsewhere
# in the file but keyed by ISO instead of free-text country name.
_X_FLAG_BY_COUNTRY = {
    "England":"🏴󠁧󠁢󠁥󠁮󠁧󠁿", "Spain":"🇪🇸", "Italy":"🇮🇹", "Germany":"🇩🇪", "France":"🇫🇷",
    "Portugal":"🇵🇹", "Netherlands":"🇳🇱", "Belgium":"🇧🇪", "Scotland":"🏴󠁧󠁢󠁳󠁣󠁴󠁿",
    "Brazil":"🇧🇷", "Argentina":"🇦🇷", "Mexico":"🇲🇽", "USA":"🇺🇸",
    "Norway":"🇳🇴", "Sweden":"🇸🇪", "Denmark":"🇩🇰", "Greece":"🇬🇷",
    "Turkey":"🇹🇷", "Switzerland":"🇨🇭", "Austria":"🇦🇹",
}


def _x_tweet_link(match_id, tip_id=None) -> str:
    """Build the tracking link for a tweet. Includes utm_source=twitter
    so GA4 attributes the visit. `tip_id` is included for finer attribution
    once we wire it on the landing page."""
    base = f"https://webpronos.com/match/{match_id}?utm_source=twitter&ref=auto"
    if tip_id:
        base += f"&tip_id={tip_id}"
    return base


# League name → 2-3 X hashtags that real fans actually search/follow.
# Keep them short and trending. Generic #BettingTips appended to all so
# the bot reaches the broader sports-betting audience too.
_X_LEAGUE_HASHTAGS = {
    "Premier League":            ["#PremierLeague"],
    "EPL":                       ["#PremierLeague"],
    "La Liga":                   ["#LaLiga"],
    "LaLiga":                    ["#LaLiga"],
    "Serie A":                   ["#SerieA"],
    "Bundesliga":                ["#Bundesliga"],
    "Ligue 1":                   ["#Ligue1"],
    "Primeira Liga":             ["#LigaPortugal"],
    "Liga Portugal":             ["#LigaPortugal"],
    "Liga Portugal Betclic":     ["#LigaPortugal"],
    "Eredivisie":                ["#Eredivisie"],
    "Jupiler Pro League":        ["#JPL"],
    "Pro League":                ["#JPL"],
    "UEFA Champions League":     ["#UCL"],
    "Champions League":          ["#UCL"],
    "UEFA Europa League":        ["#UEL"],
    "Europa League":             ["#UEL"],
    "UEFA Europa Conference League": ["#UECL"],
    "Conference League":         ["#UECL"],
    "Copa Libertadores":         ["#Libertadores"],
    "CONMEBOL Libertadores":     ["#Libertadores"],
    "Copa Sudamericana":         ["#Sudamericana"],
    "CONMEBOL Sudamericana":     ["#Sudamericana"],
    "Brasileirão":               ["#Brasileirao"],
    "Brasileirao":               ["#Brasileirao"],
    "Brasileirão Série A":       ["#Brasileirao"],
    "Campeonato Brasileiro Série A": ["#Brasileirao"],
    "MLS":                       ["#MLS"],
    "FIFA World Cup":            ["#WorldCup"],
    "World Cup":                 ["#WorldCup"],
    "FIFA World Cup 2026":       ["#WorldCup2026"],
}


# Pool of "generic" hashtag variants. We rotate through these RANDOMLY
# per tweet so we can A/B test which one drives the most engagement
# (impressions, profile clicks, follows). The variant chosen is persisted
# in `x_tweets.hashtag_variant` so we can later correlate with X's
# engagement metrics. Cap at 2 hashtags total per tweet — more triggers
# X's spam signal.
_X_HASHTAG_VARIANTS = [
    "#FootballTips",
    "#BettingTips",
    "#InPlayBetting",
    "#LiveBetting",
    "#FootballBetting",
    "#AIfootball",
    "#ValueBet",
    "#SoccerTips",
]


def _x_pick_hashtag_variant() -> str:
    """Pick one generic hashtag from the rotation. Uses random.choice
    (uniform distribution) so over 100+ tweets each variant gets ~equal
    exposure for statistical comparison."""
    import random as _r
    return _r.choice(_X_HASHTAG_VARIANTS)


def _x_hashtags_for(tournament: str, variant_override: str | None = None) -> tuple[str, str]:
    """Build a hashtag suffix for a tweet body. Returns (display_string,
    variant_used) so the caller can persist which variant was rolled.

    Format rules:
      - Always include the league hashtag if the tournament is recognised
        (relevance to the audience that follows that league).
      - Append ONE random generic hashtag from `_X_HASHTAG_VARIANTS` for
        A/B testing reach. Cap total at 2 (X penalises tweets with 3+).
      - If the league isn't in the map, the tweet still gets the generic
        variant so the A/B test data flows.

    `variant_override` lets the test endpoint pin a specific variant for
    deterministic previews without touching the random pool."""
    variant = variant_override or _x_pick_hashtag_variant()
    league_tags: list[str] = []
    if tournament:
        key = tournament.strip()
        league_tags = _X_LEAGUE_HASHTAGS.get(key) or []
        if not league_tags and "," in key:
            league_tags = _X_LEAGUE_HASHTAGS.get(key.split(",")[0].strip()) or []
    # Cap at 2: league tag (if any) + 1 variant. Drop duplicates.
    tags: list[str] = []
    if league_tags and league_tags[0].lower() != variant.lower():
        tags.append(league_tags[0])
    tags.append(variant)
    return " ".join(tags), variant


def _x_format_pick_tweet(match: dict, pick: dict, minute,
                          variant_override: str | None = None) -> tuple[str, str]:
    """Compose the tweet body for a new pick. Returns (body, variant_used).

    COST OPTIMISATION (2026-05-21): X API charges $0.20 per post that
    contains a URL vs $0.015 for plain text — a 13× difference. We
    therefore embed ZERO links in the tweet body. The @WebPronosAI
    profile bio carries the webpronos.com link, and X auto-renders the
    handle as a clickable interest link in every tweet, so users one
    click away from the site. The result reply (thread continuation)
    also stays URL-free.

    Format:
        🔔 LIVE PICK
        ⚽ {home} {hs}-{as} {away} · {min}'
        📊 {market}: {label} @ {odds}
        📈 Edge: +{edge}% (model {m}% vs market {mk}%)
        {league_tag} {variant_tag}
    """
    home  = match.get("homeTeam", "?")
    away  = match.get("awayTeam", "?")
    hs    = match.get("homeGoals", match.get("home_goals", 0)) or 0
    a_s   = match.get("awayGoals", match.get("away_goals", 0)) or 0
    flag  = _X_FLAG_BY_COUNTRY.get(match.get("country") or "", "")
    market = pick.get("market", "")
    label  = pick.get("label",  "")
    odds   = pick.get("odds")  or 0
    edge   = pick.get("edge")  or 0
    mp     = (pick.get("model") or 0) * 100
    kp     = (100.0 / odds) if odds > 0 else 0
    minute_str = f"{minute}'" if minute else "live"
    hashtags, variant = _x_hashtags_for(match.get("tournament", ""), variant_override)

    lines = [
        "🔔 LIVE PICK",
        f"{flag} ⚽ {home} {hs}-{a_s} {away} · {minute_str}",
        f"📊 {market}: {label} @ {odds:.2f}",
        f"📈 Edge: +{edge:.1f}% (model {mp:.0f}% vs market {kp:.0f}%)",
    ]
    if hashtags:
        lines.append(hashtags)
    return "\n".join(lines), variant


def _x_format_resolution_reply(market: str, label: str, odds, result: str) -> str:
    """Compose the reply-tweet body for a settled pick. Short on purpose —
    the original tweet context is right above the reply in the thread."""
    if result in ("win", "green"):
        return f"✅ WON — {market}: {label} @ {odds:.2f}\n+{(odds-1):.2f}u 💰"
    if result in ("loss", "red"):
        return f"❌ LOST — {market}: {label} @ {odds:.2f}\n-1.00u"
    # Push / void / cancelled — refunded stake
    return f"↔️ PUSH — {market}: {label}\nStake refunded"


def _x_recent_dup_exists(match_id, market: str, label: str) -> bool:
    """Anti-spam: was the same (match, market, label) tweeted in the last
    X_DEDUP_WINDOW_SEC seconds? Prevents accidental double-fire if the
    model re-emits the same pick after a brief glitch."""
    try:
        cutoff = int(time.time()) - X_DEDUP_WINDOW_SEC
        with _db() as conn:
            row = conn.execute(
                "SELECT 1 FROM x_tweets "
                "WHERE match_id = ? AND market = ? AND label = ? AND posted_at >= ? "
                "LIMIT 1",
                (match_id, market or "", label or "", cutoff),
            ).fetchone()
        return row is not None
    except Exception as e:
        log.debug(f"X dedup check failed: {e}")
        return False


# Counter helpers — the "still in flight" PENDING rows count too so
# the cap is enforced atomically at slot reservation time (see the
# atomic block in _x_tweet_new_pick). Real errors (anything other than
# PENDING) drop the row from the count so a fail-fast retry can use the
# slot again.
_X_COUNTABLE_STATES = "(error IS NULL OR error = 'PENDING')"


def _x_hourly_count() -> int:
    """Tweets (incl. in-flight) in the last 60 minutes — feeds X_MAX_PER_HOUR."""
    try:
        cutoff = int(time.time()) - 3600
        with _db() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM x_tweets "
                f"WHERE posted_at >= ? AND {_X_COUNTABLE_STATES} AND dry_run = 0",
                (cutoff,),
            ).fetchone()
        return int(row["n"] if row else 0)
    except Exception:
        return 0


def _x_daily_count() -> int:
    """Tweets (incl. in-flight) in the last 24h — feeds X_MAX_PER_DAY."""
    try:
        cutoff = int(time.time()) - 86400
        with _db() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM x_tweets "
                f"WHERE posted_at >= ? AND {_X_COUNTABLE_STATES} AND dry_run = 0",
                (cutoff,),
            ).fetchone()
        return int(row["n"] if row else 0)
    except Exception:
        return 0


def _x_match_count(match_id) -> int:
    """Tweets we've posted (incl. in-flight) for this match (any market)."""
    try:
        with _db() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM x_tweets "
                f"WHERE match_id = ? AND {_X_COUNTABLE_STATES} AND dry_run = 0",
                (match_id,),
            ).fetchone()
        return int(row["n"] if row else 0)
    except Exception:
        return 0


def _x_match_market_count(match_id, market: str) -> int:
    """Tweets we've posted (incl. in-flight) for this match + market combo.
    Default cap = 1 means the same market never tweets twice per match."""
    try:
        with _db() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM x_tweets "
                f"WHERE match_id = ? AND market = ? AND {_X_COUNTABLE_STATES} AND dry_run = 0",
                (match_id, market or ""),
            ).fetchone()
        return int(row["n"] if row else 0)
    except Exception:
        return 0


# Process-wide lock for the cap-check + intent-record critical section.
# Without this, multiple picks firing in the same ~100ms window all see
# count=0 in their cap checks (no row written yet by the still-pending
# tweet threads) and burst-tweet despite the limits. Holding the lock
# while we both query AND INSERT the intent row makes the slot reservation
# atomic w.r.t. other concurrent callers.
_x_post_lock = _threading.Lock()


def _x_tweet_new_pick(match: dict, pick: dict, minute) -> None:
    """Post a tweet for a new pick. No-op if X is not configured,
    threshold not met, or rate limit hit. Always non-blocking from the
    caller's POV — the Twitter API call itself runs in a background
    thread; the cap check + DB intent row happen synchronously under
    a lock so the limits cannot be raced.

    Failure modes are logged but never raised (X downtime must not break
    pick generation or other fan-outs).
    """
    if not (_x_configured() and X_BOT_ENABLED):
        return
    edge = pick.get("edge") or 0
    if edge < X_EDGE_THRESHOLD:
        return
    match_id = match.get("id") or match.get("match_id")
    market   = pick.get("market", "")
    label    = pick.get("label",  "")
    if not match_id:
        return

    # ── ATOMIC SLOT RESERVATION ────────────────────────────────────────
    # Cap stack runs under the lock to prevent two concurrent calls from
    # both seeing count=0 and racing past the limits. The intent row is
    # INSERTed inside the same critical section so the next caller's cap
    # query immediately sees this slot as taken.
    intent_row_id = None
    with _x_post_lock:
        if _x_recent_dup_exists(match_id, market, label):
            log.debug(f"X dedup skip: {market}/{label} on {match_id}")
            return
        if _x_match_market_count(match_id, market) >= X_MAX_PER_MARKET_PER_MATCH:
            log.info(f"X market cap ({X_MAX_PER_MARKET_PER_MATCH}) hit on "
                      f"match={match_id} market={market} — skipping")
            return
        if _x_match_count(match_id) >= X_MAX_PER_MATCH:
            log.info(f"X per-match cap ({X_MAX_PER_MATCH}) hit on "
                      f"match={match_id} — skipping")
            return
        if _x_hourly_count() >= X_MAX_PER_HOUR:
            log.warning(f"X hourly cap ({X_MAX_PER_HOUR}) hit — skipping pick")
            return
        if _x_daily_count() >= X_MAX_PER_DAY:
            log.warning(f"X daily cap ({X_MAX_PER_DAY}) hit — skipping pick")
            return

        # Reserve the slot in the DB before releasing the lock. tweet_id
        # stays NULL until the Twitter call returns; error='PENDING' so
        # cap counters CAN count this row (cap queries treat dry_run + posted_at
        # but not error filtering — see _x_*_count helpers). Resolver
        # endpoints filter PENDING rows out separately.
        try:
            with _db() as conn:
                cur = conn.execute(
                    "INSERT INTO x_tweets "
                    "  (tip_id, match_id, market, label, tweet_id, posted_at, error, dry_run) "
                    "VALUES (?, ?, ?, ?, NULL, ?, 'PENDING', ?)",
                    (
                        pick.get("tip_id"),
                        match_id,
                        market,
                        label,
                        int(time.time()),
                        1 if X_DRY_RUN else 0,
                    ),
                )
                intent_row_id = cur.lastrowid
        except Exception as db_err:
            log.error(f"X intent INSERT failed (skipping post): {db_err}")
            return

    # ── ACTUAL TWITTER CALL (async, outside lock) ──────────────────────
    def _do_post():
        body, variant = _x_format_pick_tweet(match, pick, minute)
        tweet_id = None
        err = None
        if X_DRY_RUN:
            log.info(f"[X DRY_RUN] Would tweet (variant={variant}):\n{body}")
        else:
            client = _x_client()
            if client is None:
                err = "client_unavailable"
            else:
                try:
                    resp = client.create_tweet(text=body)
                    tweet_id = str(resp.data.get("id")) if getattr(resp, "data", None) else None
                    log.info(f"X tweeted pick {match_id} {market}/{label} variant={variant} → id={tweet_id}")
                except Exception as e:
                    err = str(e)[:300]
                    log.error(f"X tweet failed: {err}")

        # Update the reserved row with the final outcome + which hashtag
        # variant we used. err stays 'PENDING' only if both branches above
        # failed to set it; we explicitly set to NULL on success so cap
        # counters keep the slot as a real send (not a freed slot).
        try:
            with _db() as conn:
                conn.execute(
                    "UPDATE x_tweets SET tweet_id = ?, error = ?, hashtag_variant = ? WHERE id = ?",
                    (tweet_id, err, variant, intent_row_id),
                )
        except Exception as db_err:
            log.error(f"X intent UPDATE failed: {db_err}")

    _threading.Thread(target=_do_post, daemon=True,
                       name=f"x-tweet-{intent_row_id}").start()


def _x_resolve_settled_tips() -> int:
    """Find x_tweets rows that have not been resolved yet and whose
    underlying tip has settled in the `tips` table. Post a reply-tweet
    to the original for each. Returns the number of resolutions posted.

    Called periodically (every 5 min) by the existing background loop.
    Idempotent — only resolves rows where resolution_tweet_id IS NULL.
    """
    if not (_x_configured() and X_BOT_ENABLED):
        return 0
    posted = 0
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT x.id, x.tweet_id, x.tip_id, x.match_id, "
                "       t.result, t.market, t.label, t.odd_entry "
                "FROM x_tweets x "
                "JOIN tips t ON t.id = x.tip_id "
                "WHERE x.resolution_tweet_id IS NULL "
                "  AND x.tweet_id IS NOT NULL "
                "  AND x.error IS NULL "
                "  AND t.result IS NOT NULL "
                "  AND x.posted_at >= ? "
                "LIMIT 20",
                (int(time.time()) - 86400 * 3,)  # only look back 3 days
            ).fetchall()
        for r in rows:
            body = _x_format_resolution_reply(
                r["market"] or "", r["label"] or "",
                r["odd_entry"] or 0, r["result"]
            )
            resolved_id = None
            err = None
            if X_DRY_RUN:
                log.info(f"[X DRY_RUN] Would reply to {r['tweet_id']}:\n{body}")
            else:
                client = _x_client()
                if client is None:
                    continue
                try:
                    resp = client.create_tweet(
                        text=body,
                        in_reply_to_tweet_id=int(r["tweet_id"]),
                    )
                    resolved_id = str(resp.data.get("id")) if getattr(resp, "data", None) else None
                except Exception as e:
                    err = str(e)[:300]
                    log.error(f"X resolution reply failed for tweet {r['tweet_id']}: {err}")

            try:
                with _db() as conn:
                    conn.execute(
                        "UPDATE x_tweets "
                        "SET resolved_at = ?, resolution_tweet_id = ? "
                        "WHERE id = ?",
                        (int(time.time()), resolved_id, r["id"]),
                    )
                if resolved_id:
                    posted += 1
            except Exception as db_err:
                log.error(f"X resolution DB update failed: {db_err}")
    except Exception as e:
        log.warning(f"_x_resolve_settled_tips: {e}")
    return posted


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

        # Fan-out to dedicated inbet Telegram bot (WC 2026 picks only, eligible
        # members only). No-op if INBET_BOT_TOKEN is not configured — safe to
        # ship before inbet provides the token.
        try:
            _broadcast_inbet_pick(match, pick, minute)
        except Exception as sub_err:
            log.error(f"inbet fan-out failed: {sub_err}")

        # Fan-out to PWA Web Push subscribers (browser/iOS standalone PWA).
        # No-op if VAPID isn't configured. Sends are tagged per-match so
        # back-to-back picks on the same game collapse on the lockscreen.
        try:
            _broadcast_push_pick(match, pick, minute)
        except Exception as sub_err:
            log.error(f"push fan-out failed: {sub_err}")

        # Fan-out to X (@WebPronosAI) for picks above the edge threshold.
        # _x_tweet_new_pick itself runs the actual API call in a background
        # thread so this is non-blocking even on Twitter latency / errors.
        # Disabled gracefully if X_API_KEY is not configured.
        try:
            _x_tweet_new_pick(match, pick, minute)
        except Exception as sub_err:
            log.error(f"X fan-out failed: {sub_err}")
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
#  BANDWIDTH TRACKING — wraps _session to size proxy plans
# ════════════════════════════════════════════════════════════
# Counts response-body bytes per destination host. Periodically flushed
# to the bandwidth_log table (rolled up per hour) so we can answer
# "how many GB/month does Sofascore traffic actually consume?" before
# paying for residential proxy bandwidth.

import threading as _threading_mod
_bw_counters: dict = {}            # host → {"bytes_in": int, "count": int}
_bw_lock = _threading_mod.Lock()


def _track_response(url, resp):
    """Record bytes-in for a response. Cheap path, runs synchronously
    after every _session.get/_session.post. Never raises — bandwidth
    accounting must never interfere with actual scraping."""
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).netloc or "unknown").lower()
        # resp.content for curl_cffi / requests / cloudscraper is bytes.
        # Length 0 means the wrapped library streamed without buffering
        # — uncommon for our endpoints (all small JSON / HTML / images).
        body = getattr(resp, "content", None)
        n = len(body) if body else 0
        with _bw_lock:
            d = _bw_counters.setdefault(host, {"bytes_in": 0, "count": 0})
            d["bytes_in"] += n
            d["count"]    += 1
    except Exception:
        pass


def _flush_bw_counters():
    """Persist in-memory counters into bandwidth_log. Called from the BG
    loop every cycle so a worker crash loses at most one cycle of data.
    Uses ON CONFLICT to fold multiple flushes within the same hour into
    one row per (hour, host)."""
    with _bw_lock:
        if not _bw_counters:
            return
        snap = {h: dict(d) for h, d in _bw_counters.items()}
        _bw_counters.clear()
    ts_hour = (int(time.time()) // 3600) * 3600
    try:
        with _db() as conn:
            for host, d in snap.items():
                conn.execute(
                    "INSERT INTO bandwidth_log (ts_hour, host, bytes_in, request_count) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(ts_hour, host) DO UPDATE SET "
                    "  bytes_in      = bytes_in + excluded.bytes_in, "
                    "  request_count = request_count + excluded.request_count",
                    (ts_hour, host, d["bytes_in"], d["count"])
                )
    except Exception as e:
        log.warning(f"_flush_bw_counters failed: {e}")


class _TrackedSession:
    """Drop-in proxy around a curl_cffi / cloudscraper / requests Session
    that records response bytes per host. Forwards all other attribute
    access to the underlying session via __getattr__ so existing callers
    don't need to change (they keep using _session.headers, .cookies,
    .post, .get etc identically)."""
    def __init__(self, inner):
        object.__setattr__(self, "_s", inner)
    def get(self, url, **kw):
        r = self._s.get(url, **kw)
        _track_response(url, r)
        return r
    def post(self, url, **kw):
        r = self._s.post(url, **kw)
        _track_response(url, r)
        return r
    def __getattr__(self, name):
        # Only fires for attrs not on the proxy itself. Forwards
        # transparently to the wrapped session.
        return getattr(self._s, name)
    def __setattr__(self, name, value):
        setattr(self._s, name, value)


def _wrap_session(s):
    """Wrap the configured session in the bandwidth tracker. Idempotent —
    re-wrapping an already-wrapped session is a no-op."""
    if isinstance(s, _TrackedSession):
        return s
    return _TrackedSession(s)


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
                _session = _wrap_session(CffiSession(impersonate=profile))
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
        _session = _wrap_session(cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True},
            delay=3,
        ))
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
        _session = _wrap_session(req.Session())
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
    """Full match data including odds & value analysis.

    Three-tier resolution so the SPA always gets *something* renderable:
      1. Sofascore live event       (full live data: shots, incidents, odds)
      2. Our games table             (stored match: teams, score, tournament,
                                       country — enough to render the basic
                                       card + tip history)
      3. 404                         (truly unknown ID)

    Without tier 2, a Sofascore outage or a stale match id (older games
    Sofascore drops from public endpoints) made the SPA render a blank
    page — the very bug the user reported.

    The 'tournament' field is canonicalised before returning so SPA
    breadcrumbs/links land users on the unified league page.
    """
    try:
        d         = None
        shots     = {"homeShots": [], "awayShots": [], "homeXg": 0, "awayXg": 0, "totalShots": 0}
        incidents = []
        odds      = None
        from_source = "live"

        # Tier 1: Sofascore live event
        try:
            d = get_event(eid)
        except Exception as e:
            log.debug(f"r_match: get_event failed for {eid}: {e}")
            d = None

        if d:
            try:    shots     = get_shotmap(eid)
            except Exception: pass
            try:    incidents = get_incidents(eid)
            except Exception: pass
            try:    odds      = get_full_odds_analysis(d, shots)
            except Exception: pass
        else:
            # Tier 2: hydrate from local DB (resilient to Sofascore outages
            # AND to old matches Sofascore dropped from /event/<id>)
            from_source = "db"
            try:
                with _db() as conn:
                    g = conn.execute(
                        "SELECT id, home_team, away_team, home_goals, away_goals, "
                        "       home_team_id, away_team_id, tournament, country, "
                        "       start_ts, is_finished FROM games WHERE id = ?",
                        (eid,)
                    ).fetchone()
                if not g:
                    return jsonify({"error": "Not found"}), 404
                d = {
                    "id":             g["id"],
                    "homeTeam":       g["home_team"],
                    "awayTeam":       g["away_team"],
                    "homeGoals":      g["home_goals"],
                    "awayGoals":      g["away_goals"],
                    "homeTeamId":     g["home_team_id"],
                    "awayTeamId":     g["away_team_id"],
                    "tournament":     g["tournament"] or "",
                    "country":        g["country"] or "",
                    "startTimestamp": g["start_ts"],
                    "isFinished":     bool(g["is_finished"]),
                    "isLive":         False,
                    "minute":         None,
                    "status":         {"type": "finished" if g["is_finished"] else "notstarted"},
                    "_source":        "db_fallback",
                }
                # Pull persisted shots from match_shots so the chart still has
                # data when Sofascore is unreachable.
                try:
                    with _db() as conn:
                        rows = conn.execute(
                            "SELECT minute, added_time, is_home, xg, is_goal, is_penalty "
                            "FROM match_shots WHERE match_id = ? AND minute >= 0 "
                            "ORDER BY minute, added_time",
                            (eid,)
                        ).fetchall()
                    home_shots = [dict(r) for r in rows if r["is_home"]]
                    away_shots = [dict(r) for r in rows if not r["is_home"]]
                    home_xg = sum(s["xg"] for s in home_shots if not s["is_penalty"])
                    away_xg = sum(s["xg"] for s in away_shots if not s["is_penalty"])
                    shots = {
                        "homeShots":  home_shots,
                        "awayShots":  away_shots,
                        "homeXg":     round(home_xg, 3),
                        "awayXg":     round(away_xg, 3),
                        "totalShots": len(home_shots) + len(away_shots),
                    }
                except Exception:
                    pass
            except Exception as e:
                log.exception(f"r_match: DB fallback failed for {eid}: {e}")
                return jsonify({"error": "Not found"}), 404

        # Canonicalise tournament + add the raw form for traceability.
        tourn_raw = d.get("tournament", "")
        if tourn_raw:
            canonical = _normalize_tournament_pretty(tourn_raw) or tourn_raw
            d["tournament_raw"] = tourn_raw
            d["tournament"]     = canonical

        # Inject team crests resolved with country context. The match page
        # otherwise calls /api/team_logo/<name> with the plain name, which
        # makes homonyms (e.g. Athletic Club Spain/Brazil) collide on the
        # same cache key. Embedding them here means the SPA can render the
        # right crest immediately without any extra round-trip.
        try:
            _h = d.get("homeTeam") or ""
            _a = d.get("awayTeam") or ""
            if isinstance(_h, dict): _h = _h.get("name", "")
            if isinstance(_a, dict): _a = _a.get("name", "")
            _country = d.get("country") or ""
            d["home_logo"] = _quick_logo(_h, _country or None)
            d["away_logo"] = _quick_logo(_a, _country or None)
        except Exception:
            pass

        return jsonify({
            "match":     d,
            "shots":     shots,
            "incidents": incidents,
            "odds":      odds,
            "_source":   from_source,
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


# ── Hardcoded league priority by sport_key ────────────────────────────────────
# Used by anything that needs to sort matches by importance:
#   - Telegram daily preview teaser (top 3 of the day)
#   - Footer live_now column (max 5 with priority tie-break)
#   - Internal admin views
#
# Lower number = MORE IMPORTANT. The DB `competitions.priority` table
# overrides this (admin-editable), but the table is sometimes empty after
# a fresh deploy / volume migration — this hardcoded map guarantees a
# sensible ordering at all times.
LEAGUE_PRIORITY_BY_SPORT_KEY: dict = {
    # Tier 1 — global flagship competitions
    "soccer_uefa_champs_league":                  1,
    "soccer_uefa_europa_league":                  2,
    "soccer_uefa_europa_conference_league":       3,
    "soccer_fifa_world_cup":                      1,
    "soccer_uefa_european_championship":          2,
    "soccer_uefa_nations_league":                 8,
    "soccer_uefa_champs_league_qualification":    10,
    "soccer_uefa_champs_league_women":            15,
    "soccer_uefa_euro_qualification":             12,
    "soccer_fifa_world_cup_qualifiers_europe":    9,
    "soccer_fifa_world_cup_qualifiers_south_america": 11,
    "soccer_fifa_world_cup_womens":               16,
    "soccer_fifa_club_world_cup":                 6,

    # Tier 2 — Top 5 European domestic leagues
    "soccer_epl":                                 5,
    "soccer_spain_la_liga":                       5,
    "soccer_italy_serie_a":                       5,
    "soccer_germany_bundesliga":                  5,
    "soccer_france_ligue_one":                    5,

    # Tier 3 — Strong second European leagues + second tiers
    "soccer_portugal_primeira_liga":              15,
    "soccer_netherlands_eredivisie":              16,
    "soccer_belgium_first_div":                   17,
    "soccer_turkey_super_league":                 18,
    "soccer_germany_bundesliga2":                 20,
    "soccer_efl_champ":                           20,
    "soccer_spl":                                 22,  # Scottish Premiership
    "soccer_greece_super_league":                 25,
    "soccer_austria_bundesliga":                  28,
    "soccer_switzerland_superleague":             28,
    "soccer_russia_premier_league":               30,

    # Tier 4 — South America top flights
    "soccer_brazil_campeonato":                   14,
    "soccer_brazil_serie_b":                      32,
    "soccer_chile_campeonato":                    35,
    "soccer_mexico_ligamx":                       19,
    "soccer_conmebol_copa_libertadores":          7,
    "soccer_conmebol_copa_sudamericana":          13,
    "soccer_conmebol_copa_america":               4,

    # Tier 5 — North America / Asia top flights
    "soccer_usa_mls":                             21,
    "soccer_japan_j_league":                      33,
    "soccer_saudi_arabia_pro_league":             34,

    # Tier 6 — Nordic + Eastern European top flights
    "soccer_sweden_allsvenskan":                  40,
    "soccer_norway_eliteserien":                  40,
    "soccer_denmark_superliga":                   40,
    "soccer_finland_veikkausliiga":               45,
    "soccer_poland_ekstraklasa":                  42,
    "soccer_league_of_ireland":                   48,
}


def _league_priority(sport_key: str | None) -> int:
    """
    Resolve the priority for a league. Lookup order:
      1. competitions DB table (admin-editable)
      2. hardcoded LEAGUE_PRIORITY_BY_SPORT_KEY map
      3. default 99 (unknown / unranked)
    Lower number = MORE IMPORTANT.
    """
    if not sport_key:
        return 99
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT priority FROM competitions WHERE sport_key = ?",
                (sport_key,)
            ).fetchone()
        if row and row["priority"]:
            return int(row["priority"])
    except Exception:
        pass
    return LEAGUE_PRIORITY_BY_SPORT_KEY.get(sport_key, 99)


# ════════════════════════════════════════════════════════════════════════════
#  FIFA WORLD CUP 2026 — inbet.io white-label widgets
# ════════════════════════════════════════════════════════════════════════════
# Drives the two embeddable widgets exposed to inbet.io's premium members:
#   1. /widget/wc2026/current — per-match state machine (LIVE → RESULTS → PREVIEW → OFF-DAY)
#   2. /widget/wc2026/performance — tournament-scoped P&L / ROI / top greens
# Both pull from the same JSON endpoints (/api/wc2026/*.json) so the iframe
# can be embedded with one HTML tag and refreshed without page reload.

# Tournament boundaries — used to scope DB queries and date math.
WC2026_START_TS = 1749600000   # 2026-06-11 00:00 UTC (opener: South Africa vs Mexico)
WC2026_END_TS   = 1753056000   # 2026-07-19 00:00 UTC (one day after final)
WC2026_EMBLEM_URL = "https://upload.wikimedia.org/wikipedia/en/thumb/1/17/2026_FIFA_World_Cup_emblem.svg/250px-2026_FIFA_World_Cup_emblem.svg.png"

# Supported widget locales. Inbet stores user locale; passes via ?lang=...
WIDGET_LOCALES = ("en", "es", "pt-pt", "pt-br")
WIDGET_DEFAULT_LOCALE = "en"

# All UI strings used by the widgets, keyed by locale → key.
# Keep keys identical across locales — missing keys fall back to English.
WIDGET_COPY: dict = {
    "en": {
        # status pills
        "live_now":        "LIVE",
        "scheduled":       "SCHEDULED",
        "finished":        "FT",
        "half_time":       "HT",
        # per-match widget sections
        "algo_picks":      "Algorithm Picks",
        "no_picks_yet":    "Awaiting value picks…",
        "result_timeline": "Pick Timeline",
        "xg_momentum":     "Live xG momentum",
        "next_up":         "Next up",
        "kickoff_in":      "Kickoff in",
        "model_preview":   "Model preview",
        "wc_resumes_in":   "World Cup resumes in",
        "upcoming_matches": "Upcoming matches",
        "model_lean_over":  "Model leans Over 2.5 — combined attacks averaging {xg} xG/match",
        "model_lean_under": "Model leans Under 2.5 — combined attacks averaging {xg} xG/match",
        "get_alerts":       "Get Pick Alerts",
        # result badges
        "result_won":      "Won",
        "result_lost":     "Lost",
        "result_push":     "Push",
        "result_pending":  "Pending",
        # performance dashboard
        "perf_title":      "World Cup 2026 · Live xG Model",
        "perf_picks":      "Total picks",
        "perf_winrate":    "Win rate",
        "perf_pnl":        "P&L",
        "perf_roi":        "ROI",
        "perf_equity":     "Equity curve",
        "perf_top_greens": "Biggest winners",
        "perf_by_market":  "Profit by market",
        "perf_updated":    "Updated every 5 min",
        # entered-at / minute label
        "min_entered":     "Entered at",
        "minute_short":    "'",
        # footers
        "powered_by":      "Powered by InBetIO Live xG Model",
        "wc_emblem_alt":   "FIFA World Cup 2026",
        # error / empty
        "no_data_yet":     "No data yet for this tournament.",
        "no_live_match":   "No World Cup match live right now.",
    },
    "es": {
        "live_now":        "EN VIVO",
        "scheduled":       "PROGRAMADO",
        "finished":        "FT",
        "half_time":       "HT",
        "algo_picks":      "Picks del Algoritmo",
        "no_picks_yet":    "Esperando picks con valor…",
        "result_timeline": "Línea de picks",
        "xg_momentum":     "Momentum xG en vivo",
        "next_up":         "A continuación",
        "kickoff_in":      "Inicio en",
        "model_preview":   "Previa del modelo",
        "wc_resumes_in":   "El Mundial vuelve en",
        "upcoming_matches": "Próximos partidos",
        "model_lean_over":  "El modelo se inclina por Más de 2.5 — ataques combinados promedian {xg} xG/partido",
        "model_lean_under": "El modelo se inclina por Menos de 2.5 — ataques combinados promedian {xg} xG/partido",
        "get_alerts":       "Recibir Picks",
        "result_won":      "Ganada",
        "result_lost":     "Perdida",
        "result_push":     "Empate",
        "result_pending":  "Pendiente",
        "perf_title":      "Mundial 2026 · Modelo xG en Vivo",
        "perf_picks":      "Picks totales",
        "perf_winrate":    "Tasa de aciertos",
        "perf_pnl":        "P&L",
        "perf_roi":        "ROI",
        "perf_equity":     "Curva de capital",
        "perf_top_greens": "Mejores aciertos",
        "perf_by_market":  "Beneficio por mercado",
        "perf_updated":    "Actualizado cada 5 min",
        "min_entered":     "Entrada al",
        "minute_short":    "'",
        "powered_by":      "Powered by InBetIO Live xG Model",
        "wc_emblem_alt":   "FIFA Mundial 2026",
        "no_data_yet":     "Aún no hay datos para este torneo.",
        "no_live_match":   "No hay partido del Mundial en vivo ahora mismo.",
    },
    "pt-pt": {
        "live_now":        "EM DIRETO",
        "scheduled":       "AGENDADO",
        "finished":        "FT",
        "half_time":       "INT",
        "algo_picks":      "Picks do Algoritmo",
        "no_picks_yet":    "À espera de picks com valor…",
        "result_timeline": "Linha do tempo das picks",
        "xg_momentum":     "Momentum xG em direto",
        "next_up":         "A seguir",
        "kickoff_in":      "Início em",
        "model_preview":   "Antevisão do modelo",
        "wc_resumes_in":   "Mundial regressa em",
        "upcoming_matches": "Próximos jogos",
        "model_lean_over":  "Modelo inclina-se para Mais de 2.5 — ataques combinados em média {xg} xG/jogo",
        "model_lean_under": "Modelo inclina-se para Menos de 2.5 — ataques combinados em média {xg} xG/jogo",
        "get_alerts":       "Receber Picks",
        "result_won":      "Ganha",
        "result_lost":     "Perdida",
        "result_push":     "Empate",
        "result_pending":  "Pendente",
        "perf_title":      "Mundial 2026 · Modelo xG ao Vivo",
        "perf_picks":      "Picks totais",
        "perf_winrate":    "Taxa de acerto",
        "perf_pnl":        "P&L",
        "perf_roi":        "ROI",
        "perf_equity":     "Curva de capital",
        "perf_top_greens": "Maiores vitórias",
        "perf_by_market":  "Lucro por mercado",
        "perf_updated":    "Atualizado a cada 5 min",
        "min_entered":     "Entrada ao",
        "minute_short":    "'",
        "powered_by":      "Powered by InBetIO Live xG Model",
        "wc_emblem_alt":   "FIFA Mundial 2026",
        "no_data_yet":     "Ainda sem dados para este torneio.",
        "no_live_match":   "Sem jogo do Mundial em direto neste momento.",
    },
    "pt-br": {
        "live_now":        "AO VIVO",
        "scheduled":       "AGENDADO",
        "finished":        "FT",
        "half_time":       "INT",
        "algo_picks":      "Picks do Algoritmo",
        "no_picks_yet":    "Aguardando picks com valor…",
        "result_timeline": "Linha do tempo das picks",
        "xg_momentum":     "Momentum xG ao vivo",
        "next_up":         "A seguir",
        "kickoff_in":      "Início em",
        "model_preview":   "Prévia do modelo",
        "wc_resumes_in":   "Copa volta em",
        "upcoming_matches": "Próximos jogos",
        "model_lean_over":  "Modelo inclina-se para Mais de 2.5 — ataques combinados na média de {xg} xG/jogo",
        "model_lean_under": "Modelo inclina-se para Menos de 2.5 — ataques combinados na média de {xg} xG/jogo",
        "get_alerts":       "Receber Picks",
        "result_won":      "Ganha",
        "result_lost":     "Perdida",
        "result_push":     "Empate",
        "result_pending":  "Pendente",
        "perf_title":      "Copa 2026 · Modelo xG ao Vivo",
        "perf_picks":      "Picks totais",
        "perf_winrate":    "Taxa de acerto",
        "perf_pnl":        "L&P",
        "perf_roi":        "ROI",
        "perf_equity":     "Curva de capital",
        "perf_top_greens": "Maiores vitórias",
        "perf_by_market":  "Lucro por mercado",
        "perf_updated":    "Atualizado a cada 5 min",
        "min_entered":     "Entrada ao",
        "minute_short":    "'",
        "powered_by":      "Powered by InBetIO Live xG Model",
        "wc_emblem_alt":   "FIFA Copa do Mundo 2026",
        "no_data_yet":     "Ainda sem dados para este torneio.",
        "no_live_match":   "Nenhum jogo da Copa ao vivo neste momento.",
    },
}


def _widget_locale(raw: str | None) -> str:
    """Normalize and validate a locale string from a query param."""
    if not raw:
        return WIDGET_DEFAULT_LOCALE
    norm = raw.strip().lower().replace("_", "-")
    return norm if norm in WIDGET_LOCALES else WIDGET_DEFAULT_LOCALE


def _t(locale: str, key: str) -> str:
    """Lookup a translated string with English fallback."""
    if locale not in WIDGET_COPY:
        locale = WIDGET_DEFAULT_LOCALE
    return WIDGET_COPY[locale].get(key) or WIDGET_COPY[WIDGET_DEFAULT_LOCALE].get(key, key)


def _wc_tournament_variants() -> list[str]:
    """All tournament-name strings in the games table that count as WC 2026."""
    # We use the league-variants helper if the index has been built, plus a
    # hardcoded list of known Sofascore variants. The fixed list ensures the
    # widgets return correct data even before the slug index is warm.
    fixed = [
        "FIFA World Cup",
        "FIFA World Cup 2026",
        "World Cup",
        "World Cup, Group Stage",
        "World Cup, Round of 32",
        "World Cup, Round of 16",
        "World Cup, Quarterfinals",
        "World Cup, Semifinals",
        "World Cup, Final",
        "World Cup, 3rd Place",
    ]
    try:
        variants = _league_variants_for("FIFA World Cup")
        merged = list({*fixed, *variants})
    except Exception:
        merged = fixed
    return merged


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

# ════════════════════════════════════════════════════════════
#  TIPS ARCHIVE — Supabase Postgres backup of historical picks
# ════════════════════════════════════════════════════════════
# Why this exists: the bom volume incident of 2026-05-20 silently wiped
# 30 days of historical tips when the underlying Fly volume was lost.
# Our /history, /league/<slug>, /team/<slug> pages went blank — broken
# the "auditable public track record" pitch which is core to the brand.
#
# This module:
#   1. Periodically dumps the local `tips` table (with cached game
#      context for self-contained reads) to a Supabase Postgres table
#      `tips_archive`. Cadence: every hour via APScheduler.
#   2. On boot, if the local SQLite is empty (volume just reset), pulls
#      every row from tips_archive back into the local DB. Automatic.
#   3. Exposes admin endpoints to trigger archive / restore manually.
#
# Cost: $0 — fits in Supabase Free tier (500MB Postgres). At ~150 bytes
# per archived tip and ~100 tips/day, 30 days = 450KB. Pad 10× for
# growth = 4.5MB. Still 1% of the free tier.
#
# RPO: 1 hour worst case (if disaster strikes 59 min after last archive).
# This is acceptable for historical track record — settled tips are
# immutable once recorded so we never overwrite good data with stale.
#
# RTO: ~5 seconds (Supabase REST returns ~1000 rows in 1-2s; we just
# bulk-insert into local SQLite).

def _supabase_archive_url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


def _supabase_archive_headers(write: bool = False) -> dict:
    """Headers for Supabase REST.

    Use SERVICE_ROLE_KEY for writes (bypasses RLS — anon would silently
    insert into a 0-row view). Anon key fine for reads since RLS on
    `tips_archive` should allow public read for the /history use case.
    """
    key = SUPABASE_SERVICE_ROLE_KEY if write else SUPABASE_ANON
    h = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }
    if write:
        # Upsert semantics: on PK conflict (match_id, tip_key), update
        # the row in place. Keeps results/odd_now fresh as picks settle.
        h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return h


def _archive_tips_to_supabase(limit: int = 5000) -> dict:
    """Dump up to `limit` most-recent settled tips from local SQLite to
    Supabase tips_archive. Upsert by (match_id, tip_key) — re-runs are
    idempotent. Returns a small summary dict.

    Cheap & safe to call on every cron tick: typical hourly delta is
    ~10-100 rows, full re-archive happens only on first run after a
    schema migration. ~1-2s per 1000 rows over Supabase REST.
    """
    if not SUPABASE_ANON or not SUPABASE_URL:
        return {"ok": False, "reason": "supabase_not_configured"}
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT t.tip_key, t.match_id, t.market, t.label,
                       t.odd_entry, t.odd_now, t.edge_entry, t.minute_entry,
                       t.wall_ts, t.result,
                       g.home_team, g.away_team, g.home_goals, g.away_goals,
                       g.tournament, g.country
                FROM tips t
                LEFT JOIN games g ON g.id = t.match_id
                ORDER BY t.wall_ts DESC
                LIMIT ?
            """, (limit,)).fetchall()
    except Exception as e:
        return {"ok": False, "reason": "local_db_read_failed", "error": str(e)[:200]}

    if not rows:
        return {"ok": True, "archived": 0, "note": "no_tips_in_local_db"}

    payload = [dict(r) for r in rows]
    try:
        import urllib.request as _ur
        req = _ur.Request(
            _supabase_archive_url("tips_archive"),
            data=json.dumps(payload).encode("utf-8"),
            headers=_supabase_archive_headers(write=True),
            method="POST",
        )
        with _ur.urlopen(req, timeout=30) as r:
            status = r.status
        return {"ok": True, "archived": len(payload), "status": status}
    except Exception as e:
        log.error(f"_archive_tips_to_supabase failed: {e}")
        return {"ok": False, "reason": "supabase_write_failed", "error": str(e)[:200]}


def _restore_tips_from_supabase(force: bool = False) -> dict:
    """Pull every row from Supabase tips_archive into local SQLite. Only
    runs if the local `tips` table is empty (or force=True).

    Triggered automatically by `_init_db()` after schema creation if the
    local volume just came up empty. Bulk INSERT OR IGNORE so any tips
    that ARE in local (e.g. just created by the bg loop while restore
    runs) win — the archive is the floor, local is the ceiling.
    """
    if not SUPABASE_ANON or not SUPABASE_URL:
        return {"ok": False, "reason": "supabase_not_configured"}

    # Cheap check: don't pull if local already has data
    try:
        with _db() as conn:
            local_count = conn.execute("SELECT COUNT(*) FROM tips").fetchone()[0]
    except Exception:
        local_count = 0
    if local_count > 0 and not force:
        return {"ok": True, "skipped": True, "local_count": local_count}

    try:
        import urllib.request as _ur
        # Supabase caps page size at 1000 — paginate via Range header
        all_rows: list = []
        page = 0
        while True:
            req = _ur.Request(
                _supabase_archive_url("tips_archive") + "?select=*&order=wall_ts.desc",
                headers={
                    **_supabase_archive_headers(write=False),
                    "Range-Unit": "items",
                    "Range":      f"{page*1000}-{(page+1)*1000-1}",
                },
            )
            with _ur.urlopen(req, timeout=30) as r:
                batch = json.loads(r.read())
            if not batch:
                break
            all_rows.extend(batch)
            if len(batch) < 1000:
                break
            page += 1
            if page > 100:  # safety: cap at 100k rows
                break
    except Exception as e:
        log.error(f"_restore_tips_from_supabase fetch failed: {e}")
        return {"ok": False, "reason": "supabase_read_failed", "error": str(e)[:200]}

    if not all_rows:
        return {"ok": True, "restored": 0, "note": "archive_empty"}

    # Hydrate local games (so JOINs in renderers still work) + tips.
    inserted_tips = 0
    inserted_games = 0
    try:
        with _db() as conn:
            # Reconstruct minimal `games` rows from cached fields. Use
            # INSERT OR IGNORE so we never overwrite richer rows the
            # live scraper added since restore started.
            seen_games = set()
            for r in all_rows:
                mid = r.get("match_id")
                if mid and mid not in seen_games:
                    seen_games.add(mid)
                    try:
                        cur = conn.execute(
                            "INSERT OR IGNORE INTO games "
                            "(id, home_team, away_team, home_goals, away_goals, "
                            " tournament, country, is_finished) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (mid, r.get("home_team") or "?", r.get("away_team") or "?",
                             r.get("home_goals") or 0, r.get("away_goals") or 0,
                             r.get("tournament"), r.get("country"),
                             1 if r.get("result") is not None else 0),
                        )
                        if cur.rowcount > 0:
                            inserted_games += 1
                    except Exception:
                        pass
                try:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO tips "
                        "(tip_key, match_id, market, label, odd_entry, odd_now, "
                        " edge_entry, minute_entry, wall_ts, result) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (r.get("tip_key"), r.get("match_id"),
                         r.get("market"), r.get("label"),
                         r.get("odd_entry"), r.get("odd_now"),
                         r.get("edge_entry"), r.get("minute_entry"),
                         r.get("wall_ts"), r.get("result")),
                    )
                    if cur.rowcount > 0:
                        inserted_tips += 1
                except Exception:
                    pass
    except Exception as e:
        log.error(f"_restore_tips_from_supabase write failed: {e}")
        return {"ok": False, "reason": "local_write_failed", "error": str(e)[:200]}

    log.info(f"_restore_tips_from_supabase: tips={inserted_tips} games={inserted_games}")
    return {"ok": True, "restored": inserted_tips,
            "games_restored": inserted_games,
            "fetched": len(all_rows)}


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
        CREATE TABLE IF NOT EXISTS inbet_subscribers (
            chat_id            INTEGER PRIMARY KEY,
            member_uuid        TEXT UNIQUE NOT NULL,
            plan_status        TEXT,
            locale             TEXT DEFAULT 'en',
            status_checked_at  INTEGER,
            linked_at          INTEGER NOT NULL,
            active             INTEGER DEFAULT 1,
            paused_by_user     INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_inbet_member ON inbet_subscribers(member_uuid);
        CREATE INDEX IF NOT EXISTS idx_inbet_active ON inbet_subscribers(active);
        CREATE TABLE IF NOT EXISTS inbet_status_audit (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            member_uuid  TEXT NOT NULL,
            old_status   TEXT,
            new_status   TEXT,
            old_active   INTEGER,
            new_active   INTEGER,
            changed_at   INTEGER NOT NULL,
            source       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_inbet_audit_member ON inbet_status_audit(member_uuid);
        CREATE INDEX IF NOT EXISTS idx_inbet_audit_ts ON inbet_status_audit(changed_at);
        -- X (Twitter) auto-publishing audit trail. Every tweet our bot
        -- fires is logged here so we can:
        --   1. Reply to the original tweet when the pick settles (resolution
        --      tweet) — needs the original tweet_id.
        --   2. Dedup picks across short windows (avoid double-tweeting if a
        --      pick fires twice within 60s).
        --   3. Rate-limit at the source (count rows in last hour for the
        --      max-8/hour guard).
        --   4. Audit any tweet from the admin UI.
        --
        -- `tip_id` is our internal id from the `tips` table (NULL during
        -- the very brief window before the tip row is committed — most
        -- inserts have it). `tweet_id` is the X-side numeric tweet id.
        -- `resolved_at` / `resolution_tweet_id` are filled by the resolver
        -- cron when the underlying pick settles.
        CREATE TABLE IF NOT EXISTS x_tweets (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            tip_id                INTEGER,
            match_id              INTEGER,
            market                TEXT,
            label                 TEXT,
            tweet_id              TEXT,
            posted_at             INTEGER NOT NULL,
            resolved_at           INTEGER,
            resolution_tweet_id   TEXT,
            error                 TEXT,
            dry_run               INTEGER DEFAULT 0,
            hashtag_variant       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_x_tweets_tip       ON x_tweets(tip_id);
        CREATE INDEX IF NOT EXISTS idx_x_tweets_match     ON x_tweets(match_id);
        CREATE INDEX IF NOT EXISTS idx_x_tweets_posted    ON x_tweets(posted_at);
        CREATE INDEX IF NOT EXISTS idx_x_tweets_unresolved
            ON x_tweets(resolved_at) WHERE resolved_at IS NULL;
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

        CREATE TABLE IF NOT EXISTS affiliate_clicks (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           INTEGER NOT NULL,
            bookmaker    TEXT NOT NULL,
            match_id     INTEGER,
            market       TEXT,
            label        TEXT,
            odd          REAL,
            lang         TEXT,
            source       TEXT,
            ip_country   TEXT,
            user_agent   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_affiliate_clicks_ts ON affiliate_clicks(ts);
        CREATE INDEX IF NOT EXISTS idx_affiliate_clicks_match ON affiliate_clicks(match_id);

        -- Bandwidth tracking. Every outbound _session.get/_session.post is
        -- wrapped to record response size against the destination host.
        -- Rolled up by hour to keep row count manageable (~24 rows/day/host).
        -- Powers /api/admin/bandwidth/stats so we can size proxy plans
        -- accurately before paying for residential bandwidth.
        CREATE TABLE IF NOT EXISTS bandwidth_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_hour       INTEGER NOT NULL,
            host          TEXT NOT NULL,
            bytes_in      INTEGER NOT NULL DEFAULT 0,
            request_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(ts_hour, host)
        );
        CREATE INDEX IF NOT EXISTS idx_bandwidth_log_ts ON bandwidth_log(ts_hour);

        -- Short links for Telegram CTAs. The inline button URL on Telegram
        -- shows up in the "Open this link?" confirmation modal — a full
        -- /go/bet URL with 7 query params looks spammy, so we proxy it
        -- through /r/<code> which 302s to the real URL. Codes are 6-char
        -- base62-ish (URL-safe random). One row per fired pick.
        CREATE TABLE IF NOT EXISTS short_links (
            code        TEXT PRIMARY KEY,
            target_url  TEXT NOT NULL,
            created_at  INTEGER NOT NULL,
            clicks      INTEGER NOT NULL DEFAULT 0,
            source      TEXT  -- 'telegram-betradar', 'web-match-page', etc.
        );
        CREATE INDEX IF NOT EXISTS idx_short_links_created ON short_links(created_at);
        -- Web Push (PWA) subscribers. One row per (endpoint), endpoint is the
        -- unique browser/FCM/APNs push URL. p256dh+auth = the public encryption
        -- keys the browser hands us, used to encrypt the push payload. We mark
        -- rows inactive (active=0) on 410 Gone / 404 from the push service
        -- instead of deleting — keeps stats honest and allows re-subscribe.
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint      TEXT UNIQUE NOT NULL,
            p256dh        TEXT NOT NULL,
            auth          TEXT NOT NULL,
            locale        TEXT DEFAULT 'en',
            user_agent    TEXT,
            created_at    INTEGER NOT NULL,
            last_seen_at  INTEGER,
            last_sent_at  INTEGER,
            send_count    INTEGER DEFAULT 0,
            fail_count    INTEGER DEFAULT 0,
            active        INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_push_subs_active ON push_subscriptions(active);
        -- Auto-failover audit trail. One row per attempt (success or fail).
        -- Used by /api/admin/failover/status and for post-mortems.
        CREATE TABLE IF NOT EXISTS failover_audit (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      INTEGER NOT NULL,
            finished_at     INTEGER,
            from_region     TEXT,
            to_region       TEXT,
            reason          TEXT,
            status          TEXT,    -- 'success' | 'failed'
            new_machine_id  TEXT,
            new_volume_id   TEXT,
            error_message   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_failover_audit_ts ON failover_audit(started_at);
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

    # Migration: add hashtag_variant column to x_tweets if missing.
    # SQLite has no `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so we
    # check via pragma + add only if absent. Safe to call on every boot.
    try:
        with _db() as conn:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(x_tweets)").fetchall()]
            if "hashtag_variant" not in cols:
                conn.execute("ALTER TABLE x_tweets ADD COLUMN hashtag_variant TEXT")
                log.info("DB migration: added x_tweets.hashtag_variant")
    except Exception as mig_err:
        log.warning(f"x_tweets.hashtag_variant migration skipped: {mig_err}")

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
                # Telegram notification for new tip — message body +
                # CTA as a real inline button (short-link URL inside).
                if match:
                    try:
                        text, kb = _format_pick_alert(match, p, minute, shots=shots)
                        _send_telegram(text, buttons=kb)
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

            # Inject logos inline — zero extra requests from the frontend.
            # Pass country so homonyms like Athletic Club (Spain/Brazil)
            # resolve to the right crest.
            _country = m.get("country") or ""
            m["home_logo"] = _quick_logo(m.get("home_team", ""), _country or None)
            m["away_logo"] = _quick_logo(m.get("away_team", ""), _country or None)

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
    # Catch matches we never observed live (Sofascore was blocked during their
    # kickoff window) — direct event-fetch retry, with eventual void after 36h
    # so a tip never stays 'pending' forever.
    try:
        _recover_stale_game_results()
    except Exception as e:
        log.warning(f"_recover_stale_game_results failed: {e}")
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


def _recover_stale_game_results(min_hours_after_kickoff: int = 4,
                                  give_up_after_hours: int = 36):
    """Safety net for matches whose final state we never observed live.

    A game might never enter _live_state if Sofascore blocks our region
    during its kickoff window. Without intervention its tips stay pending
    forever. This helper:

      1. Finds games with pending tips whose scheduled kickoff was at least
         `min_hours_after_kickoff` ago AND that aren't marked finished yet.
      2. For each, hits Sofascore /event/{id} directly via _session
         (curl_cffi — passes anti-bot). On success: writes the final score
         + is_finished=1 and lets _resolve_finished_tips do its job.
      3. If we still can't reach Sofascore after `give_up_after_hours`
         (~36h), gives up and marks the tip as 'void' — better to refund
         than to keep it pending forever.

    Runs every BG cycle (cheap when there's nothing stale).
    """
    now_ts = int(time.time())
    min_age = now_ts - min_hours_after_kickoff * 3600
    give_up = now_ts - give_up_after_hours * 3600
    try:
        with _db() as conn:
            stale = conn.execute(
                "SELECT g.id, g.start_ts, g.home_team, g.away_team "
                "FROM games g WHERE g.is_finished = 0 AND g.start_ts < ? "
                "AND EXISTS (SELECT 1 FROM tips t WHERE t.match_id = g.id AND t.result IS NULL) "
                "ORDER BY g.start_ts ASC LIMIT 30",
                (min_age,)
            ).fetchall()
    except Exception as e:
        log.warning(f"_recover_stale_game_results: query failed: {e}")
        return

    if not stale:
        return
    log.info(f"_recover_stale_game_results: {len(stale)} stale games to retry")

    recovered, voided = 0, 0
    for g in stale:
        gid       = g["id"]
        start_ts  = int(g["start_ts"] or 0)
        is_ancient = start_ts > 0 and start_ts < give_up

        # Try to fetch the event directly from Sofascore — works if the
        # current region isn't blocked even if it was during the live window.
        ev = None
        try:
            r = _session.get(f"{SOFASCORE_API}/event/{gid}", timeout=10)
            if r.status_code == 200:
                d = r.json()
                ev = d.get("event") or d
        except Exception as e:
            log.debug(f"_recover_stale_game_results: fetch failed for {gid}: {e}")

        if ev:
            status = ((ev.get("status") or {}).get("type") or "").lower()
            hg = (ev.get("homeScore") or {}).get("current")
            ag = (ev.get("awayScore") or {}).get("current")
            if status == "finished" and hg is not None and ag is not None:
                try:
                    with _db() as conn:
                        conn.execute(
                            "UPDATE games SET is_finished = 1, home_goals = ?, "
                            "away_goals = ?, archived_at = ? WHERE id = ?",
                            (int(hg), int(ag), now_ts, gid)
                        )
                    recovered += 1
                    log.info(f"_recover_stale_game_results: recovered #{gid} "
                              f"{g['home_team']} {hg}-{ag} {g['away_team']}")
                    continue
                except Exception as e:
                    log.warning(f"_recover_stale_game_results: db write failed for {gid}: {e}")

        # Couldn't recover AND game is ancient → void the tips so the user
        # gets stake refunded rather than seeing 'pending' forever.
        if is_ancient:
            try:
                with _db() as conn:
                    conn.execute(
                        "UPDATE tips SET result = 'void' WHERE match_id = ? AND result IS NULL",
                        (gid,)
                    )
                voided += 1
                log.warning(f"_recover_stale_game_results: gave up on #{gid} "
                            f"({g['home_team']} vs {g['away_team']}) — voided pending tips")
            except Exception as e:
                log.warning(f"_recover_stale_game_results: void write failed for {gid}: {e}")

    if recovered or voided:
        log.info(f"_recover_stale_game_results: recovered={recovered}, voided={voided}")
        _resolve_finished_tips()


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

    # After settling, fan out the BetRadar match recap for matches that just
    # crossed the +2.5u (€250) profit threshold. Dedup'd via DB so each match
    # is broadcast only once even if _resolve_finished_tips runs again.
    try:
        _maybe_broadcast_match_recaps()
    except Exception as e:
        log.error(f"_maybe_broadcast_match_recaps failed: {e}")


# Threshold above which a finished match's recap is auto-broadcast to all
# BetRadarAI subscribers. 2.5u with 100€ stake = €250 profit.
BETRADAR_RECAP_THRESHOLD_U = 2.5


def _maybe_broadcast_match_recaps():
    """Find finished matches whose settled profit is ≥ threshold AND that
    haven't been broadcast yet. Generate the animated recap once per match
    and fan out to all subscribers (uploads once, forwards via file_id)."""
    if not TELEGRAM_BOT_TOKEN:
        return
    STAKE = get_setting("stake_per_bet", 100.0) or 100.0

    # Make sure the dedup table exists (idempotent).
    try:
        with _db() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS betradar_recap_sent ("
                "  match_id INTEGER PRIMARY KEY, "
                "  profit_u REAL, "
                "  sent_at  INTEGER NOT NULL"
                ")"
            )
    except Exception as e:
        log.warning(f"_maybe_broadcast_match_recaps: dedup table init failed: {e}")
        return

    # Pull eligible candidates: finished games with at least one settled tip
    # whose summed profit clears the threshold AND no broadcast row yet.
    # Recency filter: only matches kicked off in the last 36h, so a fresh
    # deploy (or a revived dedup table) doesn't retroactively spam old wins.
    cutoff_ts = int(time.time()) - 36 * 3600
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT g.id, "
                "       SUM(CASE WHEN t.result IN ('green','win') THEN (t.odd_entry - 1) "
                "                WHEN t.result IN ('red','loss')  THEN -1 ELSE 0 END) AS profit_u "
                "FROM games g JOIN tips t ON t.match_id = g.id "
                "WHERE g.is_finished = 1 AND t.result IS NOT NULL "
                "  AND g.start_ts >= ? "
                "  AND NOT EXISTS (SELECT 1 FROM betradar_recap_sent s WHERE s.match_id = g.id) "
                "GROUP BY g.id "
                "HAVING profit_u >= ?",
                (cutoff_ts, BETRADAR_RECAP_THRESHOLD_U)
            ).fetchall()
    except Exception as e:
        log.warning(f"_maybe_broadcast_match_recaps: candidate query failed: {e}")
        return

    if not rows:
        return

    log.info(f"BetRadar recap: {len(rows)} match(es) cross threshold, broadcasting…")
    subs = _tg_subscribers() or []
    chat_ids = [int(c) for c in subs if str(c).lstrip("-").isdigit()]

    # Lazy-import the recap builder so this hot-path stays cheap on the
    # 99% of cycles where no match qualifies.
    sys.path.insert(0, os.path.dirname(__file__))
    try:
        from tools.build_match_recap import build_recap  # type: ignore
    except Exception as e:
        log.error(f"BetRadar recap: import builder failed: {e}")
        return

    # Same logo-fetch helper used by the admin endpoint — TLS-impersonated
    # via curl_cffi so Sofascore's CDN actually serves the image.
    def _fetch_logo_img(team_id):
        if not team_id or not _session:
            return None
        try:
            url = f"https://api.sofascore.app/api/v1/team/{team_id}/image"
            resp = _session.get(url, timeout=6)
            if resp.status_code != 200 or not resp.content:
                return None
            from PIL import Image as _PIL
            import io as _io
            img = _PIL.open(_io.BytesIO(resp.content)).convert("RGBA")
            img.thumbnail((192, 192), _PIL.LANCZOS)
            return img
        except Exception as e:
            log.warning(f"recap logo fetch failed team_id={team_id}: {e}")
            return None

    now_ts = int(time.time())
    for r in rows:
        mid = r["id"]
        # CLAIM-FIRST: insert the dedup row before any heavy work. INSERT OR
        # IGNORE means a concurrent caller (e.g. an admin/resolve while the
        # BG cycle is also running) gets rowcount=0 and bails — guarantees
        # exactly-once broadcast even under races. The row is updated with
        # the real profit_u after the broadcast succeeds.
        try:
            with _db() as conn:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO betradar_recap_sent "
                    "(match_id, profit_u, sent_at) VALUES (?, ?, ?)",
                    (mid, float(r["profit_u"]), now_ts)
                )
                if cur.rowcount == 0:
                    log.info(f"BetRadar recap: match #{mid} already claimed, skipping")
                    continue
        except Exception as e:
            log.error(f"BetRadar recap: claim failed for match #{mid}: {e}")
            continue

        try:
            # Pull team_ids for logo fetch.
            with _db() as conn:
                team_ids = conn.execute(
                    "SELECT home_team_id, away_team_id FROM games WHERE id = ?",
                    (mid,)
                ).fetchone()
            logo_home_img = _fetch_logo_img(team_ids["home_team_id"]) if team_ids else None
            logo_away_img = _fetch_logo_img(team_ids["away_team_id"]) if team_ids else None

            out_path = f"/tmp/betradar_auto_{mid}.mp4"
            result = build_recap(mid, out_path, db_path=str(DB_PATH),
                                  home_logo_img=logo_home_img,
                                  away_logo_img=logo_away_img)
            actual_path = result.split(" (", 1)[0] if isinstance(result, str) else out_path
            with open(actual_path, "rb") as fh:
                anim_bytes = fh.read()
            caption  = _betradar_match_caption(mid)
            filename = os.path.basename(actual_path)

            if chat_ids:
                stats = _broadcast_telegram_animation(
                    chat_ids, anim_bytes, caption=caption,
                    buttons=_betradar_share_buttons(),
                    filename=filename,
                )
                log.info(
                    f"BetRadar recap: match #{mid} (+{r['profit_u']:.2f}u) — "
                    f"upload {stats['sent_with_upload']}, by_id {stats['sent_with_id']}, "
                    f"failed {stats['failed']}"
                )
        except Exception as e:
            log.error(f"BetRadar recap: match #{mid} broadcast failed: {e}")
            # Roll back the claim so a future cycle can retry. Without this,
            # a transient curl_cffi error would silently swallow the recap.
            try:
                with _db() as conn:
                    conn.execute("DELETE FROM betradar_recap_sent WHERE match_id = ?", (mid,))
            except Exception:
                pass


def _background_loop():
    """Runs forever, sleeping BG_INTERVAL seconds between cycles."""
    # Stagger first cycle by 5s to let Gunicorn/Flask finish starting
    time.sleep(5)
    while True:
        try:
            _run_background_cycle()
        except Exception as e:
            log.error(f"BG loop unhandled error: {e}")
        # Persist accumulated bandwidth counters once per cycle. Cheap
        # — one SQLite INSERT per active host (usually ≤5). Never raises.
        try:
            _flush_bw_counters()
        except Exception:
            pass
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


# In-memory cache for /api/today/monitored. Today's fixture list doesn't
# change minute-by-minute (kickoffs are scheduled hours ahead, only the
# isFinished/isLive flags flip during play — and live data is already
# served by /api/state). 60s TTL gives a ~150ms cache-HIT path on the
# home critical chain (was 0.4-2.5s cold hit to Sofascore).
_today_monitored_cache: dict = {}   # date_str → (ts, response_dict)
_TODAY_MONITORED_TTL = 60


@app.route("/api/today/monitored")
def r_today_monitored():
    """Scheduled games for a given date (default: today) for monitored leagues only.

    Response is trimmed to the fields the home/today UI actually renders —
    Sofascore's raw `tournament` object alone is ~2 KB per match, ×100
    matches that's 200 KB of bloat that just inflates page weight without
    being displayed anywhere.
    """
    try:
        # Parse optional date parameter (YYYY-MM-DD)
        date_str = flask_request.args.get("date")
        if not date_str:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Cache fast lane (skip Sofascore + filtering + trimming on hot path).
        cached = _today_monitored_cache.get(date_str)
        if cached and (time.time() - cached[0]) < _TODAY_MONITORED_TTL:
            resp = jsonify(cached[1])
            resp.headers["Cache-Control"] = f"public, max-age={_TODAY_MONITORED_TTL}, s-maxage={_TODAY_MONITORED_TTL}"
            resp.headers["X-Cache"] = "HIT"
            return resp

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
            # Sofascore exposes country as `tournament.category.name`, NOT as a
            # top-level `event.country` field. Reading the wrong path silently
            # returned "" → _is_monitored_league_strict rejected every league
            # that requires a country match (LaLiga/Premier/Bundesliga/etc.)
            # so the "agendados hoje" tab dropped most of the day's fixtures.
            if isinstance(tourn, dict):
                country_name = (tourn.get("category") or {}).get("name", "")
            else:
                country_name = ""

            if _is_monitored_league_strict(tourn_name, country_name):
                # Extract team names from team objects if needed
                home_name = m.get("homeTeam")
                if isinstance(home_name, dict):
                    home_name = home_name.get("name", "")
                away_name = m.get("awayTeam")
                if isinstance(away_name, dict):
                    away_name = away_name.get("name", "")

                # Trim to the only fields the home/today UI actually consumes.
                # Cuts payload ~70% (305 KB → ~95 KB) without losing anything
                # rendered. Re-fetch full data via /api/match/<id> on detail page.
                trimmed = {
                    "id":             m.get("id"),
                    "homeTeam":       home_name,
                    "awayTeam":       away_name,
                    "startTimestamp": ts,
                    "tournament":     tourn_name,
                    "country":        country_name,
                    "slug":           m.get("slug"),
                    "status":         m.get("status") or {},
                    "isFinished":     bool(m.get("isFinished")),
                    "isLive":         bool(m.get("isLive")),
                    "_sport_key":     _resolve_sport_key(tourn_name, country_name),
                    # Country-aware logos so homonyms (Athletic Club, etc.)
                    # resolve to the right crest. Cheap — exact lookup only.
                    "home_logo":      _quick_logo(home_name, country_name or None),
                    "away_logo":      _quick_logo(away_name, country_name or None),
                }
                result.append(trimmed)
        result.sort(key=lambda m: m.get("startTimestamp") or 0)
        body = {"count": len(result), "matches": result, "date": date_str}
        _today_monitored_cache[date_str] = (time.time(), body)
        resp = jsonify(body)
        resp.headers["Cache-Control"] = f"public, max-age={_TODAY_MONITORED_TTL}, s-maxage={_TODAY_MONITORED_TTL}"
        resp.headers["X-Cache"] = "MISS"
        return resp
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

    # Resolve a league priority for any tournament — falls back through
    # competitions DB → hardcoded map → 99 (see _league_priority()).
    def _priority_for(tournament: str, country: str) -> int:
        try:
            return _league_priority(_resolve_sport_key(tournament, country))
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
                    "_priority": _league_priority(sk),
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
        # Sofascore exposes country as `tournament.category.name`, NOT as a
        # top-level `event.country` field. The previous code read
        # `m.get("country", {})` and got empty strings — which broke the
        # country flag emoji on the daily preview message and elsewhere.
        if isinstance(tourn, dict):
            country_name = (tourn.get("category") or {}).get("name", "")
        else:
            country_name = ""

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
            "home_logo":      _quick_logo(home, country_name or None),
            "away_logo":      _quick_logo(away, country_name or None),
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
                f"FROM match_shots WHERE match_id IN ({ids_csv}) AND minute >= 0 "
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
                # 'score' uses the universal home-first convention so the
                # SPA can render "Grêmio 0-1 Flamengo" naturally. Earlier
                # this field was in team-perspective (gf-ga), which made
                # the SPA show "1-0" for an away game and read like the
                # home team had scored first — wrong.
                "score":      f"{g['home_goals']}-{g['away_goals']}",
                # 'team_score' / 'opp_score' for callers who want the
                # team-perspective view explicitly.
                "team_score": gf,
                "opp_score":  ga,
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


def _league_performance(variants: list[str], recent_days: int = 30,
                         locale: str = "en") -> dict:
    """
    Aggregate league-wide stats from our local DB. Mirrors the shape of
    `_team_performance` but at competition level. Drives the SEO copy on
    /league/<slug> pages.

    Returns:
      matches_tracked:     number of finished games we observed
      goals_per_match:     average total goals across observed games
      avg_xg_per_match:    average TOTAL xG per game (home + away)
      track_record:        {settled, wins, losses, pnl, roi, avg_odds}
      top_attack:          [{team, avg_goals_for}, ...] top 3 scorers we saw
      recent_form_text:    short factual sentence about the last N days
      sample_size:         total finished games used
    """
    out = {
        "matches_tracked":  0,
        "goals_per_match":  None,
        "avg_xg_per_match": None,
        "track_record":     None,
        "top_attack":       [],
        "recent_form_text": "",
        "sample_size":      0,
    }
    if not variants:
        return out
    placeholders = ",".join("?" * len(variants))
    try:
        with _db() as conn:
            # 1. Finished games in this league
            games = conn.execute(
                f"SELECT id, home_team, away_team, home_goals, away_goals, start_ts "
                f"FROM games WHERE is_finished = 1 "
                f"AND home_goals IS NOT NULL AND away_goals IS NOT NULL "
                f"AND tournament IN ({placeholders})",
                tuple(variants)
            ).fetchall()
            if not games:
                return out
            out["matches_tracked"] = len(games)
            out["sample_size"]     = len(games)
            total_goals = sum((g["home_goals"] or 0) + (g["away_goals"] or 0) for g in games)
            out["goals_per_match"] = round(total_goals / len(games), 2)

            # 2. Average xG per match (only games that have shot data)
            ids_csv = ",".join(str(g["id"]) for g in games)
            xg_rows = conn.execute(
                f"SELECT match_id, SUM(xg) AS total_xg "
                f"FROM match_shots WHERE match_id IN ({ids_csv}) AND minute >= 0 "
                f"GROUP BY match_id"
            ).fetchall()
            if xg_rows:
                avg_xg = sum((r["total_xg"] or 0) for r in xg_rows) / len(xg_rows)
                out["avg_xg_per_match"] = round(avg_xg, 2)

            # 3. Track record on picks for this league
            STAKE = get_setting("stake_per_bet", 100.0) or 100.0
            tip_rows = conn.execute(
                f"SELECT t.result, t.odd_entry FROM tips t "
                f"JOIN games g ON g.id = t.match_id "
                f"WHERE g.tournament IN ({placeholders}) "
                f"AND t.result IN ('green', 'red', 'win', 'loss', 'void')",
                tuple(variants)
            ).fetchall()
            if tip_rows:
                wins   = sum(1 for r in tip_rows if (r["result"] or "").lower() in ("green", "win"))
                losses = sum(1 for r in tip_rows if (r["result"] or "").lower() in ("red", "loss"))
                voids  = sum(1 for r in tip_rows if (r["result"] or "").lower() == "void")
                settled_for_pnl = wins + losses  # voids contribute 0 to pnl
                pnl = 0.0
                odd_sum, odd_n = 0.0, 0
                for r in tip_rows:
                    if r["odd_entry"]:
                        odd_sum += r["odd_entry"]; odd_n += 1
                    res = (r["result"] or "").lower()
                    if res in ("green", "win") and r["odd_entry"]:
                        pnl += (r["odd_entry"] - 1) * STAKE
                    elif res in ("red", "loss"):
                        pnl -= STAKE
                roi = (pnl / (settled_for_pnl * STAKE) * 100) if settled_for_pnl else 0.0
                out["track_record"] = {
                    "settled":  wins + losses + voids,
                    "wins":     wins,
                    "losses":   losses,
                    "voids":    voids,
                    "pnl":      round(pnl, 0),
                    "roi":      round(roi, 1),
                    "avg_odds": round(odd_sum / odd_n, 2) if odd_n else 0.0,
                }

            # 4. Top scoring teams (avg goals for, per match) — needs ≥2 games
            from collections import defaultdict
            team_goals = defaultdict(lambda: {"games": 0, "goals": 0})
            for g in games:
                team_goals[g["home_team"]]["games"] += 1
                team_goals[g["home_team"]]["goals"] += g["home_goals"] or 0
                team_goals[g["away_team"]]["games"] += 1
                team_goals[g["away_team"]]["goals"] += g["away_goals"] or 0
            ranked = sorted(
                ((t, d["games"], d["goals"] / d["games"]) for t, d in team_goals.items() if d["games"] >= 2),
                key=lambda x: -x[2]
            )
            out["top_attack"] = [
                {"team": t, "matches": n, "avg_goals_for": round(g, 2)}
                for t, n, g in ranked[:3]
            ]

            # 5. Recent activity in last N days — locale-aware phrasing.
            now_ts = int(time.time())
            cutoff = now_ts - recent_days * 86400
            recent = [g for g in games if (g["start_ts"] or 0) >= cutoff]
            if recent:
                rg = sum((g["home_goals"] or 0) + (g["away_goals"] or 0) for g in recent) / len(recent)
                n = len(recent)
                # PT-BR uses comma as decimal separator; EN/ES use dot.
                if locale == "pt-br":
                    rg_str = f"{rg:.2f}".replace(".", ",")
                    out["recent_form_text"] = (
                        f"{n} jogos nos últimos {recent_days} dias "
                        f"com média de {rg_str} gols por partida"
                    )
                elif locale == "pt-pt":
                    rg_str = f"{rg:.2f}".replace(".", ",")
                    out["recent_form_text"] = (
                        f"{n} jogos nos últimos {recent_days} dias "
                        f"com uma média de {rg_str} golos por jogo"
                    )
                elif locale == "es":
                    rg_str = f"{rg:.2f}".replace(".", ",")
                    out["recent_form_text"] = (
                        f"{n} partidos en los últimos {recent_days} días "
                        f"con un promedio de {rg_str} goles por partido"
                    )
                else:
                    out["recent_form_text"] = (
                        f"{n} matches in the last {recent_days} days "
                        f"averaging {rg:.2f} goals per game"
                    )
    except Exception as e:
        log.warning(f"_league_performance failed: {e}")
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


# ════════════════════════════════════════════════════════════════════════════
#  WC 2026 widget data — state machine + tournament performance
# ════════════════════════════════════════════════════════════════════════════

def _build_xg_timeline_from_entry(entry: dict) -> list:
    """Construct a cumulative xG timeline [{minute, home, away}, …] from a live
    state entry. Returns [] if no usable shots data. Penalties are excluded
    from the cumulative total to match how we compute team xG elsewhere.

    The live_state stores shots as the dict returned by get_shotmap():
        {"homeShots":[…], "awayShots":[…], "homeXg":N, "awayXg":N, …}
    We also accept a flat list-of-shots shape for forward-compat. Each shot
    has `minute`, `xg`, `isPenalty`, and (for the flat shape) `isHome`.
    """
    if not entry:
        return []
    shots = entry.get("shots")
    if not shots:
        return []

    # Normalise to a flat [(shot_dict, is_home), …] list regardless of shape.
    flat = []
    if isinstance(shots, dict):
        for s in (shots.get("homeShots") or []):
            flat.append((s, True))
        for s in (shots.get("awayShots") or []):
            flat.append((s, False))
    elif isinstance(shots, list):
        for s in shots:
            flat.append((s, bool(s.get("isHome"))))
    else:
        return []

    rows = []
    for s, is_home in flat:
        m = s.get("minute")
        if m is None:
            continue
        if s.get("isPenalty"):
            continue
        rows.append((int(m) + (int(s.get("addedTime") or 0) or 0) / 60.0,
                     is_home,
                     float(s.get("xg") or 0)))
    rows.sort(key=lambda r: r[0])
    timeline = [{"minute": 0, "home": 0.0, "away": 0.0}]
    cum_h, cum_a = 0.0, 0.0
    for minute, is_home, xg in rows:
        if is_home:
            cum_h += xg
        else:
            cum_a += xg
        timeline.append({
            "minute": int(round(minute)),
            "home":   round(cum_h, 3),
            "away":   round(cum_a, 3),
        })
    return timeline


def _wc2026_current_state(locale: str = "en", demo: bool = False) -> dict:
    """
    Decide which of the 5 widget states to render for the inbet WC 2026
    per-match widget, given the current clock + DB state.

    States:
      live                 — a WC match is in progress
      results_profitable   — last WC match finished, day P&L > 0; show until 1h before next
      results_losing       — last WC match finished, day P&L ≤ 0; show for 15 min then preview
      preview              — next WC match upcoming, show pre-match card
      off_day              — no WC match in next 24h

    Args:
      demo — when True, drops the WC tournament filter AND the WC date window
             so the same state machine can run against ANY currently
             monitored fixture. Used by the staging widget URL so dev
             teams can rehearse end-to-end against live non-WC games
             before the tournament starts.
    """
    from datetime import datetime, timezone, timedelta
    now_ts = int(time.time())
    variants = _wc_tournament_variants()
    variant_norms = {_normalize_tournament(v) for v in variants}

    def _is_wc(tourn: str) -> bool:
        # Demo mode: accept everything (any tournament passes the filter).
        if demo:
            return True
        return _normalize_tournament(tourn or "") in variant_norms

    # 1. Currently-live match (WC by default; any monitored match in demo mode).
    # Pick the FIRST one we find — for demo this means the top of _live_state.
    live_wc_entry = None
    try:
        with _state_lock:
            for entry in _live_state.values():
                m = entry.get("match") or {}
                if m.get("isFinished") or m.get("homeGoals") is None:
                    continue
                if _is_wc(m.get("tournament", "")):
                    live_wc_entry = entry
                    break
    except Exception as e:
        log.warning(f"_wc2026_current_state: live scan failed: {e}")

    # 2. Next scheduled WC matches (from _upcoming_cache, next 4 days).
    # Collects ALL upcoming WC fixtures in window so the off_day card can
    # render the next 3, then `next_match` is just the chronologically first
    # one (same value as before for backwards compatibility).
    upcoming_wc = []
    now_utc = datetime.now(timezone.utc)
    for offset in range(0, 4):
        date_str = (now_utc + timedelta(days=offset)).strftime("%Y-%m-%d")
        cached = _upcoming_cache.get(date_str)
        if not cached:
            continue
        for m in cached.get("matches", []):
            ts = m.get("startTimestamp") or 0
            if ts <= now_ts:
                continue
            # WC date-window filter only applies in real WC mode; demo mode
            # accepts any monitored upcoming fixture.
            if not demo and not (WC2026_START_TS <= ts <= WC2026_END_TS):
                continue
            if not _is_wc(m.get("tournament", "")):
                continue
            upcoming_wc.append((ts, m))
    upcoming_wc.sort(key=lambda x: x[0])
    next_match = upcoming_wc[0][1] if upcoming_wc else None
    next_kickoff_ts = upcoming_wc[0][0] if upcoming_wc else None

    # 3. Most recently finished WC match (from DB)
    last_finished = None
    last_finished_picks = []
    last_finished_pnl = 0.0
    try:
        STAKE = get_setting("stake_per_bet", 100.0) or 100.0
        placeholders = ",".join("?" * len(variants))
        with _db() as conn:
            if demo:
                # Demo: most recent finished game in the last 24h, ANY tournament.
                # Bounded to the last day so we don't pull a 6-month-old game
                # if there's nothing fresh.
                row = conn.execute(
                    "SELECT id, home_team, away_team, home_goals, away_goals, "
                    "       start_ts, tournament, country, archived_at "
                    "FROM games WHERE is_finished = 1 "
                    "AND start_ts >= ? "
                    "ORDER BY start_ts DESC LIMIT 1",
                    (now_ts - 86400,)
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT id, home_team, away_team, home_goals, away_goals, "
                    f"       start_ts, tournament, country, archived_at "
                    f"FROM games WHERE is_finished = 1 "
                    f"AND tournament IN ({placeholders}) "
                    f"AND start_ts >= ? AND start_ts <= ? "
                    f"ORDER BY start_ts DESC LIMIT 1",
                    (*variants, WC2026_START_TS, WC2026_END_TS)
                ).fetchone()
            if row:
                last_finished = dict(row)
                pick_rows = conn.execute(
                    "SELECT market, label, odd_entry, odd_now, edge_entry, "
                    "       minute_entry, result, wall_ts "
                    "FROM tips WHERE match_id = ? "
                    "ORDER BY wall_ts ASC",
                    (row["id"],)
                ).fetchall()
                last_finished_picks = [dict(p) for p in pick_rows]
                for p in last_finished_picks:
                    res = (p.get("result") or "").lower()
                    odd = p.get("odd_entry") or 0
                    if res in ("green", "win") and odd:
                        last_finished_pnl += (odd - 1) * STAKE
                    elif res in ("red", "loss"):
                        last_finished_pnl -= STAKE
    except Exception as e:
        log.warning(f"_wc2026_current_state: last_finished query failed: {e}")

    # ───── Decide state
    state = "off_day"
    match_payload = None
    picks_payload = []
    match_pnl = None
    countdown_s = None
    model_preview_text = None

    # LIVE branch
    if live_wc_entry is not None:
        state = "live"
        m = live_wc_entry.get("match") or {}
        match_payload = {
            "id":          m.get("id"),
            "home":        m.get("homeTeam"),
            "away":        m.get("awayTeam"),
            "home_goals":  m.get("homeGoals", 0) or 0,
            "away_goals":  m.get("awayGoals", 0) or 0,
            "minute":      m.get("minute"),
            "country":     m.get("country", ""),
            "tournament":  m.get("tournament", ""),
            "is_finished": False,
        }
        live_picks = live_wc_entry.get("livePicks") or live_wc_entry.get("tips") or []
        picks_payload = [
            {
                "market":  p.get("market", ""),
                "label":   p.get("label", ""),
                "odds":    p.get("odds") or p.get("odd_entry") or 0,
                "edge":    p.get("edge") or p.get("edge_entry") or 0,
                "minute":  p.get("minute_entry") if p.get("minute_entry") is not None else p.get("minute"),
                "result":  p.get("result"),
            }
            for p in live_picks
        ]
        if next_kickoff_ts:
            countdown_s = max(0, next_kickoff_ts - now_ts)

    # RESULTS branch
    elif last_finished:
        # Use archived_at if present, else assume kickoff + 2h
        finished_at_ts = (last_finished.get("archived_at")
                          or (last_finished.get("start_ts") or 0) + 7200)
        finished_age = now_ts - finished_at_ts
        time_to_next = (next_kickoff_ts - now_ts) if next_kickoff_ts else None
        is_profitable = last_finished_pnl > 0

        show_results = False
        if is_profitable:
            # Keep on screen until 1h before next kickoff (or indefinitely if no next match)
            if time_to_next is None or time_to_next > 3600:
                show_results = True
        else:
            # Only for 15 min after final whistle
            if finished_age < 900:
                show_results = True

        if show_results:
            state = "results_profitable" if is_profitable else "results_losing"
            match_payload = {
                "id":          last_finished["id"],
                "home":        last_finished["home_team"],
                "away":        last_finished["away_team"],
                "home_goals":  last_finished["home_goals"] or 0,
                "away_goals":  last_finished["away_goals"] or 0,
                "country":     last_finished.get("country", ""),
                "tournament":  last_finished.get("tournament", ""),
                "is_finished": True,
            }
            picks_payload = [
                {
                    "market":  p["market"],
                    "label":   p["label"],
                    "odds":    p["odd_entry"],
                    "edge":    p["edge_entry"],
                    "minute":  p["minute_entry"],
                    "result":  p["result"],
                }
                for p in last_finished_picks
            ]
            match_pnl = round(last_finished_pnl, 0)
            if time_to_next is not None:
                countdown_s = max(0, time_to_next)

    # PREVIEW branch — if we didn't choose live/results, see if a kickoff is near
    if state == "off_day" and next_match and next_kickoff_ts:
        time_to_kickoff = next_kickoff_ts - now_ts
        if time_to_kickoff > 0:
            state = "preview"
            countdown_s = time_to_kickoff
            # Cheap model preview line using avg goals from team performance
            try:
                home_name = next_match.get("homeTeam", "")
                away_name = next_match.get("awayTeam", "")
                hp = _team_performance(home_name, recent_n=5)
                ap = _team_performance(away_name, recent_n=5)
                avg_total = 0.0
                count = 0
                for perf in (hp, ap):
                    xg = perf.get("xg_summary")
                    if xg:
                        avg_total += xg["avg_for"]
                        count += 1
                if count >= 1:
                    proj = avg_total / count * 2  # rough 2-team projection
                    key = "model_lean_over" if proj >= 2.5 else "model_lean_under"
                    model_preview_text = _t(locale, key).replace("{xg}", f"{proj:.1f}")
            except Exception:
                pass

    # ALWAYS include the next match in payload if we have it (for countdown / preview)
    next_match_payload = None
    if next_match and next_kickoff_ts:
        next_match_payload = {
            "id":          next_match.get("id"),
            "home":        next_match.get("homeTeam"),
            "away":        next_match.get("awayTeam"),
            "country":     next_match.get("country", ""),
            "tournament":  next_match.get("tournament", ""),
            "kickoff_ts":  next_kickoff_ts,
        }

    # OFF-DAY fallback countdown to next kickoff (could be days away)
    if state == "off_day" and next_kickoff_ts:
        countdown_s = next_kickoff_ts - now_ts

    # Poll frequency
    if state == "live":
        poll_ms = 30_000
    elif state in ("results_profitable", "results_losing"):
        poll_ms = 60_000
    elif state == "preview":
        poll_ms = 60_000 if (countdown_s and countdown_s < 1800) else 120_000
    else:  # off_day
        poll_ms = 300_000

    # xG timeline for the chart in renderLive — only meaningful in LIVE state.
    xg_timeline = []
    if state == "live" and live_wc_entry is not None:
        xg_timeline = _build_xg_timeline_from_entry(live_wc_entry)

    # Next 3 upcoming WC fixtures — used by the off_day card to surface
    # what's coming, instead of just a single countdown.
    upcoming_matches_payload = [
        {
            "id":          m.get("id"),
            "home":        m.get("homeTeam"),
            "away":        m.get("awayTeam"),
            "country":     m.get("country", ""),
            "tournament":  m.get("tournament", ""),
            "kickoff_ts":  ts,
        }
        for (ts, m) in upcoming_wc[:3]
    ]

    return {
        "state":                       state,
        "lang":                        locale,
        "match":                       match_payload,
        "picks":                       picks_payload,
        "match_pnl":                   match_pnl,
        "next_match":                  next_match_payload,
        "upcoming_matches":            upcoming_matches_payload,
        "countdown_to_next_kickoff_s": countdown_s,
        "model_preview_text":          model_preview_text,
        "wc_emblem":                   WC2026_EMBLEM_URL,
        "powered_by":                  _t(locale, "powered_by"),
        "next_poll_after_ms":          poll_ms,
        "now_ts":                      now_ts,
        "xg_timeline":                 xg_timeline,
    }


def _wc2026_performance() -> dict:
    """
    Tournament-scoped aggregate (June 11 → July 19, 2026 window).
    Powers the /widget/wc2026/performance dashboard.
    """
    from datetime import datetime, timezone
    out = {
        "settled":      0,
        "wins":         0,
        "losses":       0,
        "voids":        0,
        "winrate":      0.0,
        "pnl":          0.0,
        "roi":          0.0,
        "avg_odds":     0.0,
        "equity_curve": [],
        "top_greens":   [],
        "by_market":    [],
        "tournament_start": WC2026_START_TS,
        "tournament_end":   WC2026_END_TS,
    }
    variants = _wc_tournament_variants()
    if not variants:
        return out
    STAKE = get_setting("stake_per_bet", 100.0) or 100.0
    placeholders = ",".join("?" * len(variants))
    try:
        with _db() as conn:
            rows = conn.execute(
                f"SELECT t.market, t.label, t.odd_entry, t.result, t.wall_ts, t.minute_entry, "
                f"       g.home_team, g.away_team, g.start_ts, g.tournament "
                f"FROM tips t JOIN games g ON g.id = t.match_id "
                f"WHERE g.tournament IN ({placeholders}) "
                f"AND g.start_ts >= ? AND g.start_ts <= ? "
                f"AND t.result IN ('green','red','void','win','loss') "
                f"ORDER BY t.wall_ts ASC",
                (*variants, WC2026_START_TS, WC2026_END_TS)
            ).fetchall()

        if not rows:
            return out

        wins = losses = voids = 0
        pnl = 0.0
        odd_sum, odd_n = 0.0, 0
        by_market_dict: dict = {}
        equity_pts = []
        cum_pnl = 0.0

        for r in rows:
            res = (r["result"] or "").lower()
            odd = r["odd_entry"] or 0
            if odd:
                odd_sum += odd
                odd_n  += 1
            delta = 0.0
            if res in ("green", "win"):
                wins += 1
                delta = (odd - 1) * STAKE if odd else 0.0
            elif res in ("red", "loss"):
                losses += 1
                delta = -STAKE
            else:
                voids += 1
                delta = 0.0
            pnl     += delta
            cum_pnl += delta
            equity_pts.append({
                "ts":       r["wall_ts"],
                "date":     datetime.fromtimestamp(r["wall_ts"], tz=timezone.utc).strftime("%Y-%m-%d"),
                "cum_pnl":  round(cum_pnl, 0),
            })
            mkt = r["market"] or "Other"
            slot = by_market_dict.setdefault(mkt, {"picks": 0, "pnl": 0.0})
            slot["picks"] += 1
            slot["pnl"]   += delta

        settled = wins + losses + voids
        wl = wins + losses
        out["settled"]  = settled
        out["wins"]     = wins
        out["losses"]   = losses
        out["voids"]    = voids
        out["winrate"]  = round((wins / wl * 100) if wl else 0, 1)
        out["pnl"]      = round(pnl, 0)
        out["roi"]      = round((pnl / (wl * STAKE) * 100) if wl else 0, 1)
        out["avg_odds"] = round(odd_sum / odd_n, 2) if odd_n else 0.0
        out["equity_curve"] = equity_pts
        out["by_market"]    = [
            {"market": k, "picks": v["picks"], "pnl": round(v["pnl"], 0)}
            for k, v in sorted(by_market_dict.items(), key=lambda x: -x[1]["pnl"])
        ]

        greens = []
        for r in rows:
            res = (r["result"] or "").lower()
            if res not in ("green", "win"):
                continue
            odd = r["odd_entry"] or 0
            if not odd:
                continue
            profit = (odd - 1) * STAKE
            greens.append({
                "match":          f"{r['home_team']} vs {r['away_team']}",
                "market":         r["market"],
                "label":          r["label"],
                "odds":           round(odd, 2),
                "minute_entered": r["minute_entry"],
                "profit":         round(profit, 0),
            })
        greens.sort(key=lambda x: -x["profit"])
        out["top_greens"] = greens[:5]
    except Exception as e:
        log.warning(f"_wc2026_performance failed: {e}")
    return out


# ─── MOCK + LIVE-MATCH OVERRIDE (for pre-tournament testing) ─────────────────
# Two test affordances accessible via query params on the widget endpoints:
#   ?mock=<state>      → return synthetic payload for one of the 5 states
#                        (live | results_win | results_loss | preview | off_day)
#   ?match_id=<id>     → bypass the WC tournament filter and treat any currently
#                        monitored live match as the "current" match. Lets us
#                        rehearse the widget on a Premier League / Liga fixture
#                        before the World Cup kicks off.
# Both are opt-in via explicit query param — production traffic is untouched.

# Country / national-team name translations for mock payloads. Real Sofascore
# data is English-only, so this only applies to ?mock= responses. Keys are the
# canonical English form used in the hardcoded mock structures below.
_COUNTRY_I18N = {
    "en": {},  # identity
    "es": {
        "England": "Inglaterra", "Brazil": "Brasil", "Argentina": "Argentina",
        "Spain": "España", "Germany": "Alemania", "Mexico": "México",
        "Portugal": "Portugal", "Croatia": "Croacia",
        "South Africa": "Sudáfrica", "France": "Francia",
        "Netherlands": "Países Bajos", "Uruguay": "Uruguay",
    },
    "pt-pt": {
        "England": "Inglaterra", "Brazil": "Brasil", "Argentina": "Argentina",
        "Spain": "Espanha", "Germany": "Alemanha", "Mexico": "México",
        "Portugal": "Portugal", "Croatia": "Croácia",
        "South Africa": "África do Sul", "France": "França",
        "Netherlands": "Países Baixos", "Uruguay": "Uruguai",
    },
    "pt-br": {
        "England": "Inglaterra", "Brazil": "Brasil", "Argentina": "Argentina",
        "Spain": "Espanha", "Germany": "Alemanha", "Mexico": "México",
        "Portugal": "Portugal", "Croatia": "Croácia",
        "South Africa": "África do Sul", "France": "França",
        "Netherlands": "Holanda", "Uruguay": "Uruguai",
    },
}


def _xlate(name: str, locale: str) -> str:
    """Translate a country name to the given locale (mock data only)."""
    if not name:
        return name
    return _COUNTRY_I18N.get(locale, {}).get(name, name)


# ── Market & label localisation ──────────────────────────────────────────────
# Markets we trade: Totals (Over/Under any line) · 1X2 · Asian Handicap.
# Explicitly excluded: BTTS, Draw No Bet — the model does not produce these.
# Each locale maps an internal canonical name → its display string.
_MARKET_DISPLAY = {
    "en":    {"totals": "Totals",  "1x2": "1X2", "ah": "Asian Handicap"},
    "es":    {"totals": "Totales", "1x2": "1X2", "ah": "Hándicap Asiático"},
    "pt-pt": {"totals": "Totais",  "1x2": "1X2", "ah": "Handicap Asiático"},
    "pt-br": {"totals": "Totais",  "1x2": "1X2", "ah": "Handicap Asiático"},
}

_LABEL_PREFIX = {
    "en":    {"Over": "Over",    "Under": "Under",    "Yes": "Yes", "No": "No",  "Draw": "Draw"},
    "es":    {"Over": "Más de",  "Under": "Menos de", "Yes": "Sí",  "No": "No",  "Draw": "Empate"},
    "pt-pt": {"Over": "Mais de", "Under": "Menos de", "Yes": "Sim", "No": "Não", "Draw": "Empate"},
    "pt-br": {"Over": "Mais de", "Under": "Menos de", "Yes": "Sim", "No": "Não", "Draw": "Empate"},
}

import re as _re_i18n
_HANDICAP_LINE_RX = _re_i18n.compile(r'^(.+?)\s+([+\-]\d+(?:\.\d+)?)$')


def _xlate_market(name: str, locale: str) -> str:
    """Translate / normalize a market display name. Folds every Over/Under line
    variant (Over/Under 2.5, O/U 3.5, Totals…) into the single 'Totais' bucket
    because the project owner trades multiple lines under the same family.
    """
    if not name:
        return name
    low = name.lower()
    table = _MARKET_DISPLAY.get(locale, _MARKET_DISPLAY["en"])
    if "over/under" in low or "over / under" in low or low.startswith("o/u") or low == "totals":
        return table["totals"]
    if "1x2" in low or low in ("home/draw/away", "match result"):
        return table["1x2"]
    if "handicap" in low or low == "ah":
        return table["ah"]
    return name  # unknown — leave as-is


def _xlate_pick_label(label: str, locale: str) -> str:
    """Translate a pick label: 'Over 2.5' → 'Mais de 2.5', 'Yes' → 'Sim',
    'Brazil -0.5' → 'Brasil -0.5', etc. Team names route through _xlate().
    """
    if not label:
        return label
    tbl = _LABEL_PREFIX.get(locale, _LABEL_PREFIX["en"])
    if label.startswith("Over "):
        return f"{tbl['Over']} {label[5:]}"
    if label.startswith("Under "):
        return f"{tbl['Under']} {label[6:]}"
    if label in tbl:
        return tbl[label]
    m = _HANDICAP_LINE_RX.match(label)
    if m:
        return f"{_xlate(m.group(1), locale)} {m.group(2)}"
    return _xlate(label, locale)


def _localize_current_payload(data: dict, locale: str) -> dict:
    """Translate market + label strings in the per-match current.json output."""
    if not data:
        return data
    picks = data.get("picks") or []
    if picks:
        data["picks"] = [{
            **p,
            "market": _xlate_market(p.get("market", ""), locale),
            "label":  _xlate_pick_label(p.get("label", ""), locale),
        } for p in picks]
    return data


def _localize_performance_payload(data: dict, locale: str) -> dict:
    """Translate market + label strings in the performance dashboard output."""
    if not data:
        return data
    bm = data.get("by_market") or []
    if bm:
        # After translation multiple internal markets can collapse onto the
        # same display name ("Over/Under 2.5" + "Over/Under 3.5" → "Totais").
        # Merge them so the dashboard shows one row per family.
        merged: dict = {}
        for m in bm:
            disp = _xlate_market(m.get("market", ""), locale)
            slot = merged.setdefault(disp, {"market": disp, "picks": 0, "pnl": 0.0})
            slot["picks"] += int(m.get("picks", 0) or 0)
            slot["pnl"]   += float(m.get("pnl", 0) or 0)
        data["by_market"] = sorted(
            ({"market": v["market"], "picks": v["picks"], "pnl": round(v["pnl"], 0)}
             for v in merged.values()),
            key=lambda x: -x["pnl"])
    tg = data.get("top_greens") or []
    if tg:
        data["top_greens"] = [{
            **g,
            "market": _xlate_market(g.get("market", ""), locale),
            "label":  _xlate_pick_label(g.get("label", ""), locale),
        } for g in tg]
    return data


def _wc2026_mock_payload(state: str, locale: str = "en") -> dict:
    """Return a synthetic, realistic-looking payload for one of the 5 states.

    Used by inbet devs and us for visual QA / screenshots before the WC begins.
    Numbers and minutes are hardcoded. Team/country names are translated to
    the requested locale so screenshots look native. Touches no DB.
    """
    xl = lambda n: _xlate(n, locale)
    now_ts = int(time.time())
    base = {
        "lang":              locale,
        "wc_emblem":         WC2026_EMBLEM_URL,
        "powered_by":        _t(locale, "powered_by"),
        "now_ts":            now_ts,
        "match":             None,
        "picks":             [],
        "match_pnl":         None,
        "next_match":        None,
        "countdown_to_next_kickoff_s": None,
        "model_preview_text": None,
        "next_poll_after_ms": 30_000,
        "_mock":             True,
    }

    if state == "live":
        base.update({
            "state": "live",
            "match": {
                "id": 99000001,
                "home": xl("England"), "away": xl("Brazil"),
                "home_goals": 1, "away_goals": 1, "minute": 67,
                "country": "QAT (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "is_finished": False,
            },
            "picks": [
                {"market": "Totals",         "label": "Over 2.5",                  "odds": 1.85, "edge": 8.2, "minute": 34, "result": None},
                {"market": "1X2",            "label": xl("Brazil"),                "odds": 2.40, "edge": 5.1, "minute": 52, "result": None},
                {"market": "Asian Handicap", "label": f"{xl('Brazil')} -0.5",      "odds": 2.05, "edge": 4.3, "minute": 11, "result": None},
            ],
            # Cumulative xG over match minutes — realistic-looking curve for an
            # entertaining open game where both attacks created chances.
            "xg_timeline": [
                {"minute":  0, "home": 0.00, "away": 0.00},
                {"minute":  6, "home": 0.05, "away": 0.10},
                {"minute": 12, "home": 0.18, "away": 0.14},
                {"minute": 18, "home": 0.32, "away": 0.20},
                {"minute": 24, "home": 0.48, "away": 0.32},
                {"minute": 30, "home": 0.65, "away": 0.42},
                {"minute": 35, "home": 0.85, "away": 0.55},
                {"minute": 41, "home": 1.02, "away": 0.68},
                {"minute": 46, "home": 1.10, "away": 0.78},
                {"minute": 52, "home": 1.22, "away": 0.95},
                {"minute": 58, "home": 1.38, "away": 1.12},
                {"minute": 63, "home": 1.52, "away": 1.28},
                {"minute": 67, "home": 1.62, "away": 1.38},
            ],
            "countdown_to_next_kickoff_s": 4 * 3600,
            "next_poll_after_ms": 30_000,
        })

    elif state in ("results_win", "results_profitable"):
        base.update({
            "state": "results_profitable",
            "match": {
                "id": 99000002,
                "home": xl("Argentina"), "away": xl("Spain"),
                "home_goals": 2, "away_goals": 1, "minute": None,
                "country": "USA (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "is_finished": True,
            },
            "picks": [
                {"market": "1X2",            "label": xl("Argentina"),              "odds": 2.10, "edge": 6.4, "minute": 18, "result": "won"},
                {"market": "Totals",         "label": "Over 2.5",                   "odds": 1.95, "edge": 5.2, "minute": 41, "result": "won"},
                {"market": "Asian Handicap", "label": f"{xl('Argentina')} -0.5",    "odds": 1.85, "edge": 3.8, "minute": 12, "result": "won"},
            ],
            "match_pnl": 187,
            "next_match": {
                "id": 99000099, "home": xl("France"), "away": xl("Netherlands"),
                "country": "USA (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "kickoff_ts": now_ts + 3 * 3600 + 14 * 60,
            },
            "countdown_to_next_kickoff_s": 3 * 3600 + 14 * 60,
            "next_poll_after_ms": 60_000,
        })

    elif state in ("results_loss", "results_losing"):
        base.update({
            "state": "results_losing",
            "match": {
                "id": 99000003,
                "home": xl("Germany"), "away": xl("Mexico"),
                "home_goals": 0, "away_goals": 2, "minute": None,
                "country": "CAN (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "is_finished": True,
            },
            "picks": [
                {"market": "1X2",     "label": xl("Germany"),  "odds": 1.80, "edge": 5.0, "minute": 22, "result": "lost"},
                {"market": "Totals",  "label": "Over 2.5",     "odds": 1.95, "edge": 4.1, "minute": 38, "result": "lost"},
            ],
            "match_pnl": -200,
            "next_match": {
                "id": 99000099, "home": xl("Portugal"), "away": xl("Uruguay"),
                "country": "MEX (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "kickoff_ts": now_ts + 2 * 3600 + 45 * 60,
            },
            "countdown_to_next_kickoff_s": 2 * 3600 + 45 * 60,
            "next_poll_after_ms": 60_000,
        })

    elif state == "preview":
        base.update({
            "state": "preview",
            "match": None,
            "next_match": {
                "id": 99000004, "home": xl("Portugal"), "away": xl("Croatia"),
                "country": "USA (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "kickoff_ts": now_ts + 2 * 3600 + 14 * 60,
            },
            "countdown_to_next_kickoff_s": 2 * 3600 + 14 * 60,
            "model_preview_text": _t(locale, "model_lean_over").replace("{xg}", "1.6"),
            "next_poll_after_ms": 60_000,
        })

    elif state == "off_day":
        # Three upcoming WC fixtures spread across the next ~2 days, in
        # chronological order. The first one is also surfaced as next_match
        # for backwards-compat with the existing countdown logic.
        upcoming = [
            {
                "id": 99000005, "home": xl("South Africa"), "away": xl("Mexico"),
                "country": "MEX (Mock)",
                "tournament": "FIFA World Cup 2026 — Opening Match",
                "kickoff_ts": now_ts + 2 * 86400 + 4 * 3600,
            },
            {
                "id": 99000006, "home": xl("France"), "away": xl("Croatia"),
                "country": "USA (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "kickoff_ts": now_ts + 2 * 86400 + 7 * 3600,
            },
            {
                "id": 99000007, "home": xl("Brazil"), "away": xl("Spain"),
                "country": "CAN (Mock)",
                "tournament": "FIFA World Cup 2026 — Group Stage",
                "kickoff_ts": now_ts + 2 * 86400 + 10 * 3600,
            },
        ]
        base.update({
            "state": "off_day",
            "next_match": upcoming[0],
            "upcoming_matches": upcoming,
            "countdown_to_next_kickoff_s": upcoming[0]["kickoff_ts"] - now_ts,
            "next_poll_after_ms": 300_000,
        })

    else:
        base.update({"state": "off_day", "next_poll_after_ms": 60_000})

    return base


def _wc2026_current_state_for_match(match_id: int, locale: str = "en") -> dict:
    """Build a 'live' state payload for any monitored match (non-WC OK).

    Powers the match-detail iframe — same fixture on every poll, three
    states served from one URL: PREVIEW (countdown) before kickoff, LIVE
    (xG chart + picks) during play, RESULTS indefinitely after the final
    whistle. Never rotates to a different match.

    Lookup order:
      1) _live_state  → game is being scraped right now (LIVE or just-FT)
      2) games table  → game finished and has dropped from live state
      3) _upcoming_cache or games table → kick-off still in the future
      4) fall through with an empty payload (rare)
    """
    now_ts = int(time.time())

    # ── 1. Live state (currently scraped) ─────────────────────────────────
    entry = None
    try:
        with _state_lock:
            entry = _live_state.get(int(match_id))
            if entry:
                entry = dict(entry)  # shallow copy for safety
    except Exception as e:
        log.warning(f"_wc2026_current_state_for_match({match_id}) live state lookup failed: {e}")

    if entry:
        m = entry.get("match") or {}
        is_finished = bool(m.get("isFinished"))
        state = "results_profitable" if is_finished else "live"
        match_payload = {
            "id":          m.get("id"),
            "home":        m.get("homeTeam"),
            "away":        m.get("awayTeam"),
            "home_goals":  m.get("homeGoals", 0) or 0,
            "away_goals":  m.get("awayGoals", 0) or 0,
            "minute":      m.get("minute"),
            "country":     m.get("country", ""),
            "tournament":  m.get("tournament", ""),
            "is_finished": is_finished,
        }
        live_picks = entry.get("livePicks") or entry.get("tips") or []
        picks_payload = [
            {
                "market":  p.get("market", ""),
                "label":   p.get("label", ""),
                "odds":    p.get("odds") or p.get("odd_entry") or 0,
                "edge":    p.get("edge")  or p.get("edge_entry") or 0,
                "minute":  p.get("minute_entry") if p.get("minute_entry") is not None else p.get("minute"),
                "result":  p.get("result"),
            }
            for p in live_picks
        ]
        xg_timeline = [] if is_finished else _build_xg_timeline_from_entry(entry)
        return {
            "state":                       state,
            "lang":                        locale,
            "match":                       match_payload,
            "picks":                       picks_payload,
            "match_pnl":                   None,
            "next_match":                  None,
            "countdown_to_next_kickoff_s": None,
            "model_preview_text":          None,
            "wc_emblem":                   WC2026_EMBLEM_URL,
            "powered_by":                  _t(locale, "powered_by"),
            "next_poll_after_ms":          30_000 if not is_finished else 60_000,
            "now_ts":                      now_ts,
            "xg_timeline":                 xg_timeline,
            "_override_match_id":          int(match_id),
        }

    # ── 2 & 3. DB lookup — finished archived game OR scheduled-but-not-live-yet
    g_row = None
    try:
        with _db() as conn:
            g_row = conn.execute(
                "SELECT id, home_team, away_team, home_goals, away_goals, "
                "       start_ts, tournament, country, is_finished, archived_at "
                "FROM games WHERE id = ?",
                (int(match_id),)
            ).fetchone()
    except Exception as e:
        log.warning(f"_wc2026_current_state_for_match({match_id}) DB lookup failed: {e}")

    # ── 2.5. Not in DB? Try _upcoming_cache — scheduled fixture that the
    # background scraper has already discovered but hasn't yet promoted to
    # the games table (which only happens once it starts being tracked).
    if not g_row:
        try:
            for date_key, cached in _upcoming_cache.items():
                for m_up in (cached or {}).get("matches", []):
                    if int(m_up.get("id") or 0) == int(match_id):
                        g_row = {
                            "id":           m_up.get("id"),
                            "home_team":    m_up.get("homeTeam"),
                            "away_team":    m_up.get("awayTeam"),
                            "home_goals":   0,
                            "away_goals":   0,
                            "start_ts":     m_up.get("startTimestamp"),
                            "tournament":   m_up.get("tournament", ""),
                            "country":      m_up.get("country", ""),
                            "is_finished":  0,
                            "archived_at":  None,
                        }
                        break
                if g_row:
                    break
        except Exception as e:
            log.warning(f"_wc2026_current_state_for_match({match_id}) upcoming cache lookup failed: {e}")

    if g_row:
        g = dict(g_row)
        if g.get("is_finished"):
            # PERSISTED RESULTS — show indefinitely on the match-detail page.
            tips_rows = []
            try:
                with _db() as conn:
                    tips_rows = conn.execute(
                        "SELECT market, label, odd_entry, edge_entry, minute_entry, "
                        "       result, wall_ts FROM tips "
                        "WHERE match_id = ? AND result IS NOT NULL "
                        "ORDER BY wall_ts ASC",
                        (int(match_id),)
                    ).fetchall()
            except Exception as e:
                log.warning(f"_wc2026_current_state_for_match({match_id}) tips fetch failed: {e}")
            picks_payload = [
                {
                    "market":  r["market"],
                    "label":   r["label"],
                    "odds":    r["odd_entry"] or 0,
                    "edge":    r["edge_entry"] or 0,
                    "minute":  r["minute_entry"],
                    "result":  r["result"],
                }
                for r in tips_rows
            ]
            STAKE = get_setting("stake_per_bet", 100.0) or 100.0
            pnl = sum(
                ((r["odd_entry"] or 0) - 1) * STAKE if (r["result"] or "").lower() in ("green","win") else
                (-STAKE if (r["result"] or "").lower() in ("red","loss") else 0)
                for r in tips_rows
            )
            return {
                "state":                       "results_profitable" if pnl >= 0 else "results_losing",
                "lang":                        locale,
                "match": {
                    "id":          g["id"],
                    "home":        g["home_team"],
                    "away":        g["away_team"],
                    "home_goals":  g["home_goals"] or 0,
                    "away_goals":  g["away_goals"] or 0,
                    "minute":      None,
                    "country":     g.get("country", ""),
                    "tournament":  g.get("tournament", ""),
                    "is_finished": True,
                },
                "picks":                       picks_payload,
                "match_pnl":                   round(pnl, 0),
                "next_match":                  None,
                "countdown_to_next_kickoff_s": None,
                "model_preview_text":          None,
                "wc_emblem":                   WC2026_EMBLEM_URL,
                "powered_by":                  _t(locale, "powered_by"),
                # Finished + on a detail page → barely needs polling.
                "next_poll_after_ms":          600_000,
                "now_ts":                      now_ts,
                "_override_match_id":          int(match_id),
            }
        else:
            # NOT finished, NOT in live state yet → pre-match PREVIEW with
            # countdown. The game row already has the kickoff time.
            kickoff_ts = int(g.get("start_ts") or 0)
            countdown_s = max(0, kickoff_ts - now_ts) if kickoff_ts else None
            return {
                "state":                       "preview",
                "lang":                        locale,
                "match":                       None,
                "picks":                       [],
                "match_pnl":                   None,
                "next_match": {
                    "id":          g["id"],
                    "home":        g["home_team"],
                    "away":        g["away_team"],
                    "country":     g.get("country", ""),
                    "tournament":  g.get("tournament", ""),
                    "kickoff_ts":  kickoff_ts,
                },
                "countdown_to_next_kickoff_s": countdown_s,
                "model_preview_text":          None,
                "wc_emblem":                   WC2026_EMBLEM_URL,
                "powered_by":                  _t(locale, "powered_by"),
                # Tight polling near kickoff so we transition to LIVE quickly.
                "next_poll_after_ms":          30_000 if (countdown_s or 0) < 1800 else 120_000,
                "now_ts":                      now_ts,
                "_override_match_id":          int(match_id),
            }

    # ── 4. Truly unknown — return a friendly empty preview ────────────────
    return {
        "state":               "preview",
        "lang":                locale,
        "match":               None,
        "picks":               [],
        "match_pnl":           None,
        "next_match":          None,
        "countdown_to_next_kickoff_s": None,
        "model_preview_text":  f"Match id={match_id} not found",
        "wc_emblem":           WC2026_EMBLEM_URL,
        "powered_by":          _t(locale, "powered_by"),
        "next_poll_after_ms":  60_000,
        "now_ts":              now_ts,
        "_override_match_id":  int(match_id),
    }


def _wc2026_mock_performance(locale: str = "en") -> dict:
    """Synthetic dashboard payload — fixed numbers for pre-launch QA.

    Field shapes match the real _wc2026_performance() output:
      equity_curve: [{ts, date, cum_pnl}]    (JS reads p.cum_pnl)
      top_greens:   [{match, market, label, odds, minute_entered, profit}]
      by_market:    [{market, picks, pnl}]
    No BTTS (the model doesn't trade that market).
    """
    from datetime import datetime, timezone
    now_ts = int(time.time())
    xl = lambda n: _xlate(n, locale)

    # 19-day rising equity curve in EUR (cumulative P&L)
    curve = [0, 35, 88, 64, 142, 198, 264, 312, 388, 421, 510, 612, 698, 783, 904, 1058, 1142, 1208, 1283]
    equity_pts = []
    for i, v in enumerate(curve):
        ts = now_ts - (len(curve) - 1 - i) * 86400
        equity_pts.append({
            "ts":      ts,
            "date":    datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
            "cum_pnl": v,
        })

    # Top 5 winning picks — only the 3 markets we actually trade.
    # Note we deliberately mix Over 2.5 / Over 3.5 / Under 3.5 to demonstrate
    # that we trade multiple lines (all collapse under "Totais" / "Totals").
    top_greens = [
        {"match": f"{xl('Argentina')} vs Iran",                "market": "Totals",         "label": "Over 2.5",                "odds": 2.10, "minute_entered": 31, "profit": 110},
        {"match": f"{xl('Brazil')} vs Serbia",                 "market": "1X2",            "label": xl("Brazil"),              "odds": 1.95, "minute_entered": 22, "profit":  95},
        {"match": f"{xl('Netherlands')} vs {xl('Argentina')}", "market": "Asian Handicap", "label": f"{xl('Argentina')} +0.5", "odds": 1.85, "minute_entered": 14, "profit":  85},
        {"match": f"{xl('Portugal')} vs Ghana",                "market": "Totals",         "label": "Over 3.5",                "odds": 2.30, "minute_entered": 38, "profit":  75},
        {"match": f"{xl('Spain')} vs Morocco",                 "market": "Totals",         "label": "Under 3.5",               "odds": 1.70, "minute_entered": 19, "profit":  70},
    ]

    by_market = [
        {"market": "Totals",         "picks": 25, "pnl":  762.0},
        {"market": "1X2",            "picks": 14, "pnl":  386.0},
        {"market": "Asian Handicap", "picks":  8, "pnl":  135.0},
    ]

    return {
        "settled":      47,
        "wins":         30,
        "losses":       14,
        "voids":        3,
        "winrate":      63.8,
        "pnl":          1283.0,
        "roi":          18.2,
        "avg_odds":     1.92,
        "equity_curve": equity_pts,
        "top_greens":   top_greens,
        "by_market":    by_market,
        "tournament_start": WC2026_START_TS,
        "tournament_end":   WC2026_END_TS,
        "_mock":            True,
        "_lang":            locale,
    }


_MOCK_STATE_ALIASES = {
    "live":               "live",
    "results_win":        "results_profitable",
    "results_profitable": "results_profitable",
    "results_loss":       "results_losing",
    "results_losing":     "results_losing",
    "preview":            "preview",
    "off_day":            "off_day",
    "offday":             "off_day",
}


# In-memory cache for the full WC2026 fixture list. Sofascore's
# tournament endpoint paginates 30 events per request — we'd otherwise
# fan out 3-4 HTTP calls per partner hit. With this cache, partners can
# poll as often as they like and Sofascore sees ~4-8 calls/day from us.
#
# TTL = 6h. Tight enough that knockout bracket updates (e.g. group
# stage results filling in the W99/L101 placeholders with real team
# names) become visible to partners within a few hours of happening.
# Loose enough that we're not hammering Sofascore.
#
# Partners that need a fresh pull NOW (e.g. immediately after a key
# group match settles) can hit the endpoint with `?refresh=1` to
# bypass the cache for that single request.
_WC2026_FIXTURES_CACHE = {"data": None, "ts": 0.0}
_WC2026_FIXTURES_TTL   = 6 * 3600   # 6h


def _fetch_wc2026_all_fixtures(force: bool = False) -> list:
    """Hit Sofascore's tournament endpoint for FIFA World Cup 2026
    (unique-tournament=16, season=58210) and paginate through `next/N`
    and `last/N` pages until empty. Returns the raw list of events
    (Sofascore-shaped dicts). Cached 6h in memory. `force=True` skips
    the cache and re-fetches immediately."""
    now = time.time()
    if (not force
            and _WC2026_FIXTURES_CACHE["data"] is not None
            and (now - _WC2026_FIXTURES_CACHE["ts"]) < _WC2026_FIXTURES_TTL):
        return _WC2026_FIXTURES_CACHE["data"]

    all_events: list = []
    # `last` pages = matches already played (becomes relevant during tournament).
    # `next` pages = upcoming fixtures (relevant pre-tournament).
    for direction in ("next", "last"):
        for page in range(10):   # safety: max 300 events per direction
            try:
                data = _get(f"{SOFASCORE_API}/unique-tournament/16/season/58210/events/{direction}/{page}")
            except Exception as e:
                log.warning(f"_fetch_wc2026_all_fixtures {direction}/{page}: {e}")
                break
            evs = (data or {}).get("events") or []
            if not evs:
                break
            all_events.extend(evs)
            if len(evs) < 30:
                break   # last page, no need to keep paginating

    # Dedup by event ID (in case last + next overlap on edge matches)
    seen = set()
    deduped: list = []
    for e in all_events:
        eid = e.get("id")
        if eid and eid not in seen:
            seen.add(eid)
            deduped.append(e)

    _WC2026_FIXTURES_CACHE["data"] = deduped
    _WC2026_FIXTURES_CACHE["ts"]   = now
    log.info(f"_fetch_wc2026_all_fixtures: cached {len(deduped)} WC events")
    return deduped


def _shape_wc_fixture(e: dict, now_ts: int) -> dict:
    """Convert a Sofascore-shaped event to the partner-friendly dict
    returned by /api/wc2026/fixtures. Same shape regardless of source
    (Sofascore-API direct vs local DB) so partners see one schema."""
    from datetime import datetime, timezone
    import re as _re_grp
    ts = e.get("startTimestamp") or 0
    iso = (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
           if ts else None)
    # Tournament name from Sofascore arrives as "FIFA World Cup, Group A",
    # "FIFA World Cup, Round of 32", etc. Parse stage + group from it.
    tourn = (e.get("tournament") or {}).get("name") or ""
    tourn_l = tourn.lower()
    stage = "Group Stage"
    group = None
    if   "round of 32" in tourn_l: stage = "Round of 32"
    elif "round of 16" in tourn_l: stage = "Round of 16"
    elif "quarter"     in tourn_l: stage = "Quarterfinals"
    elif "semi"        in tourn_l: stage = "Semifinals"
    elif "3rd" in tourn_l or "third" in tourn_l: stage = "3rd Place"
    elif "final"       in tourn_l: stage = "Final"
    elif "group"       in tourn_l:
        m = _re_grp.search(r"group\s+([a-l])", tourn_l)
        if m:
            group = m.group(1).upper()
    status_type = (e.get("status") or {}).get("type", "")
    if   status_type == "finished":   status = "finished"
    elif status_type == "inprogress": status = "live"
    else:                             status = "scheduled"
    return {
        "event_id":   str(e.get("id")),
        "home_team":  (e.get("homeTeam") or {}).get("name"),
        "away_team":  (e.get("awayTeam") or {}).get("name"),
        "kickoff_ts": ts or None,
        "kickoff_iso": iso,
        "stage":      stage,
        "group":      group,
        "home_goals": (e.get("homeScore") or {}).get("current", 0),
        "away_goals": (e.get("awayScore") or {}).get("current", 0),
        "status":     status,
    }


@app.route("/api/fixtures/today")
def r_fixtures_today():
    """Test endpoint for integration partners: returns every monitored
    fixture happening today (or whichever date is passed via ?date=YYYY-MM-DD).
    Same shape as /api/wc2026/fixtures so partners can wire one mapping
    function for both. Use this before the World Cup starts to verify
    your Sofascore event_id → internal-match mapping works correctly.

    Pulls from the in-memory _upcoming_cache that the bg loop refreshes
    every cycle. CORS open, no auth.
    """
    from datetime import datetime, timezone
    requested_date = (flask_request.args.get("date") or "").strip()
    if not requested_date:
        requested_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    cached = _upcoming_cache.get(requested_date)
    raw = cached["matches"] if cached else []
    out = []
    now_ts = int(time.time())
    for m in raw:
        ts = m.get("startTimestamp") or 0
        iso = (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
               if ts else None)
        # Status — m may not have a 'status' shape since it's from the
        # upcoming cache; infer from timestamp + isFinished if present.
        if m.get("isFinished"):
            status = "finished"
        elif m.get("isLive") or (ts and 0 < (now_ts - ts) < 3 * 3600):
            status = "live"
        else:
            status = "scheduled"
        out.append({
            "event_id":   str(m.get("id")),
            "home_team":  m.get("homeTeam"),
            "away_team":  m.get("awayTeam"),
            "kickoff_ts": ts or None,
            "kickoff_iso": iso,
            "tournament": m.get("tournament"),
            "country":    m.get("country"),
            "home_goals": m.get("homeGoals", 0),
            "away_goals": m.get("awayGoals", 0),
            "status":     status,
        })
    return jsonify({
        "date":     requested_date,
        "count":    len(out),
        "fixtures": out,
    }), 200, {
        "Cache-Control":              "public, max-age=60, s-maxage=60",
        "Access-Control-Allow-Origin": "*",
    }


@app.route("/api/wc2026/fixtures")
def r_wc2026_fixtures():
    """Public fixture list for the entire FIFA World Cup 2026 tournament.

    Designed for integration partners (inbet etc.) who run their own
    match registry but need to know our Sofascore `event_id` to embed
    the Match Detail widget (`?match_id=<event_id>`). Partners hit this
    endpoint once (daily refresh is plenty — fixtures rarely change) and
    build a local mapping table keyed by (home_team, away_team,
    kickoff_date) → event_id.

    Returns a JSON object with `fixtures` array. Each fixture has:

      event_id      — Sofascore numeric ID (string).  PASS THIS to the iframe.
      home_team     — canonical English name from Sofascore.
      away_team     — canonical English name from Sofascore.
      kickoff_ts    — UTC epoch seconds (int).
      kickoff_iso   — ISO-8601 UTC string ("2026-06-11T17:00:00Z").
      stage         — "Group Stage" / "Round of 32" / "Round of 16" /
                       "Quarterfinals" / "Semifinals" / "Final" / "3rd Place".
      group         — Group letter ("A".."L") for group stage, null otherwise.
      home_goals    — int, 0 until match starts. Final score once finished.
      away_goals    — int, idem.
      status        — "scheduled" | "live" | "finished".

    Mapping example (partner pseudocode):

        partner_match = {"home": "Brazil", "away": "Argentina",
                         "kickoff": "2026-06-25"}
        fixtures = GET https://embed.webpronos.com/api/wc2026/fixtures
        match = next(
            f for f in fixtures["fixtures"]
            if normalise(f["home_team"]) == normalise(partner_match["home"])
            and normalise(f["away_team"]) == normalise(partner_match["away"])
            and f["kickoff_iso"][:10] == partner_match["kickoff"]
        )
        iframe_src = f"https://embed.webpronos.com/widget/wc2026/current?match_id={match['event_id']}&lang=pt-pt"

    Cache: 1h browser, 5min CDN — fixtures are stable, refresh cadence
    is fine for daily partner sync jobs.
    """
    out = []
    now_ts = int(time.time())
    force_refresh = (flask_request.args.get("refresh") or "").strip() in ("1", "true", "yes")
    try:
        # Primary source: Sofascore tournament endpoint (returns all 64+
        # fixtures published, including those >3 days away that our
        # normal scraper doesn't fetch). Cached 6h in memory; `?refresh=1`
        # bypasses for a fresh pull (knockout bracket changes etc.).
        events = _fetch_wc2026_all_fixtures(force=force_refresh)
        for e in events:
            out.append(_shape_wc_fixture(e, now_ts))
        out.sort(key=lambda x: x.get("kickoff_ts") or 0)
    except Exception as e:
        log.exception(f"r_wc2026_fixtures: {e}")
        return jsonify({"error": "internal", "detail": str(e)[:200]}), 500

    return jsonify({
        "tournament": "FIFA World Cup 2026",
        "window":     {"start": "2026-06-11", "end": "2026-07-19"},
        "count":      len(out),
        "fixtures":   out,
    }), 200, {
        "Cache-Control":              "public, max-age=300, s-maxage=300",
        "Access-Control-Allow-Origin": "*",
    }


@app.route("/api/wc2026/current.json")
def r_wc2026_current_json():
    """JSON the per-match widget polls every 30 s (live) / 60-300 s otherwise.

    Query params:
        lang      — en | es | pt-pt | pt-br
        mock      — force one of the 5 states with synthetic data (test only)
        match_id  — override WC filter, use any monitored live match (test only)
        demo      — full state machine without WC filter; picks the first
                    monitored live match (any tournament), shows results when
                    it ends, then transitions to the next monitored fixture.
                    Used by the staging widget URL for end-to-end dev testing.
    """
    locale = _widget_locale(flask_request.args.get("lang"))
    mock     = (flask_request.args.get("mock") or "").strip().lower()
    match_id = (flask_request.args.get("match_id") or "").strip()
    demo     = (flask_request.args.get("demo") or "").strip().lower() in ("1", "true", "yes", "on")

    if mock:
        canonical = _MOCK_STATE_ALIASES.get(mock, "off_day")
        data = _wc2026_mock_payload(canonical, locale)
    elif match_id.isdigit():
        data = _wc2026_current_state_for_match(int(match_id), locale)
    else:
        data = _wc2026_current_state(locale, demo=demo)

    # Final localisation pass — translates market names and pick labels.
    # Mock data ships canonical English names; real data comes straight from
    # Sofascore. Both pass through the same translator for consistency.
    data = _localize_current_payload(data, locale)

    return jsonify(data), 200, {
        "Cache-Control": "no-store" if (mock or match_id or demo) else "public, max-age=15",
        "Access-Control-Allow-Origin": "*",
    }


# ─── Per-match widget HTML ──────────────────────────────────────────────────
# Self-contained HTML page. Inlined CSS + JS. Polls /api/wc2026/current.json
# and renders one of 5 states (live / results_profitable / results_losing /
# preview / off_day). Designed to be embedded as an <iframe> on inbet.io.
_WC_WIDGET_MATCH_HTML = """<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="robots" content="noindex,nofollow">
<title>InBetIO Live xG Model — World Cup 2026</title>
<style>
  /* Default dark palette — matches the app.inbet.io members area chrome so
     the iframe blends seamlessly. Deep midnight navy bg, slightly lighter
     card, warm orange primary accent, royal-blue secondary accent. */
  :root{{
    --bg:#0c1126; --card:#181f38; --border:#252d4a; --text:#ffffff;
    --meta:#8b95a9; --accent:{accent}; --accent-2:#2667ff;
    --green:#22c55e; --red:#ef4444; --amber:#fbbf24; --cyan:#3b82f6;
  }}
  body[data-theme="light"]{{
    --bg:#f8fafc; --card:#ffffff; --border:#e5e7eb; --text:#0f172a;
    --meta:#64748b;
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",sans-serif;line-height:1.5;padding:0}}
  .header{{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid var(--border)}}
  .header .left{{display:flex;align-items:center;gap:10px}}
  .header img.emblem{{width:36px;height:auto;display:block}}
  .header .brand{{font-size:.8rem;color:var(--meta);font-weight:600;letter-spacing:.04em;text-transform:uppercase}}
  .pill{{display:inline-block;padding:3px 10px;border-radius:999px;font-size:.72rem;font-weight:700;letter-spacing:.05em}}
  .pill-live{{background:rgba(239,68,68,.15);color:#ef4444}}
  .pill-live::before{{content:"●";margin-right:.35rem;animation:pulse 1.4s infinite}}
  .pill-sched{{background:rgba(34,211,238,.12);color:var(--cyan)}}
  .pill-ft{{background:rgba(156,163,175,.15);color:var(--meta)}}
  @keyframes pulse{{0%{{opacity:1}}50%{{opacity:.35}}100%{{opacity:1}}}}
  .container{{padding:16px}}
  .matchcard{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px 16px;margin-bottom:14px}}
  .matchrow{{display:flex;align-items:center;justify-content:space-between;gap:12px}}
  /* Stacked team card: flag on top, name below — used in renderMatchCard,
     renderPreview and renderResults. */
  .team{{display:flex;flex-direction:column;align-items:center;gap:8px;flex:1;min-width:0}}
  .team.away{{text-align:center}}
  .team-name{{font-weight:700;font-size:1rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%;text-align:center}}
  .flag-img{{display:block;height:56px;width:auto;border-radius:4px;box-shadow:0 2px 8px rgba(0,0,0,.25);object-fit:cover}}
  /* Smaller flag for inline next-match countdown rows where stacking would
     be visually overkill (single-line "Next up: PT vs HR" copy). */
  .flag-inline{{height:18px;width:auto;border-radius:2px;vertical-align:middle;margin:0 4px;box-shadow:0 1px 3px rgba(0,0,0,.2)}}
  .flag{{font-size:1.4rem;line-height:1}}
  .flag-fallback{{font-size:1.4rem}}
  /* "Get Pick Alerts" CTA — InBet orange, present in every state above the
     Powered-by footer. Pill-shaped, prominent. */
  .alerts-cta-wrap{{display:flex;justify-content:center;margin:20px 0 12px}}
  .alerts-cta{{
    display:inline-flex;align-items:center;gap:8px;
    background:var(--accent);color:#1a1a2e;
    font-weight:800;font-size:.95rem;letter-spacing:.02em;
    padding:11px 22px;border-radius:999px;text-decoration:none;
    box-shadow:0 4px 12px rgba(255,138,30,.30);
    transition:transform .12s ease, box-shadow .12s ease, filter .12s ease;
  }}
  .alerts-cta:hover{{transform:translateY(-1px);filter:brightness(1.05);
    box-shadow:0 6px 16px rgba(255,138,30,.40)}}
  .alerts-cta:active{{transform:translateY(0)}}
  .alerts-cta .tg{{font-size:1.1rem;line-height:1}}
  /* Upcoming-matches list (off_day state) — symmetric layout matching the
     InBet members-area look: team name + logo left, time stacked centred,
     logo + team name right. */
  .upcoming-list{{display:flex;flex-direction:column;gap:10px;margin-top:6px}}
  .upcoming-row{{
    display:grid;
    grid-template-columns:1fr auto 1fr;
    align-items:center;
    padding:14px 16px;
    background:var(--card);border:1px solid var(--border);
    border-radius:12px;gap:14px;
  }}
  .upcoming-side{{display:flex;align-items:center;gap:12px;min-width:0}}
  .upcoming-side.home{{justify-content:flex-end}}
  .upcoming-side.away{{justify-content:flex-start}}
  .upcoming-team{{font-weight:700;font-size:1rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .upcoming-mid{{display:flex;flex-direction:column;align-items:center;gap:1px;padding:0 10px;line-height:1.15}}
  .upcoming-mid .day{{font-size:.68rem;color:var(--meta);text-transform:uppercase;letter-spacing:.06em}}
  .upcoming-mid .time{{font-weight:800;font-size:1.15rem;color:var(--accent)}}
  .upcoming-mid .vs{{font-size:.7rem;color:var(--meta);margin-top:2px;text-transform:uppercase;letter-spacing:.08em}}
  /* Override the global .flag-img 56px height — upcoming list uses smaller flags */
  .upcoming-row .flag-img{{height:32px}}
  /* Mobile / narrow iframes: shrink everything proportionally */
  @media (max-width: 520px){{
    .upcoming-row{{padding:10px 10px;gap:8px;grid-template-columns:1fr auto 1fr}}
    .upcoming-side{{gap:7px}}
    .upcoming-team{{font-size:.85rem}}
    .upcoming-mid{{padding:0 4px}}
    .upcoming-mid .time{{font-size:.95rem}}
    .upcoming-mid .day,.upcoming-mid .vs{{font-size:.62rem}}
    .upcoming-row .flag-img{{height:22px}}
  }}
  .score{{font-size:1.8rem;font-weight:800;letter-spacing:.05em;padding:0 14px;color:var(--text);min-width:90px;text-align:center}}
  .meta-line{{font-size:.78rem;color:var(--meta);text-align:center;margin-top:10px}}
  .section-title{{font-size:.72rem;color:var(--meta);font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin:18px 0 8px}}
  .xg-chart{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 12px 8px;margin-bottom:14px}}
  .xg-chart svg{{display:block;width:100%;height:auto}}
  .xg-chart .legend{{display:flex;gap:14px;justify-content:flex-end;font-size:.72rem;color:var(--meta);margin-top:6px;flex-wrap:wrap}}
  .xg-chart .legend .dot{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px;vertical-align:middle}}
  .xg-chart .legend .dot-home{{background:var(--green)}}
  .xg-chart .legend .dot-away{{background:var(--red)}}
  .xg-chart .legend .totals{{color:var(--text);font-weight:700}}
  .pick-row{{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:var(--card);border:1px solid var(--border);border-radius:10px;margin-bottom:6px;font-size:.9rem;gap:10px}}
  .pick-left{{display:flex;align-items:center;gap:10px;min-width:0;flex:1}}
  .pick-minute{{font-size:.72rem;color:var(--meta);min-width:34px;text-align:center;background:rgba(255,255,255,.04);border-radius:6px;padding:3px 6px}}
  body[data-theme="light"] .pick-minute{{background:rgba(0,0,0,.04)}}
  .pick-market{{font-size:.7rem;color:var(--meta);text-transform:uppercase;letter-spacing:.04em}}
  .pick-label{{font-weight:600}}
  .pick-right{{display:flex;align-items:center;gap:10px;white-space:nowrap}}
  .pick-odds{{font-weight:700;color:var(--text)}}
  .pick-edge{{color:var(--green);font-size:.78rem;font-weight:600}}
  .badge{{padding:3px 9px;border-radius:6px;font-size:.7rem;font-weight:700;letter-spacing:.04em}}
  .badge-won{{background:rgba(16,185,129,.18);color:var(--green)}}
  .badge-lost{{background:rgba(239,68,68,.18);color:var(--red)}}
  .badge-push{{background:rgba(156,163,175,.18);color:var(--meta)}}
  .badge-pending{{background:rgba(251,191,36,.18);color:var(--amber)}}
  .countdown{{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px 16px;background:var(--card);border:1px solid var(--border);border-radius:12px;margin-bottom:14px}}
  .countdown .label{{font-size:.72rem;color:var(--meta);text-transform:uppercase;letter-spacing:.08em}}
  .countdown .value{{font-size:1.8rem;font-weight:800;margin-top:6px}}
  .pnl-strip{{display:flex;align-items:center;justify-content:center;padding:10px;border-radius:10px;font-weight:700;font-size:1rem;margin-bottom:14px}}
  .pnl-strip.profit{{background:rgba(16,185,129,.12);color:var(--green);border:1px solid rgba(16,185,129,.3)}}
  .pnl-strip.loss{{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.3)}}
  .empty{{text-align:center;color:var(--meta);padding:20px;font-size:.85rem}}
  .footer{{text-align:center;padding:12px 16px;border-top:1px solid var(--border);font-size:.7rem;color:var(--meta);letter-spacing:.04em}}
  .preview-card{{text-align:center;padding:18px 16px;background:var(--card);border:1px solid var(--border);border-radius:12px}}
  .preview-card .label{{font-size:.72rem;color:var(--meta);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}}
  .preview-card .vs{{color:var(--meta);font-weight:400;margin:0 8px}}
  .preview-card .model-preview{{margin-top:14px;padding:10px 12px;background:rgba(34,211,238,.06);border:1px solid rgba(34,211,238,.18);border-radius:8px;font-size:.85rem;color:var(--cyan)}}
</style>
</head>
<body data-theme="{theme}">
<div class="header">
  <div class="left">
    <img class="emblem" src="{emblem}" alt="{emblem_alt}">
    <span class="brand">InBetIO Live xG Model</span>
  </div>
  <div id="status-pill"></div>
</div>
<div id="app" class="container">
  <div class="empty">Loading…</div>
</div>
<div class="footer">{powered_by}</div>
<script>
(function(){{
  const params  = new URLSearchParams(location.search);
  const lang    = params.get('lang')  || 'en';
  // Test affordances forwarded to the JSON endpoint:
  //   ?mock=<state>   — hardcoded payload for one of the 5 states
  //   ?match_id=<id>  — track ONE specific live match by id
  //   ?demo=1         — full state machine using ANY monitored fixture (no
  //                     WC tournament filter); ideal for dev/staging tests
  const mock     = params.get('mock');
  const matchId  = params.get('match_id');
  const demo     = params.get('demo');
  let apiUrl  = '/api/wc2026/current.json?lang=' + encodeURIComponent(lang);
  if (mock)    apiUrl += '&mock='     + encodeURIComponent(mock);
  if (matchId) apiUrl += '&match_id=' + encodeURIComponent(matchId);
  if (demo)    apiUrl += '&demo='     + encodeURIComponent(demo);
  const root    = document.getElementById('app');
  const pillBox = document.getElementById('status-pill');
  let timer;

  // --- i18n (injected by server below) ---
  const COPY = {copy_json};

  function t(key){{ return (COPY && COPY[key]) || key; }}

  // For a national-team competition the team name IS the country. Map every
  // English name AND its localized variants to an ISO 3166-1 alpha-2 code
  // (lowercase). UK home nations use the `gb-eng`/`gb-wls`/`gb-sct` flagcdn
  // subdivision IDs. Real flag images are served from https://flagcdn.com/
  // so we drop the emoji approach entirely.
  const FLAG_CODE_MAP = {{
    // Confederations Cup / typical WC qualifiers — English
    'south africa':'za','mexico':'mx','argentina':'ar','brazil':'br',
    'france':'fr','england':'gb-eng','spain':'es','germany':'de','italy':'it',
    'portugal':'pt','netherlands':'nl','belgium':'be','usa':'us','united states':'us',
    'canada':'ca','morocco':'ma','japan':'jp','south korea':'kr','korea republic':'kr',
    'australia':'au','saudi arabia':'sa','iran':'ir','ir iran':'ir','uruguay':'uy',
    'colombia':'co','chile':'cl','poland':'pl','denmark':'dk','sweden':'se',
    'norway':'no','switzerland':'ch','croatia':'hr','serbia':'rs','ghana':'gh',
    'senegal':'sn','tunisia':'tn','cameroon':'cm','ecuador':'ec','egypt':'eg',
    'nigeria':'ng','wales':'gb-wls','qatar':'qa','peru':'pe','bolivia':'bo',
    'paraguay':'py','venezuela':'ve','panama':'pa','costa rica':'cr','jamaica':'jm',
    'honduras':'hn','el salvador':'sv','guatemala':'gt','curacao':'cw','haiti':'ht',
    'algeria':'dz','ivory coast':'ci','cote d\\'ivoire':'ci','mali':'ml','burkina faso':'bf',
    'cape verde':'cv','dr congo':'cd','gabon':'ga','zambia':'zm','kenya':'ke',
    'iraq':'iq','jordan':'jo','uae':'ae','united arab emirates':'ae','oman':'om',
    'uzbekistan':'uz','china':'cn','china pr':'cn','thailand':'th','vietnam':'vn',
    'new zealand':'nz','fiji':'fj','turkey':'tr','türkiye':'tr','austria':'at',
    'czech republic':'cz','czechia':'cz','slovakia':'sk','hungary':'hu','romania':'ro',
    'bulgaria':'bg','ukraine':'ua','russia':'ru','greece':'gr','iceland':'is',
    'ireland':'ie','scotland':'gb-sct','northern ireland':'gb-nir','finland':'fi','albania':'al',
    'bosnia':'ba','bosnia and herzegovina':'ba','north macedonia':'mk','slovenia':'si',
    'georgia':'ge','azerbaijan':'az','armenia':'am',
    // Localized variants — pt-pt / pt-br / es
    'inglaterra':'gb-eng','brasil':'br','espanha':'es','españa':'es',
    'alemanha':'de','alemania':'de','méxico':'mx','frança':'fr','francia':'fr',
    'croácia':'hr','croacia':'hr','áfrica do sul':'za','sudáfrica':'za',
    'países baixos':'nl','países bajos':'nl','holanda':'nl',
    'uruguai':'uy','turquia':'tr'
  }};

  // Legacy emoji map — kept for fallback only (used by very old code paths).
  // Prefer flagImg() everywhere.
  const FLAG_MAP = {{
    'south africa':'🇿🇦','mexico':'🇲🇽','argentina':'🇦🇷','brazil':'🇧🇷',
    'france':'🇫🇷','england':'🏴󠁧󠁢󠁥󠁮󠁧󠁿','spain':'🇪🇸','germany':'🇩🇪','italy':'🇮🇹',
    'portugal':'🇵🇹','netherlands':'🇳🇱','belgium':'🇧🇪','usa':'🇺🇸','united states':'🇺🇸',
    'canada':'🇨🇦','morocco':'🇲🇦','japan':'🇯🇵','south korea':'🇰🇷','korea republic':'🇰🇷',
    'australia':'🇦🇺','saudi arabia':'🇸🇦','iran':'🇮🇷','ir iran':'🇮🇷','uruguay':'🇺🇾',
    'colombia':'🇨🇴','chile':'🇨🇱','poland':'🇵🇱','denmark':'🇩🇰','sweden':'🇸🇪',
    'norway':'🇳🇴','switzerland':'🇨🇭','croatia':'🇭🇷','serbia':'🇷🇸','ghana':'🇬🇭',
    'senegal':'🇸🇳','tunisia':'🇹🇳','cameroon':'🇨🇲','ecuador':'🇪🇨','egypt':'🇪🇬',
    'nigeria':'🇳🇬','wales':'🏴󠁧󠁢󠁷󠁬󠁳󠁿','qatar':'🇶🇦','peru':'🇵🇪','bolivia':'🇧🇴',
    'paraguay':'🇵🇾','venezuela':'🇻🇪','panama':'🇵🇦','costa rica':'🇨🇷','jamaica':'🇯🇲',
    'honduras':'🇭🇳','el salvador':'🇸🇻','guatemala':'🇬🇹','curacao':'🇨🇼','haiti':'🇭🇹',
    'algeria':'🇩🇿','ivory coast':'🇨🇮','cote d\\'ivoire':'🇨🇮','mali':'🇲🇱','burkina faso':'🇧🇫',
    'cape verde':'🇨🇻','dr congo':'🇨🇩','gabon':'🇬🇦','zambia':'🇿🇲','kenya':'🇰🇪',
    'iraq':'🇮🇶','jordan':'🇯🇴','uae':'🇦🇪','united arab emirates':'🇦🇪','oman':'🇴🇲',
    'uzbekistan':'🇺🇿','china':'🇨🇳','china pr':'🇨🇳','thailand':'🇹🇭','vietnam':'🇻🇳',
    'new zealand':'🇳🇿','fiji':'🇫🇯','turkey':'🇹🇷','türkiye':'🇹🇷','austria':'🇦🇹',
    'czech republic':'🇨🇿','czechia':'🇨🇿','slovakia':'🇸🇰','hungary':'🇭🇺','romania':'🇷🇴',
    'bulgaria':'🇧🇬','ukraine':'🇺🇦','russia':'🇷🇺','greece':'🇬🇷','iceland':'🇮🇸',
    'ireland':'🇮🇪','scotland':'🏴󠁧󠁢󠁳󠁣󠁴󠁿','northern ireland':'🇬🇧','finland':'🇫🇮','albania':'🇦🇱',
    'bosnia':'🇧🇦','bosnia and herzegovina':'🇧🇦','north macedonia':'🇲🇰','slovenia':'🇸🇮',
    'georgia':'🇬🇪','azerbaijan':'🇦🇿','armenia':'🇦🇲',
    // Localized variants used in mock payloads (es / pt-pt / pt-br)
    'inglaterra':'🏴󠁧󠁢󠁥󠁮󠁧󠁿','brasil':'🇧🇷','espanha':'🇪🇸','españa':'🇪🇸',
    'alemanha':'🇩🇪','alemania':'🇩🇪','méxico':'🇲🇽','frança':'🇫🇷','francia':'🇫🇷',
    'croácia':'🇭🇷','croacia':'🇭🇷','áfrica do sul':'🇿🇦','sudáfrica':'🇿🇦',
    'países baixos':'🇳🇱','países bajos':'🇳🇱','holanda':'🇳🇱',
    'uruguai':'🇺🇾','turquia':'🇹🇷'
  }};

  function _cleanName(name){{
    return (name || '').replace(/\\s*\\(.*?\\)\\s*$/, '').trim().toLowerCase();
  }}
  function flagFor(name){{
    // Legacy emoji helper — used only by inline rendering paths where an
    // <img> would break the surrounding text (e.g. countdown one-liners).
    if (!name) return '⚽';
    return FLAG_MAP[_cleanName(name)] || '⚽';
  }}
  // flagcdn.com only serves a fixed set of heights: h20, h24, h40, h60, h80,
  // h120, h240. Round the requested source height up to the next supported
  // bucket so requests don't 404 (which is what broke earlier).
  const FLAG_CDN_HEIGHTS = [20, 24, 40, 60, 80, 120, 240];
  function _flagCdnHeight(target){{
    for (let i = 0; i < FLAG_CDN_HEIGHTS.length; i++) {{
      if (FLAG_CDN_HEIGHTS[i] >= target) return FLAG_CDN_HEIGHTS[i];
    }}
    return 240;
  }}
  // flagImg: returns an <img> tag pointing at the flagcdn.com CDN.
  //   sizePx — display height in CSS pixels. The source request asks for
  //   ~2× for retina sharpness, snapped to the next valid CDN bucket.
  function flagImg(name, sizePx){{
    if (!name) return '';
    const code = FLAG_CODE_MAP[_cleanName(name)];
    if (!code) return '<span class="flag flag-fallback">⚽</span>';
    const srcH = _flagCdnHeight(sizePx * 2);
    const src  = 'https://flagcdn.com/h' + srcH + '/' + code + '.png';
    return '<img class="flag-img" src="' + src + '" alt="' + name +
           '" height="' + sizePx + '" loading="lazy" decoding="async" />';
  }}

  function fmtCountdown(s){{
    if (s == null || s < 0) return '—';
    const d = Math.floor(s/86400); s -= d*86400;
    const h = Math.floor(s/3600);  s -= h*3600;
    const m = Math.floor(s/60);
    if (d > 0) return d + 'd ' + h + 'h';
    if (h > 0) return h + 'h ' + (m < 10 ? '0' : '') + m + 'm';
    return m + 'm';
  }}

  function renderPill(state, minute){{
    let cls = 'pill pill-sched'; let text = t('scheduled');
    if (state === 'live') {{ cls = 'pill pill-live'; text = t('live_now') + (minute != null ? ' ' + minute + "'" : ''); }}
    else if (state === 'results_profitable' || state === 'results_losing') {{ cls = 'pill pill-ft'; text = t('finished'); }}
    pillBox.innerHTML = '<span class="' + cls + '">' + text + '</span>';
  }}

  function badge(result){{
    const r = (result || '').toLowerCase();
    if (r === 'green' || r === 'win')  return '<span class="badge badge-won">✓ ' + t('result_won') + '</span>';
    if (r === 'red'   || r === 'loss') return '<span class="badge badge-lost">✗ ' + t('result_lost') + '</span>';
    if (r === 'void')                  return '<span class="badge badge-push">↔ ' + t('result_push') + '</span>';
    return '<span class="badge badge-pending">' + t('result_pending') + '</span>';
  }}

  function renderPicks(picks){{
    if (!picks || !picks.length) return '<div class="empty">' + t('no_picks_yet') + '</div>';
    return picks.map(function(p){{
      const minStr = p.minute != null ? (p.minute + "'") : '—';
      const oddStr = p.odds ? '@' + Number(p.odds).toFixed(2) : '';
      const edgeStr = p.edge ? '+' + Number(p.edge).toFixed(1) + '%' : '';
      return ''
        + '<div class="pick-row">'
        +   '<div class="pick-left">'
        +     '<div class="pick-minute">' + minStr + '</div>'
        +     '<div><div class="pick-market">' + (p.market || '') + '</div><div class="pick-label">' + (p.label || '') + '</div></div>'
        +   '</div>'
        +   '<div class="pick-right">'
        +     (edgeStr ? '<span class="pick-edge">' + edgeStr + '</span>' : '')
        +     (oddStr  ? '<span class="pick-odds">'  + oddStr  + '</span>' : '')
        +     badge(p.result)
        +   '</div>'
        + '</div>';
    }}).join('');
  }}

  function renderMatchCard(m){{
    if (!m) return '';
    const score = (m.home_goals != null && m.away_goals != null && (m.is_finished || (m.home_goals + m.away_goals) > 0))
      ? (m.home_goals + ' — ' + m.away_goals) : 'vs';
    // Stacked layout per team: real flag image on top, country/team name
    // below, both centered. flagImg returns <span class="flag-fallback">⚽
    // when the name is not in FLAG_CODE_MAP (e.g. club fixtures used as
    // ?match_id= rehearsal).
    return ''
      + '<div class="matchcard">'
      +   '<div class="matchrow">'
      +     '<div class="team">' + flagImg(m.home, 56) + '<div class="team-name">' + (m.home || '') + '</div></div>'
      +     '<div class="score">' + score + '</div>'
      +     '<div class="team away">' + flagImg(m.away, 56) + '<div class="team-name">' + (m.away || '') + '</div></div>'
      +   '</div>'
      +   '<div class="meta-line">' + (m.tournament || 'FIFA World Cup 2026') + '</div>'
      + '</div>';
  }}

  // Inline SVG xG chart — cumulative xG line for each team over match minutes.
  // Sized to fit the iframe column; renders nothing if no timeline data.
  function renderXgChart(d){{
    const tl = d && d.xg_timeline;
    if (!Array.isArray(tl) || tl.length < 2) return '';

    const W = 580, H = 170;
    const padL = 32, padR = 12, padT = 14, padB = 26;

    const maxMin = Math.max(90, ...tl.map(p => p.minute || 0));
    const maxXg  = Math.max(0.5, ...tl.map(p => Math.max(p.home || 0, p.away || 0))) + 0.3;
    const sx = m => padL + (m / maxMin) * (W - padL - padR);
    const sy = v => H - padB - (v / maxXg) * (H - padT - padB);

    const buildPath = (key) => tl.map(p => sx(p.minute || 0) + ',' + sy(p[key] || 0)).join(' ');
    const homePts = buildPath('home');
    const awayPts = buildPath('away');

    // y-axis gridlines (0, 0.5, 1.0, 1.5, 2.0 …)
    const yStep = maxXg > 2.5 ? 1.0 : 0.5;
    let grid = '';
    for (let v = 0; v <= maxXg; v += yStep) {{
      const y = sy(v);
      grid += '<line x1="' + padL + '" y1="' + y + '" x2="' + (W - padR) + '" y2="' + y + '" stroke="currentColor" stroke-opacity="0.08" stroke-width="1"/>';
      grid += '<text x="' + (padL - 6) + '" y="' + (y + 3) + '" font-size="9" text-anchor="end" fill="currentColor" opacity="0.5">' + v.toFixed(1) + '</text>';
    }}

    // x-axis ticks (0, 15, 30, 45, 60, 75, 90)
    let xticks = '';
    [0, 15, 30, 45, 60, 75, 90].forEach(m => {{
      if (m > maxMin) return;
      const x = sx(m);
      xticks += '<line x1="' + x + '" y1="' + (H - padB) + '" x2="' + x + '" y2="' + (H - padB + 3) + '" stroke="currentColor" stroke-opacity="0.3"/>';
      xticks += '<text x="' + x + '" y="' + (H - padB + 14) + '" font-size="9" text-anchor="middle" fill="currentColor" opacity="0.55">' + m + "'" + '</text>';
    }});

    // Filled area under each line (soft, low opacity)
    const areaHome = padL + ',' + sy(0) + ' ' + homePts + ' ' + sx(tl[tl.length-1].minute) + ',' + sy(0);
    const areaAway = padL + ',' + sy(0) + ' ' + awayPts + ' ' + sx(tl[tl.length-1].minute) + ',' + sy(0);

    const last = tl[tl.length - 1] || {{home:0, away:0}};
    const homeName = (d.match && d.match.home) || 'Home';
    const awayName = (d.match && d.match.away) || 'Away';

    const svg = '<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="xG momentum chart">'
      + '<rect x="' + padL + '" y="' + padT + '" width="' + (W - padL - padR) + '" height="' + (H - padT - padB) + '" fill="transparent"/>'
      + grid + xticks
      + '<polygon points="' + areaHome + '" fill="#10b981" fill-opacity="0.12"/>'
      + '<polygon points="' + areaAway + '" fill="#ef4444" fill-opacity="0.12"/>'
      + '<polyline points="' + homePts + '" fill="none" stroke="#10b981" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>'
      + '<polyline points="' + awayPts + '" fill="none" stroke="#ef4444" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>'
      + '</svg>';

    const legend = '<div class="legend">'
      + '<span><span class="dot dot-home"></span>' + homeName + ' · <span class="totals">' + (last.home || 0).toFixed(2) + ' xG</span></span>'
      + '<span><span class="dot dot-away"></span>' + awayName + ' · <span class="totals">' + (last.away || 0).toFixed(2) + ' xG</span></span>'
      + '</div>';

    return '<div class="xg-chart">' + svg + legend + '</div>';
  }}

  function renderLive(d){{
    return renderMatchCard(d.match)
      + '<div class="section-title">' + t('xg_momentum') + '</div>'
      + renderXgChart(d)
      + '<div class="section-title">' + t('algo_picks') + '</div>'
      + renderPicks(d.picks);
  }}

  function renderResults(d){{
    let strip = '';
    if (d.match_pnl != null) {{
      const cls = d.match_pnl > 0 ? 'profit' : 'loss';
      const sign = d.match_pnl > 0 ? '+' : '';
      strip = '<div class="pnl-strip ' + cls + '">' + t('perf_pnl') + ': ' + sign + d.match_pnl + '€</div>';
    }}
    let next = '';
    if (d.next_match && d.countdown_to_next_kickoff_s != null) {{
      // Inline mini-flag — only emitted when we have a code (no broken-image
      // placeholder if a country isn't in FLAG_CODE_MAP). h40 is the smallest
      // bucket flagcdn.com supports that's still crisp at ~18px display.
      const inlineFlag = (n) => {{
        const c = FLAG_CODE_MAP[_cleanName(n)];
        return c ? '<img class="flag-inline" src="https://flagcdn.com/h40/' + c + '.png" alt="" />' : '';
      }};
      next = '<div class="countdown"><div class="label">' + t('next_up') + ' · ' + inlineFlag(d.next_match.home) + d.next_match.home + ' vs ' + d.next_match.away + inlineFlag(d.next_match.away) + '</div><div class="value">' + t('kickoff_in') + ' ' + fmtCountdown(d.countdown_to_next_kickoff_s) + '</div></div>';
    }}
    return renderMatchCard(d.match)
      + strip
      + '<div class="section-title">' + t('result_timeline') + '</div>'
      + renderPicks(d.picks)
      + next;
  }}

  function renderPreview(d){{
    const nm = d.next_match;
    if (!nm) return '<div class="empty">' + t('no_data_yet') + '</div>';
    const teams = ''
      + '<div class="matchrow" style="margin-top:10px">'
      +   '<div class="team">' + flagImg(nm.home, 56) + '<div class="team-name">' + nm.home + '</div></div>'
      +   '<div class="score" style="font-size:1.2rem">vs</div>'
      +   '<div class="team away">' + flagImg(nm.away, 56) + '<div class="team-name">' + nm.away + '</div></div>'
      + '</div>';
    const cd = d.countdown_to_next_kickoff_s != null
      ? '<div class="value">' + t('kickoff_in') + ' ' + fmtCountdown(d.countdown_to_next_kickoff_s) + '</div>'
      : '';
    const preview = d.model_preview_text
      ? '<div class="model-preview"><strong>' + t('model_preview') + ':</strong> ' + d.model_preview_text + '</div>'
      : '';
    return '<div class="preview-card"><div class="label">' + t('next_up') + ' · ' + (nm.tournament || 'FIFA World Cup 2026') + '</div>'
      + teams + cd + preview + '</div>';
  }}

  function renderOffDay(d){{
    const cd = d.countdown_to_next_kickoff_s != null
      ? '<div class="countdown"><div class="label">' + t('wc_resumes_in') + '</div><div class="value">' + fmtCountdown(d.countdown_to_next_kickoff_s) + '</div></div>'
      : '';

    // List of next 3 upcoming WC fixtures, InBet-style layout:
    //   [team-home  flag]   [DAY/TIME/VS]   [flag  team-away]
    // CSS centres the middle column and shrinks flags + fonts on mobile.
    const list = Array.isArray(d.upcoming_matches) ? d.upcoming_matches : [];
    let listHtml = '';
    if (list.length > 0) {{
      listHtml = '<div class="section-title">' + t('upcoming_matches') + '</div>'
        + '<div class="upcoming-list">'
        + list.slice(0, 3).map(function(m){{
            const dt = new Date((m.kickoff_ts || 0) * 1000);
            const day = dt.toLocaleDateString(undefined, {{day:'2-digit', month:'short'}});
            const time = dt.toLocaleTimeString(undefined, {{hour:'2-digit', minute:'2-digit'}});
            return ''
              + '<div class="upcoming-row">'
              +   '<div class="upcoming-side home">'
              +     '<span class="upcoming-team">' + (m.home || '') + '</span>'
              +     flagImg(m.home, 32)
              +   '</div>'
              +   '<div class="upcoming-mid">'
              +     '<span class="day">' + day + '</span>'
              +     '<span class="time">' + time + '</span>'
              +     '<span class="vs">vs</span>'
              +   '</div>'
              +   '<div class="upcoming-side away">'
              +     flagImg(m.away, 32)
              +     '<span class="upcoming-team">' + (m.away || '') + '</span>'
              +   '</div>'
              + '</div>';
          }}).join('')
        + '</div>';
    }}

    if (!cd && !listHtml) return '<div class="empty">' + t('no_live_match') + '</div>';
    return cd + listHtml;
  }}

  // "Get Pick Alerts" CTA — same DOM in every state, deep-links to the
  // dedicated InBet Telegram bot. Localised label per data.lang.
  const ALERTS_CTA_HTML = ''
    + '<div class="alerts-cta-wrap">'
    +   '<a class="alerts-cta" href="https://t.me/InBetWC2026_Bot" '
    +      'target="_blank" rel="noopener" '
    +      'aria-label="' + t('get_alerts') + '">'
    +     '<span class="tg" aria-hidden="true">📲</span>'
    +     '<span>' + t('get_alerts') + '</span>'
    +   '</a>'
    + '</div>';

  function render(d){{
    let body = '';
    switch (d.state) {{
      case 'live':                body = renderLive(d); break;
      case 'results_profitable':
      case 'results_losing':      body = renderResults(d); break;
      case 'preview':             body = renderPreview(d); break;
      default:                    body = renderOffDay(d);
    }}
    // Always-visible CTA below the state body so members can link their
    // Telegram from any screen.
    root.innerHTML = body + ALERTS_CTA_HTML;
    renderPill(d.state, d.match && d.match.minute);
    // postMessage host for iframe auto-resize
    try {{
      parent.postMessage({{type:'webpronos:resize', height: document.body.scrollHeight}}, '*');
    }} catch(e){{}}
  }}

  async function tick(){{
    try {{
      const r = await fetch(apiUrl, {{cache:'no-store'}});
      if (!r.ok) throw new Error('http ' + r.status);
      const d = await r.json();
      render(d);
      clearTimeout(timer);
      timer = setTimeout(tick, d.next_poll_after_ms || 60000);
    }} catch(e) {{
      root.innerHTML = '<div class="empty">' + t('no_data_yet') + '</div>';
      clearTimeout(timer);
      timer = setTimeout(tick, 30000);
    }}
  }}
  tick();
}})();
</script>
</body>
</html>
"""


@app.route("/widget/wc2026/current")
def r_wc2026_widget_current():
    """Per-match state-machine widget. Self-contained HTML page for <iframe> embed."""
    locale = _widget_locale(flask_request.args.get("lang"))
    theme  = (flask_request.args.get("theme") or "dark").strip().lower()
    if theme not in ("dark", "light"):
        theme = "dark"
    # Default accent matches the InBet members area orange. Callers can still
    # override with ?accent=#HEX for a different brand colour.
    accent_raw = (flask_request.args.get("accent") or "#ff8a1e").strip()
    import re as _re
    accent = accent_raw if _re.match(r"^#[0-9a-fA-F]{3,8}$", accent_raw) else "#10b981"

    # Inline the locale's COPY as JS so the page works without a second fetch.
    copy_for_locale = WIDGET_COPY.get(locale, WIDGET_COPY[WIDGET_DEFAULT_LOCALE])
    html = _WC_WIDGET_MATCH_HTML.format(
        lang        = locale,
        theme       = theme,
        accent      = accent,
        emblem      = WC2026_EMBLEM_URL,
        emblem_alt  = _t(locale, "wc_emblem_alt"),
        powered_by  = _t(locale, "powered_by"),
        copy_json   = json.dumps(copy_for_locale, ensure_ascii=False),
    )
    return html, 200, {
        "Content-Type":  "text/html; charset=utf-8",
        "Cache-Control": "public, max-age=60",
        "Access-Control-Allow-Origin": "*",
        "X-Frame-Options": "ALLOWALL",
        "Content-Security-Policy": "frame-ancestors *",
    }


@app.route("/api/wc2026/performance.json")
def r_wc2026_performance_json():
    """JSON for the performance dashboard widget. Cached 5 min.

    Query params:
        lang  — en | es | pt-pt | pt-br
        mock  — if truthy (1, true, yes), return synthetic dashboard data
                with realistic-looking numbers. Useful before the WC starts
                when the tips table has no settled WC picks yet.
    """
    locale = _widget_locale(flask_request.args.get("lang"))
    mock = (flask_request.args.get("mock") or "").strip().lower() in ("1", "true", "yes", "on")

    if mock:
        data = _wc2026_mock_performance(locale)
        data = _localize_performance_payload(data, locale)
        return jsonify(data), 200, {
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
        }

    # Real data path. We cache the raw (English-canonical) payload for 5 min,
    # then localise on the way out per request so different locales hit the
    # same underlying cache.
    cache_key = "wc2026_performance"
    entry = _seo_cache.get(cache_key)
    if entry and (time.time() - entry["ts"]) < 300:
        try:
            data = json.loads(entry["html"])
        except Exception:
            data = _wc2026_performance()
            _seo_cache_put(cache_key, json.dumps(data))
    else:
        data = _wc2026_performance()
        _seo_cache_put(cache_key, json.dumps(data))

    data["_lang"] = locale
    data = _localize_performance_payload(data, locale)
    return jsonify(data), 200, {
        "Cache-Control": "public, max-age=300",
        "Access-Control-Allow-Origin": "*",
    }


# ─── Performance dashboard widget HTML ──────────────────────────────────────
_WC_WIDGET_PERF_HTML = """<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="robots" content="noindex,nofollow">
<title>InBetIO Live xG Model — World Cup 2026 Performance</title>
<style>
  /* Default dark palette — matches the app.inbet.io members area chrome so
     the iframe blends seamlessly. Deep midnight navy bg, slightly lighter
     card, warm orange primary accent, royal-blue secondary accent. */
  :root{{
    --bg:#0c1126; --card:#181f38; --border:#252d4a; --text:#ffffff;
    --meta:#8b95a9; --accent:{accent}; --accent-2:#2667ff;
    --green:#22c55e; --red:#ef4444; --amber:#fbbf24; --cyan:#3b82f6;
  }}
  body[data-theme="light"]{{
    --bg:#f8fafc; --card:#ffffff; --border:#e5e7eb; --text:#0f172a;
    --meta:#64748b;
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",sans-serif;line-height:1.5}}
  .header{{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid var(--border)}}
  .header img.emblem{{width:36px;height:auto;display:block}}
  .header .brand{{font-weight:700;font-size:.95rem}}
  .header .brand span{{display:block;font-size:.72rem;color:var(--meta);font-weight:500;letter-spacing:.04em}}
  .container{{padding:16px}}
  /* "Get Pick Alerts" CTA — same look as the per-match widget */
  .alerts-cta-wrap{{display:flex;justify-content:center;margin:20px 0 12px}}
  .alerts-cta{{display:inline-flex;align-items:center;gap:8px;
    background:var(--accent);color:#1a1a2e;
    font-weight:800;font-size:.95rem;letter-spacing:.02em;
    padding:11px 22px;border-radius:999px;text-decoration:none;
    box-shadow:0 4px 12px rgba(255,138,30,.30);
    transition:transform .12s ease, box-shadow .12s ease, filter .12s ease;}}
  .alerts-cta:hover{{transform:translateY(-1px);filter:brightness(1.05);
    box-shadow:0 6px 16px rgba(255,138,30,.40)}}
  .alerts-cta:active{{transform:translateY(0)}}
  .alerts-cta .tg{{font-size:1.1rem;line-height:1}}
  .stat-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:18px}}
  .stat{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px 14px}}
  .stat .label{{font-size:.7rem;color:var(--meta);text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}}
  .stat .value{{font-size:1.5rem;font-weight:800}}
  .stat.profit .value{{color:var(--green)}}
  .stat.loss .value{{color:var(--red)}}
  .section-title{{font-size:.72rem;color:var(--meta);font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin:18px 0 10px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}}
  svg.chart{{display:block;width:100%;height:auto}}
  .greens-list .row{{display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);gap:10px;font-size:.88rem}}
  .greens-list .row:last-child{{border:none}}
  .greens-list .match{{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .greens-list .label{{font-size:.7rem;color:var(--meta);margin-top:2px}}
  .greens-list .pnl{{color:var(--green);font-weight:700;white-space:nowrap}}
  .greens-list .odds{{color:var(--meta);font-size:.78rem;margin-right:6px}}
  .markets-bar{{display:flex;flex-direction:column;gap:6px}}
  .markets-bar .row{{display:flex;align-items:center;gap:10px;font-size:.82rem}}
  .markets-bar .name{{flex:0 0 110px;color:var(--meta);font-weight:600}}
  .markets-bar .bar{{flex:1;height:8px;background:rgba(255,255,255,.04);border-radius:4px;overflow:hidden;position:relative}}
  body[data-theme="light"] .markets-bar .bar{{background:rgba(0,0,0,.04)}}
  .markets-bar .fill{{height:100%;border-radius:4px}}
  .markets-bar .fill.profit{{background:linear-gradient(90deg,var(--green) 0%,#34d399 100%)}}
  .markets-bar .fill.loss{{background:linear-gradient(90deg,var(--red) 0%,#fb7185 100%)}}
  .markets-bar .pnl{{flex:0 0 70px;text-align:right;font-weight:600}}
  .markets-bar .pnl.profit{{color:var(--green)}} .markets-bar .pnl.loss{{color:var(--red)}}
  .empty{{text-align:center;color:var(--meta);padding:20px;font-size:.85rem}}
  .footer{{text-align:center;padding:12px 16px;border-top:1px solid var(--border);font-size:.7rem;color:var(--meta);letter-spacing:.04em}}
</style>
</head>
<body data-theme="{theme}">
<div class="header">
  <img class="emblem" src="{emblem}" alt="{emblem_alt}">
  <div class="brand">{title}<span>{powered_by}</span></div>
</div>
<div id="app" class="container"><div class="empty">Loading…</div></div>
<div class="footer">{footer_label}</div>
<script>
(function(){{
  const params = new URLSearchParams(location.search);
  const lang   = params.get('lang')  || 'en';
  // ?mock=1 → server returns synthetic dashboard data. Used for pre-launch
  // QA and for screenshots when no settled WC picks exist yet.
  const mock   = params.get('mock');
  let apiUrl = '/api/wc2026/performance.json?lang=' + encodeURIComponent(lang);
  if (mock) apiUrl += '&mock=' + encodeURIComponent(mock);
  const root   = document.getElementById('app');
  const COPY   = {copy_json};

  function t(k){{ return (COPY && COPY[k]) || k; }}
  function fmtPnL(v){{
    if (v == null) return '0€';
    const sign = v > 0 ? '+' : '';
    return sign + Number(v).toFixed(0) + '€';
  }}
  function fmtPct(v){{ if (v == null) return '0%'; const s = v > 0 ? '+' : ''; return s + Number(v).toFixed(1) + '%'; }}

  function renderEquity(points){{
    if (!points || points.length < 2) return '<div class="empty">' + t('no_data_yet') + '</div>';
    const w = 600, h = 160, pad = 24;
    const pnls = points.map(function(p){{return p.cum_pnl;}});
    const minY = Math.min.apply(null, pnls.concat([0]));
    const maxY = Math.max.apply(null, pnls.concat([0]));
    const span = Math.max(1, maxY - minY);
    const stepX = (w - pad*2) / (points.length - 1);
    const yFor = function(v){{ return h - pad - ((v - minY) / span) * (h - pad*2); }};
    const path = points.map(function(p,i){{return (i===0?'M':'L') + (pad + i*stepX).toFixed(1) + ',' + yFor(p.cum_pnl).toFixed(1);}}).join(' ');
    // zero line
    const zeroY = yFor(0);
    return '<svg class="chart" viewBox="0 0 ' + w + ' ' + h + '" xmlns="http://www.w3.org/2000/svg">'
      + '<line x1="' + pad + '" y1="' + zeroY.toFixed(1) + '" x2="' + (w-pad) + '" y2="' + zeroY.toFixed(1) + '" stroke="rgba(255,255,255,0.15)" stroke-dasharray="3,4"/>'
      + '<path d="' + path + '" fill="none" stroke="var(--accent)" stroke-width="2.5" stroke-linejoin="round"/>'
      + '</svg>';
  }}

  function renderGreens(greens){{
    if (!greens || !greens.length) return '<div class="empty">' + t('no_data_yet') + '</div>';
    return '<div class="greens-list">' + greens.map(function(g){{
      const min = g.minute_entered != null ? (' · ' + g.minute_entered + "'") : '';
      return ''
        + '<div class="row">'
        +   '<div style="flex:1;min-width:0">'
        +     '<div class="match">' + g.match + '</div>'
        +     '<div class="label">' + (g.market || '') + ' · ' + (g.label || '') + min + '</div>'
        +   '</div>'
        +   '<span class="odds">@' + Number(g.odds).toFixed(2) + '</span>'
        +   '<span class="pnl">+' + Number(g.profit).toFixed(0) + '€</span>'
        + '</div>';
    }}).join('') + '</div>';
  }}

  function renderMarkets(by){{
    if (!by || !by.length) return '<div class="empty">' + t('no_data_yet') + '</div>';
    const maxAbs = Math.max.apply(null, by.map(function(m){{return Math.abs(m.pnl);}}).concat([1]));
    return '<div class="markets-bar">' + by.map(function(m){{
      const pct = (Math.abs(m.pnl)/maxAbs) * 100;
      const cls = m.pnl >= 0 ? 'profit' : 'loss';
      return ''
        + '<div class="row">'
        +   '<div class="name">' + (m.market || '?') + '</div>'
        +   '<div class="bar"><div class="fill ' + cls + '" style="width:' + pct.toFixed(1) + '%"></div></div>'
        +   '<div class="pnl ' + cls + '">' + fmtPnL(m.pnl) + '</div>'
        + '</div>';
    }}).join('') + '</div>';
  }}

  // Same orange CTA as the per-match widget — keeps the two iframes
  // visually consistent on the InBet members area.
  const ALERTS_CTA_HTML = ''
    + '<div class="alerts-cta-wrap">'
    +   '<a class="alerts-cta" href="https://t.me/InBetWC2026_Bot" '
    +      'target="_blank" rel="noopener" '
    +      'aria-label="' + t('get_alerts') + '">'
    +     '<span class="tg" aria-hidden="true">📲</span>'
    +     '<span>' + t('get_alerts') + '</span>'
    +   '</a>'
    + '</div>';

  function render(d){{
    if (!d || d.settled === 0) {{
      root.innerHTML = '<div class="empty">' + t('no_data_yet') + '</div>'
                     + ALERTS_CTA_HTML;
      return;
    }}
    const pnlCls = (d.pnl || 0) >= 0 ? 'profit' : 'loss';
    const roiCls = (d.roi || 0) >= 0 ? 'profit' : 'loss';
    root.innerHTML = ''
      + '<div class="stat-grid">'
      +   '<div class="stat"><div class="label">' + t('perf_picks')   + '</div><div class="value">' + d.settled + '</div></div>'
      +   '<div class="stat"><div class="label">' + t('perf_winrate') + '</div><div class="value">' + Number(d.winrate || 0).toFixed(1) + '%</div></div>'
      +   '<div class="stat ' + pnlCls + '"><div class="label">' + t('perf_pnl') + '</div><div class="value">' + fmtPnL(d.pnl) + '</div></div>'
      +   '<div class="stat ' + roiCls + '"><div class="label">' + t('perf_roi') + '</div><div class="value">' + fmtPct(d.roi) + '</div></div>'
      + '</div>'
      + '<div class="section-title">' + t('perf_equity') + '</div>'
      + '<div class="card">' + renderEquity(d.equity_curve) + '</div>'
      + '<div class="section-title">' + t('perf_top_greens') + '</div>'
      + '<div class="card">' + renderGreens(d.top_greens) + '</div>'
      + '<div class="section-title">' + t('perf_by_market') + '</div>'
      + '<div class="card">' + renderMarkets(d.by_market) + '</div>'
      + ALERTS_CTA_HTML;
    try {{ parent.postMessage({{type:'webpronos:resize', height: document.body.scrollHeight}}, '*'); }} catch(e){{}}
  }}

  async function tick(){{
    try {{
      const r = await fetch(apiUrl, {{cache:'no-store'}});
      if (!r.ok) throw new Error('http ' + r.status);
      const d = await r.json();
      render(d);
    }} catch(e) {{
      root.innerHTML = '<div class="empty">' + t('no_data_yet') + '</div>';
    }}
    setTimeout(tick, 5*60*1000);
  }}
  tick();
}})();
</script>
</body>
</html>
"""


@app.route("/widget/wc2026/performance")
def r_wc2026_widget_performance():
    """Performance dashboard widget — self-contained HTML for <iframe> embed."""
    locale = _widget_locale(flask_request.args.get("lang"))
    theme  = (flask_request.args.get("theme") or "dark").strip().lower()
    if theme not in ("dark", "light"):
        theme = "dark"
    # Default accent matches the InBet members area orange. Callers can still
    # override with ?accent=#HEX for a different brand colour.
    accent_raw = (flask_request.args.get("accent") or "#ff8a1e").strip()
    import re as _re
    accent = accent_raw if _re.match(r"^#[0-9a-fA-F]{3,8}$", accent_raw) else "#10b981"

    copy_for_locale = WIDGET_COPY.get(locale, WIDGET_COPY[WIDGET_DEFAULT_LOCALE])
    html = _WC_WIDGET_PERF_HTML.format(
        lang         = locale,
        theme        = theme,
        accent       = accent,
        emblem       = WC2026_EMBLEM_URL,
        emblem_alt   = _t(locale, "wc_emblem_alt"),
        title        = _t(locale, "perf_title"),
        powered_by   = _t(locale, "powered_by"),
        footer_label = _t(locale, "perf_updated"),
        copy_json    = json.dumps(copy_for_locale, ensure_ascii=False),
    )
    return html, 200, {
        "Content-Type":  "text/html; charset=utf-8",
        "Cache-Control": "public, max-age=300",
        "Access-Control-Allow-Origin": "*",
        "X-Frame-Options": "ALLOWALL",
        "Content-Security-Policy": "frame-ancestors *",
    }


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

    i18n: pass `?lang=en|pt-br|pt-pt|es` and every `livePicks[]` / `tips[]`
    entry gains `market_localized` + `label_localized` siblings to the
    canonical EN `market` / `label` fields. The SPA renders the localised
    versions directly. Default = en (back-compat).

    Short Cache-Control header (5s) so back-to-back navigations don't
    re-hit Flask for the same payload. Cache key varies by lang via Vary
    header so each locale has its own edge cache slot.
    """
    lang_q = (flask_request.args.get("lang") or "en").strip().lower()
    locale = lang_q if lang_q in ("en", "pt-br", "pt-pt", "es") else "en"

    with _state_lock:
        state_copy = dict(_live_state)

    # Augment each game's livePicks + tips with localised labels (when
    # the request asks for a non-EN locale). Cheap (string ops on small
    # lists — typically <5 picks per game).
    games_out = []
    for g in state_copy.values():
        if locale != "en":
            gd = dict(g)
            for key in ("livePicks", "tips"):
                rows = gd.get(key) or []
                gd[key] = [
                    {**r,
                     "market_localized": _xlate_market(r.get("market") or "", locale),
                     "label_localized":  _xlate_pick_label(r.get("label") or "", locale)}
                    for r in rows
                ]
            games_out.append(gd)
        else:
            games_out.append(g)

    resp = jsonify({
        "games":    games_out,
        "count":    len(games_out),
        "cycleTsIso": datetime.fromtimestamp(_last_cycle_ts, tz=timezone.utc).isoformat() if _last_cycle_ts else None,
        "cycleReq": _last_cycle_req,
        "quotaRemaining": _api_requests_remaining,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    resp.headers["Cache-Control"] = "public, max-age=5, s-maxage=5"
    resp.headers["Vary"] = "Accept-Language"
    return resp


def _current_edge_for_tip(market: str, label: str, odds: dict, match: dict):
    """Live edge% for a historical tip given the current full odds analysis.

    Returns None when the tip's market/outcome can't be matched (e.g. the
    line has moved since the pick was issued, or the bookmaker no longer
    quotes that outcome). Otherwise returns a float in percent units
    (same scale as `edge_entry`, e.g. +13.6 or -4.2).
    """
    if not odds or not odds.get("available"):
        return None
    benter = odds.get("benter") or {}
    home = match.get("homeTeam") or ""
    away = match.get("awayTeam") or ""
    market = (market or "").strip()
    label = (label or "").strip()
    lb_low = label.lower()
    import re as _re_edge

    # 1X2
    if market == "1X2":
        outs = (benter.get("h2h") or {}).get("outcomes") or {}
        if lb_low in ("draw", "empate"):
            side = "draw"
        elif label == home or lb_low == "home":
            side = "home"
        elif label == away or lb_low == "away":
            side = "away"
        else:
            return None
        o = outs.get(side) or {}
        return o.get("edge") if o.get("bookieOdds") else None

    # Totals — market is like "O/U 2.5"; line must still match.
    if market.startswith("O/U") or market in ("Totals", "Over/Under"):
        bt = benter.get("totals") or {}
        outs = bt.get("outcomes") or {}
        m_lbl = _re_edge.search(r"(\d+\.?\d*)", label)
        m_mkt = _re_edge.search(r"(\d+\.?\d*)", market)
        tip_line = float(m_lbl.group(1)) if m_lbl else (float(m_mkt.group(1)) if m_mkt else None)
        cur_line = bt.get("line")
        if tip_line is not None and cur_line is not None and abs(tip_line - cur_line) > 0.05:
            return None
        if lb_low.startswith("over"):
            side = "over"
        elif lb_low.startswith("under"):
            side = "under"
        else:
            return None
        o = outs.get(side) or {}
        return o.get("edge") if o.get("bookieOdds") else None

    # Handicap — label is "{team} {±N}"; both team AND line must still match.
    if market.lower() == "handicap":
        bs = benter.get("spreads") or {}
        outs = bs.get("outcomes") or {}
        m_pt = _re_edge.search(r"([+-]?\d+\.?\d*)\s*$", label)
        tip_point = float(m_pt.group(1)) if m_pt else None
        if home and label.startswith(home):
            cur_point = bs.get("homePoint")
            side = "home"
        elif away and label.startswith(away):
            cur_point = bs.get("awayPoint")
            side = "away"
        else:
            return None
        if tip_point is not None and cur_point is not None and abs(tip_point - cur_point) > 0.05:
            return None
        o = outs.get(side) or {}
        return o.get("edge") if o.get("bookieOdds") else None

    return None


@app.route("/api/match/<int:mid>/timeline")
def r_match_timeline(mid: int):
    """
    Returns full xG timeline data for a match — used for the animated replay UI.
    Response:
      {
        "match": { id, home_team, away_team, home_goals, away_goals, tournament, start_ts },
        "shots": [ { minute, added_time, is_home, xg, is_goal, is_penalty, player, shot_type } ],
        "tips":  [ { minute_entry, market, label, odd_entry, result,
                     xg_home_at_entry, xg_away_at_entry, wall_ts, edge_entry,
                     current_edge, current_status } ]
      }
    `current_edge` and `current_status` are only computed while the match
    is live. Possible `current_status` values:
      - "valid"            → live and edge ≥ threshold (default 10%)
      - "no_value"         → live but edge has dropped below threshold
      - "resolved_green"   → tip already won
      - "resolved_red"     → tip already lost
      - "resolved_void"    → tip pushed
      - "pending"          → match not live (pre/finished) and unresolved
      - "unknown"          → match live but line moved / odds unavailable
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
                WHERE match_id = ? AND minute >= 0
                ORDER BY minute, added_time, is_home
            """, (mid,)).fetchall()

            tips = conn.execute("""
                SELECT minute_entry, market, label, odd_entry, result,
                       xg_home_at_entry, xg_away_at_entry, wall_ts, edge_entry
                FROM tips
                WHERE match_id = ?
                ORDER BY wall_ts
            """, (mid,)).fetchall()

        # Compute per-tip current edge + status. Only meaningful while
        # the match is live — finished matches are status='pending' or
        # 'resolved_*' based on the stored result.
        match_obj = {}
        live_odds = None
        try:
            with _state_lock:
                entry = _live_state.get(mid)
            if entry:
                match_obj = entry.get("match", {}) or {}
                live_odds = entry.get("odds")
        except Exception:
            pass
        is_live = bool(match_obj.get("isLive")) and not bool(match_obj.get("isFinished"))
        edge_threshold = float(get_setting("min_edge_pct", 10.0))

        tips_out = []
        for t in tips:
            td = dict(t)
            result = td.get("result")
            if result in ("green", "red", "void"):
                td["current_status"] = f"resolved_{result}"
                td["current_edge"] = None
            elif not is_live:
                td["current_status"] = "pending"
                td["current_edge"] = None
            else:
                ce = _current_edge_for_tip(td["market"], td["label"], live_odds or {}, match_obj)
                if ce is None:
                    td["current_status"] = "unknown"
                    td["current_edge"] = None
                else:
                    td["current_edge"] = round(float(ce), 1)
                    td["current_status"] = "valid" if ce >= edge_threshold else "no_value"
            tips_out.append(td)

        return jsonify({
            "match": dict(game),
            "shots": [dict(s) for s in shots],
            "tips":  tips_out,
            "edge_threshold": edge_threshold,
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
                FROM match_shots WHERE match_id = ? AND minute >= 0
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
    """Returns tip history (all games with tips) from the DB, optionally filtered by date range.

    i18n: pass `?lang=en|pt-br|pt-pt|es` and each tip in the response
    gains `market_localized` + `label_localized` siblings to the
    canonical `market` / `label` fields. The SPA can then use the
    localized versions directly without maintaining its own translation
    map. Defaults to EN if omitted (back-compat — old clients keep working).
    """
    from_ts = flask_request.args.get("from_ts", type=int)   # unix seconds
    to_ts   = flask_request.args.get("to_ts",   type=int)
    limit   = flask_request.args.get("limit", 500, type=int)
    lang_q  = (flask_request.args.get("lang") or "en").strip().lower()
    # Normalize to one of the 4 supported locale codes we localize for.
    locale  = lang_q if lang_q in ("en", "pt-br", "pt-pt", "es") else "en"

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
            # Augment each tip with localised variants so the SPA can render
            # directly. Canonical `market`/`label` stay in EN for analytics
            # / cross-locale consistency; `market_localized`/`label_localized`
            # are the strings to display.
            tips_list = []
            for t in tips_rows:
                td = dict(t)
                td["market_localized"] = _xlate_market(td.get("market") or "", locale)
                td["label_localized"]  = _xlate_pick_label(td.get("label") or "", locale)
                tips_list.append(td)
            gd["tips"] = tips_list
            # Inject logos directly so the frontend makes zero extra requests.
            # Pass country to disambiguate homonyms (Athletic Club, etc).
            _country = g["country"] if "country" in g.keys() else None
            gd["home_logo"] = _quick_logo(g["home_team"], _country or None)
            gd["away_logo"] = _quick_logo(g["away_team"], _country or None)
            # Canonicalise tournament so cards/links land users on the
            # unified league page (e.g. 'Pro League, Conference League
            # Playoffs' → 'Pro League'). Raw kept for traceability.
            if gd.get("tournament"):
                gd["tournament_raw"] = gd["tournament"]
                gd["tournament"] = _normalize_tournament_pretty(gd["tournament"]) or gd["tournament"]
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
# We read from TWO tabs in the same sheet:
#   1. team_logos_master     — primary, we own. Auto-populated with every team
#                              name we've ever seen (col A) + manual logo URL (col B).
#                              Wins on conflicts.
#   2. footballstats team logo — legacy, large but with name-mismatch issues.
#                              Used as fallback for teams not yet filled in tab 1.
_LOGOS_SHEET_PRIMARY = (
    "https://docs.google.com/spreadsheets/d/"
    "1tDUlWmZZcJKXHd0Nlr5QIm1V15OMsvkOgfhXUuPI9_M/"
    "gviz/tq?tqx=out:csv&sheet=team_logos_master"
)
_LOGOS_SHEET_LEGACY = (
    "https://docs.google.com/spreadsheets/d/"
    "1tDUlWmZZcJKXHd0Nlr5QIm1V15OMsvkOgfhXUuPI9_M/"
    "gviz/tq?tqx=out:csv&sheet=footballstats+team+logo"
)
# Kept for backwards-compat callers — points to the primary tab now.
_LOGOS_SHEET = _LOGOS_SHEET_PRIMARY
_logos_cache: dict = {}        # original_name → url
_logos_norm_cache: dict = {}   # normalized_name → url  (for fuzzy lookup)
_logos_ts: float  = 0.0
_LOGOS_TTL = 600               # refresh every 10 min

# Shared on-disk snapshot. With multiple gunicorn workers each process has
# its own _logos_cache; one worker calling /api/admin/refresh-logos only
# updates ITS cache, leaving the others returning stale fuzzy matches (or
# stale 'None' entries cached in _fuzzy_logo_memo). Solution: every refresh
# also writes to /data/logos_cache.json (persistent volume), and every
# worker's _get_logos() checks the file mtime — if newer than its in-memory
# state, the worker reloads from disk (fast) and clears its memo. End result:
# changes propagate across all workers within one request.
_LOGOS_DISK_SNAPSHOT = "/data/logos_cache.json"
_logos_disk_mtime_seen: float = 0.0

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
    """Pull both sheet tabs and merge into one cache.

    Order of precedence (last write wins per key):
      1. legacy tab loaded FIRST (broad coverage, name-mismatch risk)
      2. primary 'team_logos_master' loaded SECOND, overwrites legacy entries
         when a curated row exists. This guarantees the manual list always
         beats stale fuzzy hits from the old tab.
    """
    global _logos_cache, _logos_norm_cache, _logos_ts
    import csv, io

    def _ingest(csv_text: str, into_logos: dict, into_norm: dict, label: str) -> int:
        added = 0
        for row in csv.reader(io.StringIO(csv_text)):
            # Primary tab uses 2 cols (name, url). Legacy uses 6 cols with
            # paired (name,url) at positions (0,1) and (3,5). We try both
            # patterns on every row — extra positions are no-ops on the
            # primary tab.
            for name_i, url_i in [(0, 1), (3, 5)]:
                if len(row) > url_i:
                    name = (row[name_i] or "").strip()
                    url  = (row[url_i]  or "").strip()
                    if name and url.startswith("http"):
                        into_logos[name] = url
                        nkey = _normalize_team_for_logo(name)
                        if nkey:
                            into_norm[nkey] = url   # latest wins
                        added += 1
        log.info(f"  · {label}: ingested {added} entries")
        return added

    logos: dict = {}
    logos_norm: dict = {}

    for sheet_url, label in [(_LOGOS_SHEET_LEGACY,  "legacy tab"),
                              (_LOGOS_SHEET_PRIMARY, "team_logos_master tab")]:
        try:
            resp = _session.get(sheet_url, timeout=30)
            resp.raise_for_status()
            _ingest(resp.text, logos, logos_norm, label)
        except Exception as e:
            log.warning(f"_load_logos: {label} fetch failed: {e}")

    if not logos:
        log.error("_load_logos: BOTH tabs failed — keeping previous cache")
        return

    _logos_cache      = logos
    _logos_norm_cache = logos_norm
    _logos_ts         = time.time()
    _fuzzy_logo_memo.clear()

    # Persist to /data so other gunicorn workers can pick up the same data
    # on their next request without each having to re-fetch the sheet.
    try:
        import json as _json, tempfile, os as _os
        snap = {"ts": _logos_ts, "logos": logos, "logos_norm": logos_norm}
        # Atomic write: tmp file + rename so concurrent reads never see a
        # half-written JSON.
        d = _os.path.dirname(_LOGOS_DISK_SNAPSHOT)
        _os.makedirs(d, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=d, delete=False) as fh:
            _json.dump(snap, fh)
            tmp_path = fh.name
        _os.replace(tmp_path, _LOGOS_DISK_SNAPSHOT)
        global _logos_disk_mtime_seen
        _logos_disk_mtime_seen = _os.path.getmtime(_LOGOS_DISK_SNAPSHOT)
    except Exception as e:
        log.warning(f"_load_logos: failed to write disk snapshot: {e}")

    log.info(f"Team logos loaded: {len(logos)} entries ({len(logos_norm)} normalized); memo cleared; disk snapshot updated")


def _maybe_reload_from_disk():
    """If a sibling worker wrote a fresher snapshot since we last looked,
    reload our in-memory cache from it. Cheap: only stat() the file."""
    global _logos_cache, _logos_norm_cache, _logos_ts, _logos_disk_mtime_seen
    try:
        import os as _os, json as _json
        if not _os.path.exists(_LOGOS_DISK_SNAPSHOT):
            return False
        mtime = _os.path.getmtime(_LOGOS_DISK_SNAPSHOT)
        if mtime <= _logos_disk_mtime_seen:
            return False
        with open(_LOGOS_DISK_SNAPSHOT, "r") as fh:
            snap = _json.load(fh)
        _logos_cache      = snap.get("logos", {}) or {}
        _logos_norm_cache = snap.get("logos_norm", {}) or {}
        _logos_ts         = float(snap.get("ts") or time.time())
        _logos_disk_mtime_seen = mtime
        _fuzzy_logo_memo.clear()
        log.info(f"_maybe_reload_from_disk: synced from disk snapshot ({len(_logos_cache)} entries)")
        return True
    except Exception as e:
        log.warning(f"_maybe_reload_from_disk failed: {e}")
        return False


def _get_logos():
    # FIRST: pick up any fresher snapshot another worker may have just written.
    # This is the cross-worker sync point — without it, a refresh on worker A
    # leaves worker B serving stale data until its own TTL kicks in.
    _maybe_reload_from_disk()

    # NEVER block a gevent worker — curl_cffi is not gevent-compatible.
    # If cache is empty or stale, kick off a background thread and return
    # whatever we have immediately (empty dict on first boot, stale on refresh).
    if time.time() - _logos_ts > _LOGOS_TTL:
        threading.Thread(target=_load_logos, daemon=True).start()
    return _logos_cache  # may be {} until background thread finishes

# Memoization cache: team name → logo URL (or None). Persisted across requests
# so each unique team is fuzzy-matched only ONCE per server lifetime.
_fuzzy_logo_memo: dict = {}

def _quick_logo(name: str, country: str | None = None) -> str | None:
    """
    Fast logo lookup — exact + normalized only, NO fuzzy fallback.
    Use this in bulk endpoints (state/tips, upcoming, live state) where 700+
    teams may be looked up per request. Fuzzy matching against 10k logos is
    far too slow for that path. Returns None if no exact match.

    `country` (optional) disambiguates homonyms like "Athletic Club" (Spain
    = Bilbao) vs "Athletic Club" (Brazil = Belo Horizonte). The sheet
    should carry rows keyed `"<Team> (<Country>)"` for the disambiguated
    versions; this lookup tries those FIRST when country is provided,
    then falls back to the plain name.
    """
    if not name:
        return None
    # Country-qualified lookup runs first so disambiguated rows win.
    if country:
        qkey = f"{name} ({country})"
        memo_key = f"{qkey}::{country}"
        if memo_key in _fuzzy_logo_memo:
            return _fuzzy_logo_memo[memo_key]
        logos = _get_logos()
        if logos and qkey in logos:
            _fuzzy_logo_memo[memo_key] = logos[qkey]
            return logos[qkey]
        nkey = _normalize_team_for_logo(qkey)
        if nkey in _logos_norm_cache:
            url = _logos_norm_cache[nkey]
            _fuzzy_logo_memo[memo_key] = url
            return url
        # Country-qualified miss — don't cache as None; let the unqualified
        # path below try, then memo the final result.

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


def _backfill_xg_for_finished_games(limit: int = 50, only_recent_days: int = 60) -> dict:
    """For finished games we have NO match_shots persisted, fetch the
    /event/{id}/shotmap from Sofascore and store them. Lets historical
    team pages show 'xG (for-against)' for matches we never observed
    live (e.g. when our region was blocked during their kickoff window).

    Bounded to `only_recent_days` so we don't hammer Sofascore for
    dead-old games no user will look at, and to `limit` per call so a
    single invocation stays fast.
    """
    now_ts  = int(time.time())
    cutoff  = now_ts - only_recent_days * 86400
    fetched, persisted, errors = 0, 0, 0
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT g.id FROM games g "
                "WHERE g.is_finished = 1 AND g.start_ts >= ? "
                "  AND NOT EXISTS (SELECT 1 FROM match_shots ms WHERE ms.match_id = g.id) "
                "ORDER BY g.start_ts DESC LIMIT ?",
                (cutoff, limit)
            ).fetchall()
    except Exception as e:
        return {"ok": False, "error": f"query failed: {e}"}

    for r in rows:
        gid = r["id"]
        try:
            shots = get_shotmap(gid)
        except Exception as e:
            log.debug(f"_backfill_xg: shotmap failed for {gid}: {e}")
            errors += 1
            continue
        # get_shotmap returns dict with homeShots/awayShots even on empty
        if shots and (shots.get("homeShots") or shots.get("awayShots")):
            _persist_shots(gid, shots)
            persisted += 1
        else:
            # Empty/unavailable shotmap (lower divisions, women's, archived
            # data) — write a sentinel row so future backfill calls skip
            # this game instead of retrying it forever. Sentinel has xg=0
            # and minute=-1; readers filter on xg > 0 so it's invisible
            # to chart/stat code.
            try:
                with _db() as conn:
                    conn.execute(
                        "INSERT OR IGNORE INTO match_shots "
                        "(match_id, minute, added_time, is_home, xg, is_goal, is_penalty, "
                        " player, shot_type, situation, body_part, recorded_at) "
                        "VALUES (?, -1, 0, 0, 0, 0, 0, '_no_data', '_no_data', '_no_data', '_no_data', ?)",
                        (gid, now_ts)
                    )
            except Exception as e:
                log.debug(f"_backfill_xg: sentinel insert failed for {gid}: {e}")
        fetched += 1

    log.info(f"_backfill_xg: fetched={fetched} · persisted={persisted} · errors={errors} (limit={limit})")
    return {"ok": True, "fetched": fetched, "persisted": persisted, "errors": errors}


@app.route("/api/admin/backfill-xg", methods=["POST", "GET"])
def r_admin_backfill_xg():
    """Backfill missing xG (match_shots rows) for recently-finished games.
    Useful after the team-page perspective fix — older games whose live
    state we never captured can now show their post-match xG once this
    runs. Rate-limited per call (limit=50) so a manual trigger never
    overloads the Sofascore API."""
    try:
        limit = int(flask_request.args.get("limit", 50))
        days  = int(flask_request.args.get("days",  60))
    except ValueError:
        return jsonify({"ok": False, "error": "limit/days must be ints"}), 400
    result = _backfill_xg_for_finished_games(limit=limit, only_recent_days=days)
    status = 200 if result.get("ok") else 500
    return jsonify(result), status


@app.route("/api/admin/recover-stale-tips", methods=["POST", "GET"])
def r_admin_recover_stale_tips():
    """Manually trigger the stale-game recovery loop. Same logic that runs
    each BG cycle — direct fetch, settle, void after 36h."""
    try:
        _recover_stale_game_results()
        _resolve_finished_tips()
        with _db() as conn:
            still_pending = conn.execute(
                "SELECT COUNT(*) AS n FROM tips WHERE result IS NULL"
            ).fetchone()["n"]
        return jsonify({"ok": True, "still_pending": still_pending})
    except Exception as e:
        log.error(f"r_admin_recover_stale_tips: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/admin/refresh-logos", methods=["POST", "GET"])
def r_refresh_logos():
    """Force-reload the logo sheet NOW (bypass the 10-min TTL) and clear the
    per-team memoization cache so a sheet edit propagates immediately.

    Use this whenever you've changed a row in the team-logos Google Sheet
    and want the new URL live without waiting for the next TTL window.
    """
    before_count = len(_logos_cache)
    _load_logos()  # synchronous so the response reflects the new state
    after_count = len(_logos_cache)
    # If the caller asked about a specific team, return that team's new URL
    name = (flask_request.args.get("team") or "").strip()
    team_url = _quick_logo(name) if name else None
    return jsonify({
        "ok":            True,
        "before_count":  before_count,
        "after_count":   after_count,
        "memo_cleared":  True,
        "team":          name or None,
        "team_url":      team_url,
    })


# ════════════════════════════════════════════════════════════
#  X bot admin endpoints
# ════════════════════════════════════════════════════════════
# Useful during the launch phase to (a) verify credentials work,
# (b) preview tweet body for a real candidate pick without posting,
# (c) inspect recent posts and rate-limit state, and (d) force the
# resolver to run NOW instead of waiting for the next 5-min tick.

@app.route("/api/admin/x/status", methods=["GET"])
def r_admin_x_status():
    """Health check for the X bot: are secrets present, can we init the
    client, what's the recent posting volume, any failed tweets."""
    try:
        client = _x_client()
        with _db() as conn:
            row = conn.execute(
                "SELECT "
                "  COUNT(*) AS total, "
                "  SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS errored, "
                "  SUM(CASE WHEN resolution_tweet_id IS NOT NULL THEN 1 ELSE 0 END) AS resolved, "
                "  SUM(CASE WHEN posted_at >= ? THEN 1 ELSE 0 END) AS last_hour "
                "FROM x_tweets",
                (int(time.time()) - 3600,)
            ).fetchone()
            last5 = conn.execute(
                "SELECT id, match_id, market, label, tweet_id, posted_at, "
                "       resolution_tweet_id, error "
                "FROM x_tweets ORDER BY posted_at DESC LIMIT 5"
            ).fetchall()
        return jsonify({
            "configured":     _x_configured(),
            "enabled":        X_BOT_ENABLED,
            "dry_run":        X_DRY_RUN,
            "client_ok":      client is not None,
            "edge_threshold": X_EDGE_THRESHOLD,
            "caps": {
                "per_hour":              X_MAX_PER_HOUR,
                "per_day":               X_MAX_PER_DAY,
                "per_match":             X_MAX_PER_MATCH,
                "per_market_per_match":  X_MAX_PER_MARKET_PER_MATCH,
                "dedup_window_sec":      X_DEDUP_WINDOW_SEC,
            },
            "usage": {
                "hourly":  _x_hourly_count(),
                "daily":   _x_daily_count(),
            },
            "stats": {
                "total":     row["total"] if row else 0,
                "errored":   row["errored"] if row else 0,
                "resolved":  row["resolved"] if row else 0,
                "last_hour": row["last_hour"] if row else 0,
            },
            "last_5": [dict(r) for r in last5],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/x/preview", methods=["GET"])
def r_admin_x_preview():
    """Preview the tweet body for each hashtag variant on a hypothetical
    pick. No tweet is sent — pure rendering. Query params:
      home, away, tournament, country, market, label, odds, edge, minute
    All optional with sensible defaults so plain GET works."""
    qa = flask_request.args.get
    fake_match = {
        "homeTeam":   qa("home", "Real Madrid"),
        "awayTeam":   qa("away", "Barcelona"),
        "homeGoals":  int(qa("hg", "1")),
        "awayGoals":  int(qa("ag", "0")),
        "tournament": qa("tournament", "La Liga"),
        "country":    qa("country", "Spain"),
        "id":         12345678,
    }
    fake_pick = {
        "market": qa("market", "1X2"),
        "label":  qa("label",  "Barcelona"),
        "odds":   float(qa("odds", "3.40")),
        "edge":   float(qa("edge", "52.5")),
        "model":  float(qa("model", "0.38")),
    }
    minute = int(qa("minute", "37"))
    out = []
    for variant in _X_HASHTAG_VARIANTS:
        body, used = _x_format_pick_tweet(fake_match, fake_pick, minute,
                                           variant_override=variant)
        out.append({
            "variant":        used,
            "body":           body,
            "char_count":     len(body),
            "remaining_280":  280 - len(body),
        })
    return jsonify({
        "variants": out,
        "pool_size": len(_X_HASHTAG_VARIANTS),
        "note": "Each new tweet picks one variant uniformly at random and "
                "the choice is persisted in x_tweets.hashtag_variant for "
                "later attribution analysis.",
    })


@app.route("/api/admin/x/test-tweet", methods=["POST"])
def r_admin_x_test_tweet():
    """Post a hardcoded smoke-test tweet to verify credentials end-to-end.
    POST body: {"text": "..."} optional, defaults to a brand-safe ping.
    Returns the tweet id on success."""
    text = (flask_request.get_json(silent=True) or {}).get(
        "text",
        # No URL on purpose — X charges 13× per post with a link ($0.20 vs $0.015).
        # The profile bio carries the webpronos.com link.
        "🤖 WebPronos AI ping — bot live, scope checks in progress."
    )
    client = _x_client()
    if client is None:
        return jsonify({"error": "x_not_configured_or_disabled",
                         "configured": _x_configured(),
                         "enabled":    X_BOT_ENABLED}), 503
    if X_DRY_RUN:
        return jsonify({"dry_run": True, "would_tweet": text})
    try:
        resp = client.create_tweet(text=text)
        tweet_id = str(resp.data.get("id")) if getattr(resp, "data", None) else None
        return jsonify({"ok": True, "tweet_id": tweet_id, "text": text})
    except Exception as e:
        return jsonify({"error": str(e)[:500]}), 500


@app.route("/api/admin/x/resolve-now", methods=["POST", "GET"])
def r_admin_x_resolve_now():
    """Force the resolution worker to run immediately (instead of waiting
    for the next 5-min APScheduler tick). Returns how many replies it posted."""
    posted = _x_resolve_settled_tips()
    return jsonify({"ok": True, "resolutions_posted": posted})


@app.route("/api/admin/tips/archive-now", methods=["POST", "GET"])
def r_admin_tips_archive_now():
    """Force the hourly tips→Supabase archive to run immediately.
    Returns row count + status. Idempotent — safe to call repeatedly."""
    out = _archive_tips_to_supabase()
    return jsonify(out)


@app.route("/api/admin/tips/restore-now", methods=["POST"])
def r_admin_tips_restore_now():
    """Emergency: pull tips_archive from Supabase into local SQLite.
    Body: {"force": true} to restore even if local has data.
    Only POST (not GET) since this mutates state."""
    body = flask_request.get_json(silent=True) or {}
    force = bool(body.get("force", False))
    out = _restore_tips_from_supabase(force=force)
    return jsonify(out)


@app.route("/api/admin/tips/archive-status", methods=["GET"])
def r_admin_tips_archive_status():
    """Quick health check: how many tips in local vs in Supabase archive."""
    out = {}
    try:
        with _db() as conn:
            out["local"] = {
                "tips":  conn.execute("SELECT COUNT(*) FROM tips").fetchone()[0],
                "games": conn.execute("SELECT COUNT(*) FROM games").fetchone()[0],
            }
    except Exception as e:
        out["local"] = {"error": str(e)[:200]}
    try:
        import urllib.request as _ur
        req = _ur.Request(
            _supabase_archive_url("tips_archive") + "?select=count",
            headers={**_supabase_archive_headers(), "Prefer": "count=exact"},
        )
        with _ur.urlopen(req, timeout=10) as r:
            arr = json.loads(r.read())
            out["supabase"] = {"tips": arr[0]["count"] if arr else 0}
    except Exception as e:
        out["supabase"] = {"error": str(e)[:200]}
    return jsonify(out)


@app.route("/api/admin/x/analyze-thresholds", methods=["GET"])
def r_admin_x_analyze_thresholds():
    """Historical analysis of how many picks/day would pass at each edge
    threshold. Used to calibrate X_EDGE_THRESHOLD for a target volume.

    Query params:
      days   — lookback window (default 30)
      target — desired picks/day (default 6)

    Returns: per-threshold avg/max/min picks/day, plus a recommended
    threshold that hits the target window.
    """
    from datetime import datetime, timezone
    days = int(flask_request.args.get("days", "30"))
    target = float(flask_request.args.get("target", "6"))
    cutoff_ts = int(time.time()) - days * 86400
    thresholds = [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 15.0, 18.0, 20.0]

    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT match_id, market, label, edge_entry AS edge, wall_ts FROM tips "
                "WHERE wall_ts >= ? AND edge_entry IS NOT NULL "
                "ORDER BY wall_ts ASC",
                (cutoff_ts,),
            ).fetchall()

        # Bucket per day (YYYY-MM-DD UTC)
        days_with_data = {}
        for r in rows:
            day = datetime.fromtimestamp(r["wall_ts"], tz=timezone.utc).strftime("%Y-%m-%d")
            days_with_data.setdefault(day, []).append(r)
        total_days = max(len(days_with_data), 1)

        results = []
        for thr in thresholds:
            counts_per_day = []
            picks_per_match_per_day = []
            picks_per_market_per_match = []
            for day, day_rows in days_with_data.items():
                passing = [r for r in day_rows if (r["edge"] or 0) >= thr]
                counts_per_day.append(len(passing))
                # per match
                by_match = {}
                for r in passing:
                    by_match.setdefault(r["match_id"], []).append(r)
                if by_match:
                    picks_per_match_per_day.append(max(len(v) for v in by_match.values()))
                # per market per match
                for m, m_rows in by_match.items():
                    by_market = {}
                    for r in m_rows:
                        by_market.setdefault(r["market"] or "", []).append(r)
                    if by_market:
                        picks_per_market_per_match.append(max(len(v) for v in by_market.values()))
            avg = sum(counts_per_day) / len(counts_per_day) if counts_per_day else 0
            results.append({
                "threshold":            thr,
                "avg_picks_per_day":    round(avg, 1),
                "max_picks_per_day":    max(counts_per_day) if counts_per_day else 0,
                "min_picks_per_day":    min(counts_per_day) if counts_per_day else 0,
                "max_picks_per_match":  max(picks_per_match_per_day) if picks_per_match_per_day else 0,
                "max_picks_per_market_per_match": max(picks_per_market_per_match) if picks_per_market_per_match else 0,
                "total_picks_in_window": sum(counts_per_day),
            })

        # Pick recommended threshold: closest avg to target without exceeding
        recommended = None
        for r in results:
            if r["avg_picks_per_day"] <= target + 1:  # small wiggle
                recommended = r["threshold"]
                break
        return jsonify({
            "window_days":   days,
            "total_days":    total_days,
            "total_tips":    len(rows),
            "target_per_day": target,
            "recommended_threshold": recommended,
            "by_threshold":  results,
        })
    except Exception as e:
        return jsonify({"error": str(e)[:500]}), 500


@app.route("/api/admin/x/debug-auth", methods=["GET"])
def r_admin_x_debug_auth():
    """Diagnostic endpoint — tries raw OAuth1 against X API and returns the
    full Twitter error response (tweepy hides details).
    Useful to debug Read-only vs Read+Write permission issues."""
    out = {
        "key_prefixes": {
            "api_key":             (X_API_KEY[:6] + "...") if X_API_KEY else None,
            "access_token_userid": X_ACCESS_TOKEN.split("-")[0] if "-" in X_ACCESS_TOKEN else None,
            "access_token_tail":   X_ACCESS_TOKEN[-8:] if X_ACCESS_TOKEN else None,
        },
    }
    try:
        from requests_oauthlib import OAuth1
        import requests as _req
        auth = OAuth1(
            X_API_KEY, X_API_SECRET,
            X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET,
        )
        # 1. Try GET /2/users/me — pure read endpoint
        r1 = _req.get("https://api.x.com/2/users/me", auth=auth, timeout=10)
        out["users_me"] = {
            "status": r1.status_code,
            "body":   (r1.text or "")[:500],
        }
        # 2. Try POST /2/tweets — write endpoint (we'll catch 401 cleanly)
        r2 = _req.post(
            "https://api.x.com/2/tweets",
            auth=auth,
            json={"text": "diagnostic_ping_will_be_deleted"},
            timeout=10,
        )
        out["create_tweet"] = {
            "status": r2.status_code,
            "body":   (r2.text or "")[:500],
        }
        # If the tweet went through, immediately delete it
        if r2.status_code == 201:
            tweet_id = r2.json().get("data", {}).get("id")
            if tweet_id:
                rdel = _req.delete(
                    f"https://api.x.com/2/tweets/{tweet_id}",
                    auth=auth, timeout=10,
                )
                out["cleanup_delete"] = {"status": rdel.status_code, "tweet_id": tweet_id}
    except Exception as e:
        out["error"] = str(e)[:500]
    return jsonify(out)


@app.route("/api/admin/logos/missing-teams", methods=["GET"])
def r_logos_missing_teams():
    """Lists every team in our DB + upcoming cache + live state for which the
    logo cache currently has NO entry. Use this to know exactly which rows
    you need to fill in the team_logos_master sheet tab.

    Query params:
      ?format=tsv  → returns tab-separated text ready to paste into Sheets
                     (columns: team_name, logo_url, total_games, competitions)
      (default JSON)
    """
    from collections import Counter
    seen = Counter()
    tournaments_for = {}

    # 1. Games table
    try:
        with _db() as conn:
            for r in conn.execute(
                "SELECT home_team AS team, tournament FROM games WHERE home_team IS NOT NULL "
                "UNION ALL "
                "SELECT away_team AS team, tournament FROM games WHERE away_team IS NOT NULL"
            ):
                t = (r["team"] or "").strip()
                if not t: continue
                seen[t] += 1
                tournaments_for.setdefault(t, set()).add(r["tournament"] or "")
    except Exception as e:
        log.warning(f"missing-teams: games query failed: {e}")

    # 2. Upcoming cache + live state
    try:
        for cached in (_upcoming_cache or {}).values():
            for m in (cached or {}).get("matches", []):
                for k in ("homeTeam", "awayTeam"):
                    t = (m.get(k) or "").strip()
                    if t and t not in seen:
                        seen[t] = 1
                        tournaments_for.setdefault(t, set()).add(m.get("tournament", ""))
        with _state_lock:
            for entry in _live_state.values():
                m = entry.get("match", {})
                for k in ("homeTeam", "awayTeam"):
                    t = (m.get(k) or "").strip()
                    if t and t not in seen:
                        seen[t] = 1
                        tournaments_for.setdefault(t, set()).add(m.get("tournament", ""))
    except Exception:
        pass

    # 3. Cross-reference against the merged logo cache
    have = set(_logos_cache.keys())
    have_norm = set(_logos_norm_cache.keys())
    missing = []
    for team, n in seen.most_common():
        if team in have:
            continue
        if _normalize_team_for_logo(team) in have_norm:
            continue
        missing.append({
            "team_name":     team,
            "total_games":   n,
            "competitions":  " · ".join(sorted(c for c in tournaments_for.get(team, set()) if c))[:120],
        })

    fmt = (flask_request.args.get("format") or "json").strip().lower()
    if fmt == "tsv":
        lines = ["team_name\tlogo_url\ttotal_games\tcompetitions"]
        for m in missing:
            lines.append(f"{m['team_name']}\t\t{m['total_games']}\t{m['competitions']}")
        body = "\n".join(lines) + "\n"
        return Response(body, mimetype="text/tab-separated-values",
                        headers={"Content-Disposition":
                                 f"inline; filename=missing_team_logos.tsv"})

    return jsonify({
        "total_teams_in_system": len(seen),
        "teams_with_logo":       len(seen) - len(missing),
        "missing":               missing,
    })


# ─── BetRadarAI match recap (animated GIF + caption) ────────────────────────
BETRADAR_BOT_USERNAME = "BetRadarAI_bot"
BETRADAR_BOT_LINK     = f"https://t.me/{BETRADAR_BOT_USERNAME}"


# Map of common English pick labels to pt-pt, applied to every user-facing
# message (daily summary, match recap caption, daily recap caption, etc.).
# Team-name labels (e.g. "Brazil -1.0") are pass-through — team names are
# already what we want to show.
_PICK_LABEL_PT = {
    "Draw":           "Empate",
    "Yes":            "Sim",
    "No":             "Não",
    "Both teams":     "Ambas Marcam",
    "Both Teams":     "Ambas Marcam",
    "Home":           "Casa",
    "Away":           "Fora",
}


def _localize_pick_label(label: str) -> str:
    """Translate a tip label to pt-pt for display. Falls through for labels
    that are team names or already localised (e.g. 'Mais de 2.5')."""
    if not label:
        return label
    s = label.strip()
    if s in _PICK_LABEL_PT:
        return _PICK_LABEL_PT[s]
    if s.startswith("Over "):   return "Mais de "  + s[5:]
    if s.startswith("Under "):  return "Menos de " + s[6:]
    return label


def _betradar_match_caption(match_id: int) -> str:
    """Build a Portuguese-language summary caption shown above the GIF in
    the BetRadarAI bot. Includes country flag emoji of the competition,
    win/loss counts with green accents, and a share CTA at the bottom."""
    with _db() as conn:
        g = conn.execute(
            "SELECT home_team, away_team, home_goals, away_goals, tournament, country "
            "FROM games WHERE id = ?", (match_id,)
        ).fetchone()
        tips = conn.execute(
            "SELECT market, label, odd_entry, edge_entry, minute_entry, result "
            "FROM tips WHERE match_id = ? "
            "AND result IN ('green','red','win','loss') "
            "ORDER BY minute_entry", (match_id,)
        ).fetchall()
    if not g or not tips:
        return ""
    wins   = sum(1 for t in tips if t["result"] in ("green","win"))
    losses = len(tips) - wins
    profit = 0.0
    for t in tips:
        if t["result"] in ("green","win"):
            profit += float(t["odd_entry"] or 0) - 1.0
        else:
            profit -= 1.0
    eur = profit * 100  # 1u stake = €100 convention
    sign = "+" if profit > 0 else ""
    flag = _country_flag(g["country"] or "") or "🌍"

    return (
        f"{flag} <b>{g['tournament']}</b>\n"
        f"⚽ <b>{g['home_team']} {g['home_goals']}–{g['away_goals']} {g['away_team']}</b>\n"
        f"\n"
        f"🤖 O algoritmo lançou <b>{len(tips)} picks</b>:\n"
        f"  🟢 <b>{wins}</b> ganhas\n"
        f"  🔴 <b>{losses}</b> perdidas\n"
        f"\n"
        f"💰 Lucro: <b>{sign}{profit:.2f}u  (~€{eur:+.0f})</b> ✅\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📲 Recebe estas picks em tempo real:\n"
        f"👉 @{BETRADAR_BOT_USERNAME}\n"
        f"🔗 {BETRADAR_BOT_LINK}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )


def _betradar_daily_caption(target_start_ts: int, target_end_ts: int,
                             date_label: str, day_label: str = "Hoje") -> str:
    """Mirror of the existing _send_daily_summary text, returned as caption
    HTML so we can attach it to the animated daily-recap MP4."""
    STAKE = get_setting("stake_per_bet", 100.0) or 100.0
    with _db() as conn:
        tips = conn.execute(
            "SELECT t.result, t.odd_entry, t.label, t.market, "
            "       g.home_team, g.away_team "
            "FROM tips t LEFT JOIN games g ON g.id = t.match_id "
            "WHERE t.wall_ts >= ? AND t.wall_ts < ? AND t.result IS NOT NULL "
            "ORDER BY t.odd_entry DESC",
            (target_start_ts, target_end_ts)
        ).fetchall()
    if not tips:
        return ""
    lucro, odds_sum, wins, losses = 0.0, 0.0, 0, 0
    for t in tips:
        o = float(t["odd_entry"] or 0)
        if (t["result"] or "").lower() in ("green", "win"):
            lucro += (o - 1) * STAKE; wins += 1; odds_sum += o
        elif (t["result"] or "").lower() in ("red", "loss"):
            lucro -= STAKE; losses += 1; odds_sum += o
    settled = wins + losses
    avg_odds = odds_sum / settled if settled else 0.0
    roi = (lucro / (settled * STAKE) * 100) if settled else 0.0

    winning = [t for t in tips if (t["result"] or "").lower() in ("green","win")]
    biggest = max(winning, key=lambda t: t["odd_entry"] or 0) if winning else None
    big_block = ""
    if biggest:
        bw_odd    = biggest["odd_entry"] or 0
        bw_label  = _localize_pick_label(biggest["label"] or "?")
        bw_market = biggest["market"] or "?"
        bw_match  = f"{biggest['home_team']} vs {biggest['away_team']}" \
                    if biggest["home_team"] and biggest["away_team"] else "—"
        bw_profit = (bw_odd - 1) * STAKE
        big_block = (
            f"\n🎯 <b>Maior Odd do Dia:</b> {bw_odd:.2f}\n"
            f"   <i>{bw_label} ({bw_market})</i>\n"
            f"   <i>{bw_match}</i>\n"
            f"   💰 Lucro gerado: <b>+€{bw_profit:.2f}</b>"
        )

    return (
        f"<b>Resumo Diário — {day_label} ({date_label})</b>\n"
        f"\n"
        f"💶 <b>Lucro:</b> €{lucro:,.2f}\n"
        f"📊 <b>Odds Médias:</b> {avg_odds:.2f}\n"
        f"📈 <b>ROI:</b> {roi:.1f}%"
        f"{big_block}\n"
        f"\n"
        f"<i>Mantém a vigilância nas entradas de amanhã — o edge está lá! 🚀</i>"
    )


@app.route("/api/admin/betradar/monthly-recap", methods=["POST", "GET"])
def r_betradar_monthly_recap():
    """Manually trigger the monthly recap broadcast.

    Use this when:
      - You want to test the monthly recap outside the 23:55 cron window.
      - The cron fired but the build/broadcast failed (e.g. ffmpeg blip)
        and you need to retry the day.
      - You want to send an interim monthly status mid-month.

    Query params:
      force=1   (default 1) — bypass the per-day dedup lock and the €25
                month-profit threshold. Always broadcasts to subscribers
                as long as there are settled tips for the month so far.
      force=0   — honour the lock + threshold. Won't re-send if today's
                slot has already been claimed, and skips if month
                profit ≤ €25.

    Returns a small JSON status. Errors during build/broadcast are
    captured in logs (and visible via `flyctl logs`).
    """
    force_raw = (flask_request.args.get("force") or "1").strip().lower()
    force = force_raw not in ("0", "false", "no", "")
    try:
        _send_monthly_summary_if_profitable(force_send=force)
        return jsonify({"ok": True, "force": force, "note": "Check flyctl logs + Telegram for delivery confirmation."})
    except Exception as e:
        log.exception("monthly-recap admin trigger failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/admin/betradar/daily-recap", methods=["POST", "GET"])
def r_betradar_daily_recap():
    """Generate the day's cumulative-P&L animation + send to a chat_id.

    Query params:
      date     — YYYY-MM-DD (Lisbon timezone), defaults to today
      chat_id  — target Telegram chat (defaults to first admin)
    """
    from datetime import datetime, timedelta
    import pytz

    date_str = (flask_request.args.get("date") or "").strip()
    lisbon   = pytz.timezone("Europe/Lisbon")
    if date_str:
        try:
            y, m, d = map(int, date_str.split("-"))
        except Exception:
            return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
    else:
        now = datetime.now(lisbon)
        y, m, d = now.year, now.month, now.day

    start = lisbon.localize(datetime(y, m, d))
    end   = start + timedelta(days=1)
    target_start_ts = int(start.timestamp())
    target_end_ts   = int(end.timestamp())
    date_label = start.strftime("%d/%m/%Y")

    chat_id_raw = (flask_request.args.get("chat_id") or "").strip()
    if chat_id_raw.lstrip("-").isdigit():
        chat_id = int(chat_id_raw)
    elif TELEGRAM_ADMIN_CHAT_IDS:
        chat_id = next(iter(TELEGRAM_ADMIN_CHAT_IDS))
    else:
        return jsonify({"ok": False, "error": "no chat_id and no admin chat configured"}), 400

    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from tools.build_daily_recap import build_daily_recap  # type: ignore
    except Exception as e:
        return jsonify({"ok": False, "error": f"recap import failed: {e}"}), 500

    requested_path = f"/tmp/daily_recap_{date_str or 'today'}.mp4"
    actual_path = requested_path
    try:
        result = build_daily_recap(
            target_start_ts=target_start_ts,
            target_end_ts=target_end_ts,
            date_label=date_label,
            out_path=requested_path,
            db_path=str(DB_PATH),
        )
        actual_path = result.split(" (", 1)[0] if isinstance(result, str) else requested_path
        anim_bytes = open(actual_path, "rb").read()
    except Exception as e:
        log.error(f"betradar daily recap build failed for {date_str}: {e}")
        return jsonify({"ok": False, "error": f"build failed: {e}"}), 500

    caption  = _betradar_daily_caption(target_start_ts, target_end_ts, date_label)
    filename = os.path.basename(actual_path)
    _send_telegram_animation(chat_id, anim_bytes, caption=caption,
                              buttons=_betradar_share_buttons(),
                              filename=filename)

    return jsonify({
        "ok":       True,
        "date":     date_label,
        "format":   filename.rsplit(".", 1)[-1],
        "size":     len(anim_bytes),
        "chat_id":  chat_id,
        "caption_chars": len(caption),
    })


def _betradar_share_buttons() -> list:
    """Inline keyboard: single 'Partilhar Resultados' button that opens
    Telegram's native chat-picker pre-populated with the share message via
    switch_inline_query. Lets the user forward this recap to any contact
    or group in two taps."""
    return [
        [
            {"text": "🔗 Partilhar Resultados",
             "switch_inline_query": (
                 "🟢 Picks ao vivo de futebol no @"
                 f"{BETRADAR_BOT_USERNAME} — junta-te grátis"
             )},
        ],
    ]


@app.route("/api/admin/betradar/recap/<int:match_id>", methods=["POST", "GET"])
def r_betradar_recap(match_id: int):
    """Generate the GIF recap for a match and send to a chat_id (defaults to
    the admin). Used both for manual testing and as the production fan-out
    handler when a match's settled profit crosses the threshold.

    Query params:
      chat_id  — target Telegram chat (defaults to first TELEGRAM_ADMIN_CHAT_IDS)
    """
    # Resolve target chat
    chat_id_raw = (flask_request.args.get("chat_id") or "").strip()
    if chat_id_raw.lstrip("-").isdigit():
        chat_id = int(chat_id_raw)
    elif TELEGRAM_ADMIN_CHAT_IDS:
        chat_id = next(iter(TELEGRAM_ADMIN_CHAT_IDS))
    else:
        return jsonify({"ok": False, "error": "no chat_id and no TELEGRAM_ADMIN_CHAT_IDS"}), 400

    # Lazy-import the recap builder (matplotlib pulls a lot of memory; only
    # imported when this endpoint actually runs).
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from tools.build_match_recap import build_recap  # type: ignore
    except Exception as e:
        return jsonify({"ok": False, "error": f"recap import failed: {e}"}), 500

    # Fetch the two team crests using OUR curl_cffi session (TLS-impersonated
    # — gets past Sofascore CDN's anti-bot which plain urllib doesn't). This
    # is the same path that scrapes match data, so if scraping works, logos
    # work. Falls back gracefully (coloured circle) when something fails.
    def _fetch_logo_img(team_id):
        if not team_id or not _session:
            return None
        try:
            url = f"https://api.sofascore.app/api/v1/team/{team_id}/image"
            resp = _session.get(url, timeout=6)
            if resp.status_code != 200 or not resp.content:
                return None
            from PIL import Image as _PIL
            import io as _io
            img = _PIL.open(_io.BytesIO(resp.content)).convert("RGBA")
            img.thumbnail((192, 192), _PIL.LANCZOS)
            return img
        except Exception as e:
            log.warning(f"recap logo fetch failed team_id={team_id}: {e}")
            return None

    with _db() as conn:
        team_ids = conn.execute(
            "SELECT home_team_id, away_team_id FROM games WHERE id = ?",
            (match_id,)
        ).fetchone()
    logo_home_img = _fetch_logo_img(team_ids["home_team_id"]) if team_ids else None
    logo_away_img = _fetch_logo_img(team_ids["away_team_id"]) if team_ids else None

    # Generate the recap. Prefer MP4 (H.264 via ffmpeg) — falls back to GIF
    # only if ffmpeg isn't installed. build_recap may rewrite the extension
    # in that fallback case, so we read whatever it actually produced.
    requested_path = f"/tmp/betradar_recap_{match_id}.mp4"
    actual_path = requested_path
    try:
        result = build_recap(match_id, requested_path, db_path=str(DB_PATH),
                              home_logo_img=logo_home_img,
                              away_logo_img=logo_away_img)
        # build_recap returns "path (size · frames · duration)" — extract path
        actual_path = result.split(" (", 1)[0] if isinstance(result, str) else requested_path
        anim_bytes = open(actual_path, "rb").read()
    except Exception as e:
        log.error(f"betradar recap build failed for match {match_id}: {e}")
        return jsonify({"ok": False, "error": f"build failed: {e}"}), 500

    caption  = _betradar_match_caption(match_id)
    filename = os.path.basename(actual_path)  # recap.mp4 or recap.gif
    _send_telegram_animation(chat_id, anim_bytes, caption=caption,
                              buttons=_betradar_share_buttons(),
                              filename=filename)

    return jsonify({
        "ok":         True,
        "match_id":   match_id,
        "format":     filename.rsplit(".", 1)[-1],
        "size":       len(anim_bytes),
        "chat_id":    chat_id,
        "caption_chars": len(caption),
    })



@app.route("/api/team_logos")
def r_team_logos():
    return jsonify({"teams": _get_logos(), "count": len(_logos_cache)})

@app.route("/api/team_logo/<path:name>")
def r_team_logo_lookup(name: str):
    """
    Fuzzy logo lookup for a single team name.
    GET /api/team_logo/Manchester%20Utd
    → {"name": "Manchester Utd", "url": "https://...", "matched": true}

    Optional ?country=Spain disambiguates homonyms like Athletic Club
    (Spain = Bilbao) vs Athletic Club (Brazil). When provided we try the
    country-qualified key first, then fall back to the plain name.
    """
    country = (flask_request.args.get("country") or "").strip() or None
    # Country-qualified exact-match path first (fast lane).
    if country:
        q = _quick_logo(name, country)
        if q:
            return jsonify({"name": name, "url": q, "matched": True, "country": country})
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

    Each team may be passed as a string OR as {"name": "...", "country": "..."}.
    The dict form goes through the country-qualified fast lane so homonyms
    resolve to the right crest. String form keeps backwards compatibility.
    """
    data  = flask_request.get_json(force=True, silent=True) or {}
    names = data.get("teams", [])
    results = {}
    for item in names[:200]:   # cap at 200 per call
        if isinstance(item, dict):
            n = (item.get("name") or "").strip()
            c = (item.get("country") or "").strip() or None
            if not n:
                continue
            results[n] = _quick_logo(n, c) or _fuzzy_logo(n)
        else:
            n = str(item)
            results[n] = _fuzzy_logo(n)
    return jsonify({"results": results, "resolved": sum(1 for v in results.values() if v)})

@app.route("/api/team_logos/refresh", methods=["POST", "GET"])
def r_team_logos_refresh():
    """Force immediate reload of team logos from Google Sheets."""
    global _logos_ts
    _logos_ts = 0.0  # Expire cache immediately
    _load_logos()
    return jsonify({"ok": True, "count": len(_logos_cache), "message": f"Loaded {len(_logos_cache)} logos from Google Sheets"})


# ═══════════════════════════════════════════════════════════════════════════
#  Affiliate redirect — "Find best odds" smart loader
# ═══════════════════════════════════════════════════════════════════════════
# When the user clicks a "Bet now" CTA on webpronos.com, the SPA links here
# instead of going straight to a bookmaker. We render a 2-3 second loader
# that animates a comparison across 4 random competitor books — Betlabel
# always wins the comparison by a small margin — then auto-redirects to
# the Betlabel affiliate URL. The comparison is presentational only; the
# competitor odds are generated, not fetched.
#
# Rotation: competitors are deterministic per match_id (so reloading the
# same fixture shows the same 4 books — looks legitimate), but vary across
# matches so the user doesn't see the same 4 names everywhere.
#
# Every render writes a row to affiliate_clicks for CTR analytics.

# Per-affiliate config: which competitors to compare against and where
# to land the user. Adding a new affiliate = one entry here; the rest of
# the route is fully driven by this dict.
#
# - web:      international books shown on webpronos.com home/match pages
#             → redirects to Betlabel
# - leon:     PT-friendly books shown in the BetRadar Telegram bot
#             → redirects to LEON
# - twin:     same PT pool, alternating with leon for telegram CTAs
#             → redirects to TWIN
# Welcome-bonus offers shown under the "Open now" button after the winner
# row reveals. Rotated per match_id so the same fixture always shows the
# same offer (reload-safe) but different matches vary. Copy is English for
# now per the boss; localise later via the same _GO_BET_COPY pattern.
_AFFILIATE_OFFERS_DEFAULT = [
    {"icon": "🎁", "title": "Free Bet",
     "desc": "15% free bet up to €100 on first deposit"},
    {"icon": "⚽", "title": "Sports Bonus",
     "desc": "First deposit bonus up to €100"},
]

_AFFILIATE_CONFIGS = {
    "betlabel": {
        "destination_url": (
            "http://welcome.toptrendyinc.com/redirect.aspx"
            "?pid=138989&lpid=1453&bid=1651"
        ),
        "winner_display": "Betlabel",
        # Recognizable international names — wide pool so rotation feels fresh.
        "competitor_pool": [
            "Bet365", "Pinnacle", "Bwin", "William Hill", "Ladbrokes",
            "Unibet", "888sport", "Marathonbet", "Betway", "Paddy Power",
            "Sportingbet", "22Bet",
        ],
        "welcome_offers": _AFFILIATE_OFFERS_DEFAULT,
    },
    "leon": {
        "destination_url": (
            "https://dashboard.onetwoaffiliates.com/click?"
            "campaign_id=797&ref_id=370&path=https%3A%2F%2Fleon317.casino%2Flive"
        ),
        "winner_display": "LEON",
        # PT-friendly books familiar to BetRadar AI subscribers.
        "competitor_pool": ["LeBull", "Betclic", "22Bet", "Betano", "Bwin"],
        "welcome_offers": _AFFILIATE_OFFERS_DEFAULT,
    },
    "twin": {
        "destination_url": (
            "https://dashboard.onetwoaffiliates.com/click?"
            "campaign_id=796&ref_id=370&path=https%3A%2F%2Ftwin191.com%2Flive"
        ),
        "winner_display": "TWIN",
        "competitor_pool": ["LeBull", "Betclic", "22Bet", "Betano", "Bwin"],
        "welcome_offers": _AFFILIATE_OFFERS_DEFAULT,
    },
}

# Backwards-compat name used by /api/admin/affiliate/stats — points at
# the web flow's destination.
_BETLABEL_AFFILIATE_URL = _AFFILIATE_CONFIGS["betlabel"]["destination_url"]

_GO_BET_COPY = {
    "en":    {"title": "Finding the best odds…", "comparing": "Comparing {n} bookmakers",
              "best":  "BEST PRICE", "opening": "Opening Betlabel in",
              "open_now": "Open now", "vary": "Odds may vary at the bookmaker · 18+",
              "checked": "verified seconds ago"},
    "pt-pt": {"title": "À procura das melhores odds…", "comparing": "A comparar {n} casas",
              "best":  "MELHOR PREÇO", "opening": "A abrir Betlabel em",
              "open_now": "Abrir agora", "vary": "Odds podem variar na casa · 18+",
              "checked": "verificado há segundos"},
    "pt-br": {"title": "Procurando as melhores odds…", "comparing": "Comparando {n} casas",
              "best":  "MELHOR PREÇO", "opening": "Abrindo Betlabel em",
              "open_now": "Abrir agora", "vary": "Odds podem variar na casa · 18+",
              "checked": "verificado há segundos"},
    "es":    {"title": "Buscando las mejores cuotas…", "comparing": "Comparando {n} casas",
              "best":  "MEJOR PRECIO", "opening": "Abriendo Betlabel en",
              "open_now": "Abrir ahora", "vary": "Las cuotas pueden variar · 18+",
              "checked": "verificado hace segundos"},
}


def _affiliate_pick_competitors(match_id: int, k: int = 4,
                                  pool: list[str] | None = None) -> list[str]:
    """Deterministic per match_id: same fixture always shows the same
    competitors (reload-safe), different fixtures vary. The pool argument
    lets each affiliate flow (web vs telegram) advertise a different set
    of competitors."""
    import random as _r
    seed = match_id or 0
    rng = _r.Random(seed * 2654435761 & 0xFFFFFFFF)
    pool_local = list(pool) if pool is not None else list(
        _AFFILIATE_CONFIGS["betlabel"]["competitor_pool"]
    )
    rng.shuffle(pool_local)
    # Pool may be smaller than k (PT pool only has 5 entries) — clamp.
    return pool_local[: min(k, len(pool_local))]


def _affiliate_generate_competitor_odds(betlabel_odd: float,
                                          competitors: list[str],
                                          match_id: int) -> list[tuple[str, float]]:
    """Each competitor gets an odd 0.03–0.12 BELOW the Betlabel price.
    Deterministic per (match_id, name) so reloads stay consistent.
    Result is sorted descending by odd (so the visual list builds
    naturally up to the winning Betlabel row)."""
    import random as _r
    out: list[tuple[str, float]] = []
    for i, name in enumerate(competitors):
        # Python 3.12's Random() rejects tuple seeds — use a stable string
        # so each (match_id, name, i) combo still maps to a unique RNG.
        rng = _r.Random(f"{match_id or 0}|{name}|{i}")
        # Bigger fixtures (round Betlabel odd) get tighter comp spread;
        # underdogs (>3.0) can have wider variance — feels realistic.
        max_gap = 0.07 if betlabel_odd < 2.0 else (0.12 if betlabel_odd < 3.5 else 0.20)
        gap = round(rng.uniform(0.03, max_gap), 2)
        comp = max(1.01, round(betlabel_odd - gap, 2))
        out.append((name, comp))
    out.sort(key=lambda x: x[1])   # ascending → Betlabel revealed last as best
    return out


@app.route("/go/bet")
def r_go_bet():
    """Smart-loader interstitial → auto-redirect to Betlabel affiliate URL.

    Query params (all optional except `odd`):
      match_id   — int, used as seed for competitor selection + tracking
      market     — display label, e.g. "Over 2.5"
      label      — display label, e.g. "Over"
      odd        — Betlabel's displayed odd (required for the comparison)
      lang       — en|pt-pt|pt-br|es (default en)
      source     — match-page|live-list|recap|… for analytics
      delay      — redirect delay in ms (default 2500, min 1500, max 5000)
    """
    import random as _r
    try:
        odd_raw   = (flask_request.args.get("odd") or "").strip()
        try:
            odd = float(odd_raw)
            if not (1.01 <= odd <= 50.0):
                raise ValueError("out of range")
        except Exception:
            # If odd missing/invalid, fall back to a neutral 1.85 so the
            # page still renders rather than 500-ing on a malformed CTA.
            odd = 1.85

        match_id  = int(flask_request.args.get("match_id") or 0)
        market    = (flask_request.args.get("market") or "").strip()[:60]
        label     = (flask_request.args.get("label") or "").strip()[:60]

        # Locale resolution — defence in depth: explicit `lang` query wins;
        # if missing, infer from Referer URL path (eg /br/jogo/X → pt-br).
        # This way the popup ALWAYS matches the surrounding page even if
        # the SPA forgets to append &lang=...
        lang_raw  = (flask_request.args.get("lang") or "").strip().lower()
        if not lang_raw:
            try:
                from urllib.parse import urlparse as _up
                ref_path = _up(flask_request.headers.get("Referer") or "").path or ""
                if ref_path.startswith("/br/") or ref_path == "/br":
                    lang_raw = "pt-br"
                elif ref_path.startswith("/pt/") or ref_path == "/pt":
                    lang_raw = "pt-pt"
                elif ref_path.startswith("/es/") or ref_path == "/es":
                    lang_raw = "es"
            except Exception:
                pass
        lang = lang_raw if lang_raw in _GO_BET_COPY else "en"
        source    = (flask_request.args.get("source") or "").strip()[:40]
        try:
            delay_ms = int(flask_request.args.get("delay") or 4000)
        except ValueError:
            delay_ms = 4000
        delay_ms = max(1500, min(6000, delay_ms))

        # Which affiliate flow is the user on. `betlabel` = webpronos.com
        # (international books), `leon` / `twin` = BetRadar AI Telegram
        # (PT-friendly books). Unknown values silently fall back to web.
        aff_key = (flask_request.args.get("aff") or "betlabel").strip().lower()
        if aff_key not in _AFFILIATE_CONFIGS:
            aff_key = "betlabel"
        cfg = _AFFILIATE_CONFIGS[aff_key]

        # Embed mode: strip the full-page chrome and emit postMessage to
        # the parent window instead of doing a same-tab redirect. SPA
        # wraps this in a modal + opens the bookmaker in window.open()
        # on receipt of the message.
        embed = (flask_request.args.get("embed") or "").strip().lower() in ("1", "true", "yes")

        competitors  = _affiliate_pick_competitors(
            match_id, k=4, pool=cfg["competitor_pool"]
        )
        comp_pairs   = _affiliate_generate_competitor_odds(odd, competitors, match_id)

        # Pick ONE welcome offer for this render, deterministic per
        # match_id so reloads stay consistent. Empty list → render nothing.
        offers = cfg.get("welcome_offers") or []
        offer = None
        if offers:
            import random as _r
            rng = _r.Random(f"offer|{match_id or 0}")
            offer = rng.choice(offers)

        # Tracking — fire-and-forget; never block the redirect on DB.
        try:
            ua = (flask_request.headers.get("User-Agent") or "")[:200]
            country = (flask_request.headers.get("CF-IPCountry")
                       or flask_request.headers.get("X-Country") or "")[:8]
            with _db() as conn:
                conn.execute(
                    "INSERT INTO affiliate_clicks "
                    "(ts, bookmaker, match_id, market, label, odd, lang, source, ip_country, user_agent) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (int(time.time()), aff_key, match_id, market, label,
                     odd, lang, source, country, ua)
                )
        except Exception as e:
            log.warning(f"affiliate_clicks insert failed: {e}")

        # Build the affiliate URL — append clickid for future postback
        # tracking. Affiliate platforms ignore unknown params so this is
        # safe whether or not postbacks are configured.
        sep = "&" if "?" in cfg["destination_url"] else "?"
        target = cfg["destination_url"] + f"{sep}clickid={int(time.time())}_{match_id}"
        winner_name = cfg["winner_display"]

        copy = _GO_BET_COPY[lang]
        n_books = len(comp_pairs) + 1   # competitors + Betlabel

        # Pre-format competitor rows so the template doesn't need Jinja loops.
        # Each row has a stagger CSS animation delay so the list reveals
        # one-by-one over ~1.2s.
        comp_rows_html = ""
        # 4 competitors × 500ms ≈ 2.0s of staggered reveal, then Betlabel
        # pops in at ~2.2s and the user gets to admire the "winner" for
        # ~1.8s before the redirect fires at 4s total.
        per_step_ms = 500
        for i, (name, c_odd) in enumerate(comp_pairs):
            delay = i * per_step_ms
            comp_rows_html += (
                f'<li class="row" style="animation-delay:{delay}ms">'
                f'<span class="check">✓</span>'
                f'<span class="bk">{name}</span>'
                f'<span class="odd">{c_odd:.2f}</span>'
                f'</li>'
            )
        betlabel_delay = len(comp_pairs) * per_step_ms + 200
        countdown_start = max(1, int(round(delay_ms / 1000)))

        market_display = (market or "").strip()
        label_display  = (label or "").strip()
        sub_line = ""
        if market_display or label_display:
            sub_line = f'<div class="sub">{label_display} {f"· {market_display}" if market_display and label_display else market_display}</div>'

        # Welcome offer card — fades in 600 ms AFTER the winner row reveals.
        offer_html = ""
        if offer:
            offer_delay = betlabel_delay + 600
            offer_html = (
                f'<div class="offer" style="animation-delay:{offer_delay}ms">'
                f'  <div class="offer-icon">{offer["icon"]}</div>'
                f'  <div class="offer-body">'
                f'    <div class="offer-title">{offer["title"]}</div>'
                f'    <div class="offer-desc">{offer["desc"]}</div>'
                f'  </div>'
                f'</div>'
            )

        # Embed-mode adjustments: KEEP the dark background (otherwise the
        # light-on-white text in the SPA's modal is unreadable — see
        # screenshot from boss 2026-05). Just shrink the chrome and round
        # the corners so it sits nicely inside the SPA modal frame.
        embed_css = ""
        if embed:
            embed_css = (
                "html,body{min-height:auto !important;}"
                ".wrap{min-height:auto !important;padding:1.5rem 1.25rem 1rem !important;"
                "border-radius:14px;background:var(--bg);}"
            )

        html = f"""<!doctype html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="robots" content="noindex, nofollow">
<title>{copy['title']}</title>
<style>
  :root {{
    --bg: #0a1a0f; --card:#0f2418; --ink:#e2e8f0; --muted:#94a3b8;
    --line:#1a3b27; --accent:#22d3ee; --good:#22c55e; --bad:#ef4444;
    --warn: #fb923c;
  }}
  * {{ box-sizing:border-box; }}
  html,body {{ margin:0; padding:0; background:var(--bg); color:var(--ink);
    font-family: Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    min-height:100vh; -webkit-font-smoothing:antialiased; }}
  {embed_css}
  .wrap {{ max-width:520px; margin:0 auto; padding:2.5rem 1.25rem 1.5rem;
    min-height:100vh; display:flex; flex-direction:column; }}
  .header {{ text-align:center; margin-bottom:1.5rem; }}
  .spinner {{ width:36px; height:36px; border:3px solid var(--line);
    border-top-color:var(--accent); border-radius:50%; margin:0 auto 1rem;
    animation: spin 0.9s linear infinite; }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
  .title {{ font-size:1.25rem; font-weight:800; margin:0 0 .35rem; letter-spacing:-.01em; }}
  .sub {{ color:var(--muted); font-size:.92rem; margin-bottom:.25rem; }}
  .meta {{ color:var(--muted); font-size:.78rem; }}
  .list {{ list-style:none; padding:0; margin:0 0 1.25rem;
    background:var(--card); border:1px solid var(--line); border-radius:14px;
    overflow:hidden; }}
  .row {{ display:flex; align-items:center; justify-content:space-between;
    padding:.85rem 1.1rem; border-bottom:1px solid var(--line);
    opacity:0; animation: reveal .35s ease forwards;
    font-variant-numeric: tabular-nums; }}
  .row:last-child {{ border-bottom:0; }}
  @keyframes reveal {{
    from {{ opacity:0; transform: translateY(-4px); }}
    to {{ opacity:1; transform: translateY(0); }}
  }}
  .check {{ color:var(--good); font-weight:800; width:1.25rem; display:inline-block; }}
  .bk {{ flex:1; padding-left:.65rem; font-weight:600; color:var(--ink); }}
  .odd {{ font-weight:700; color:var(--muted); }}
  .row.best {{ background: rgba(34,197,94,0.08);
    animation: revealBest .5s ease forwards, pulse 1.4s ease-in-out infinite; }}
  .row.best .bk {{ color:#fff; }}
  .row.best .odd {{ color:var(--good); font-size:1.05rem; }}
  .row.best .check {{ color:var(--good); }}
  .badge {{ display:inline-block; background:var(--good); color:#062710;
    font-size:.62rem; font-weight:800; padding:.15rem .4rem; border-radius:4px;
    letter-spacing:.04em; margin-right:.4rem; }}
  @keyframes revealBest {{
    from {{ opacity:0; transform: scale(0.97); }}
    to {{ opacity:1; transform: scale(1); }}
  }}
  @keyframes pulse {{
    0%, 100% {{ background: rgba(34,197,94,0.08); }}
    50%      {{ background: rgba(34,197,94,0.18); }}
  }}
  .countdown {{ text-align:center; color:var(--muted); font-size:.85rem;
    margin-bottom:1rem; }}
  .countdown b {{ color:var(--ink); font-weight:800; font-variant-numeric: tabular-nums; }}
  .open-btn {{ display:block; width:100%; background:var(--warn);
    color:#1a1300; text-align:center; font-weight:800; padding:.95rem;
    border-radius:10px; text-decoration:none; font-size:1rem;
    transition: filter .15s, transform .12s;
    box-shadow: 0 6px 18px rgba(251,146,60,.25); }}
  .open-btn:hover {{ filter:brightness(1.08); transform: translateY(-1px); }}
  /* Welcome offer card under the "Open now" button */
  .offer {{ margin-top: .85rem; display:flex; align-items:center;
    gap: .75rem; padding: .75rem .9rem; background: var(--card);
    border: 1px solid var(--line); border-radius: 10px;
    opacity:0; animation: reveal .35s ease forwards; }}
  .offer-icon {{ font-size: 1.4rem; flex-shrink: 0; line-height: 1; }}
  .offer-body {{ flex: 1; min-width: 0; }}
  .offer-title {{ font-weight: 800; font-size: .85rem; color: #fff;
    margin-bottom: .15rem; }}
  .offer-desc {{ font-size: .78rem; color: var(--muted); line-height: 1.35; }}
  .small {{ color:var(--muted); font-size:.72rem; text-align:center;
    margin-top:1.25rem; line-height:1.45; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="spinner"></div>
    <h1 class="title">{copy['title']}</h1>
    {sub_line}
    <div class="meta">{copy['comparing'].replace('{n}', str(n_books))}</div>
  </div>

  <ul class="list">
    {comp_rows_html}
    <li class="row best" style="animation-delay:{betlabel_delay}ms">
      <span class="check">★</span>
      <span class="bk"><span class="badge">{copy['best']}</span>{winner_name}</span>
      <span class="odd">{odd:.2f}</span>
    </li>
  </ul>

  <a class="open-btn" id="openBtn" href="{target}" target="{'_blank' if embed else '_self'}" rel="noopener sponsored">{copy['open_now']}</a>

  {offer_html}

  <div class="small">{copy['vary']}</div>
</div>

<script>
  (function() {{
    var target = {json.dumps(target)};
    var delay  = {delay_ms};
    var embed  = {json.dumps(embed)};

    // Auto-redirect (no user gesture available). On mobile, asking the
    // SPA parent to window.open() is blocked by Safari/Chrome iOS popup
    // blockers — so we navigate the top window directly. Same-origin
    // (webpronos.com everywhere) so this is allowed; modal dismisses
    // naturally as the page navigates away.
    setTimeout(function autoRedirect() {{
      if (embed) {{
        // Best-effort: tell SPA the redirect is happening so it can
        // log / track / dismiss its modal in advance.
        try {{
          window.parent.postMessage({{
            type: 'webpronos:open-affiliate',
            url:  target,
            ts:   Date.now()
          }}, '*');
        }} catch(e) {{}}
        try {{ window.top.location.href = target; return; }} catch(e) {{}}
      }}
      window.location.replace(target);
    }}, delay);

    // Manual click on "Open now": the <a target="_blank"|"_self"> link
    // navigates natively under the user gesture — DO NOT preventDefault,
    // it kills the click on iOS. Just fire-and-forget postMessage so
    // the SPA can dismiss its modal in parallel.
    var btn = document.getElementById('openBtn');
    if (btn && embed) {{
      btn.addEventListener('click', function() {{
        try {{
          window.parent.postMessage({{
            type: 'webpronos:open-affiliate',
            url:  target,
            ts:   Date.now(),
            manual: true
          }}, '*');
        }} catch(e) {{}}
      }});
    }}
  }})();
</script>
</body>
</html>"""

        resp = Response(html, mimetype="text/html; charset=utf-8")
        # Never cache — odds change, competitor rotation depends on params
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["X-Robots-Tag"]  = "noindex, nofollow"
        return resp

    except Exception as e:
        log.exception(f"r_go_bet failed: {e}")
        # Bail straight to the matching affiliate URL — never strand the
        # user on an error page when they're trying to bet. Falls back
        # to betlabel if aff= param wasn't readable.
        try:
            fallback_aff = (flask_request.args.get("aff") or "betlabel").strip().lower()
            fb_url = _AFFILIATE_CONFIGS.get(fallback_aff, _AFFILIATE_CONFIGS["betlabel"])["destination_url"]
        except Exception:
            fb_url = _BETLABEL_AFFILIATE_URL
        return redirect(fb_url, code=302)


@app.route("/api/admin/affiliate/stats")
def r_admin_affiliate_stats():
    """Quick stats endpoint for the smart-loader CTR.

    Query: ?days=7  (default)
    Returns: total clicks, daily breakdown, top matches, country split.
    """
    try:
        days = max(1, min(90, int(flask_request.args.get("days") or 7)))
        since_ts = int(time.time()) - days * 86400
        with _db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS n FROM affiliate_clicks WHERE ts >= ?",
                (since_ts,)
            ).fetchone()["n"]
            by_day = [dict(r) for r in conn.execute(
                "SELECT date(ts, 'unixepoch') AS day, COUNT(*) AS clicks "
                "FROM affiliate_clicks WHERE ts >= ? "
                "GROUP BY day ORDER BY day DESC",
                (since_ts,)
            ).fetchall()]
            by_source = [dict(r) for r in conn.execute(
                "SELECT source, COUNT(*) AS clicks FROM affiliate_clicks "
                "WHERE ts >= ? GROUP BY source ORDER BY clicks DESC",
                (since_ts,)
            ).fetchall()]
            by_country = [dict(r) for r in conn.execute(
                "SELECT ip_country AS country, COUNT(*) AS clicks "
                "FROM affiliate_clicks WHERE ts >= ? "
                "GROUP BY country ORDER BY clicks DESC LIMIT 20",
                (since_ts,)
            ).fetchall()]
            top_matches = [dict(r) for r in conn.execute(
                "SELECT match_id, COUNT(*) AS clicks FROM affiliate_clicks "
                "WHERE ts >= ? AND match_id > 0 "
                "GROUP BY match_id ORDER BY clicks DESC LIMIT 10",
                (since_ts,)
            ).fetchall()]
        return jsonify({
            "ok":            True,
            "days":          days,
            "total":         total,
            "by_day":        by_day,
            "by_source":     by_source,
            "by_country":    by_country,
            "top_matches":   top_matches,
            "affiliate_url": _BETLABEL_AFFILIATE_URL,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─── Bandwidth stats (powers admin panel Bandwidth page) ─────────────────────

# Reference pricing for the Lovable admin panel to render alongside the
# actual measured usage. Numbers verified 2026-05; refresh quarterly.
# All ISP plans assume unlimited bandwidth so cost is constant regardless
# of measured volume; rotating residential plans cap monthly bandwidth.
_PROXY_PRICING = [
    {"name": "Webshare ISP — 20 IPs",      "price_eur": 30,  "limit_gb": None,  "type": "isp"},
    {"name": "IPRoyal ISP — 10 IPs",        "price_eur": 25,  "limit_gb": None,  "type": "isp"},
    {"name": "Smartproxy ISP — 20 IPs",     "price_eur": 56,  "limit_gb": None,  "type": "isp"},
    {"name": "Bright Data ISP — 10 IPs",    "price_eur": 150, "limit_gb": None,  "type": "isp"},
    {"name": "Webshare Residential — 25 GB",   "price_eur": 150, "limit_gb": 25,  "type": "residential"},
    {"name": "IPRoyal Residential — 25 GB",    "price_eur": 135, "limit_gb": 25,  "type": "residential"},
    {"name": "Smartproxy Residential — 25 GB", "price_eur": 200, "limit_gb": 25,  "type": "residential"},
    {"name": "Bright Data Residential — 25 GB","price_eur": 250, "limit_gb": 25,  "type": "residential"},
]


@app.route("/api/admin/bandwidth/stats")
def r_admin_bandwidth_stats():
    """Bandwidth consumption per host + per day, plus cost projections
    against the proxy plans we'd realistically buy. Powers the Bandwidth
    page in the Lovable admin panel.

    Query: ?days=7  (default 7, range 1-90)
    """
    try:
        # Flush any in-memory counters so the response reflects the very
        # latest cycle — otherwise the admin sees data up to the last
        # BG cycle flush, which can lag by 2 minutes.
        try: _flush_bw_counters()
        except Exception: pass

        days = max(1, min(90, int(flask_request.args.get("days") or 7)))
        since_ts_hour = (int(time.time()) // 3600) * 3600 - days * 86400

        with _db() as conn:
            by_host = [dict(r) for r in conn.execute(
                "SELECT host, SUM(bytes_in) AS bytes, SUM(request_count) AS requests "
                "FROM bandwidth_log WHERE ts_hour >= ? "
                "GROUP BY host ORDER BY bytes DESC",
                (since_ts_hour,)
            ).fetchall()]
            by_day = [dict(r) for r in conn.execute(
                "SELECT date(ts_hour, 'unixepoch') AS day, "
                "       SUM(bytes_in) AS bytes, SUM(request_count) AS requests "
                "FROM bandwidth_log WHERE ts_hour >= ? "
                "GROUP BY day ORDER BY day DESC",
                (since_ts_hour,)
            ).fetchall()]
            # Top-host-per-day for the stacked chart
            by_day_host = [dict(r) for r in conn.execute(
                "SELECT date(ts_hour, 'unixepoch') AS day, host, "
                "       SUM(bytes_in) AS bytes "
                "FROM bandwidth_log WHERE ts_hour >= ? "
                "GROUP BY day, host ORDER BY day DESC, bytes DESC",
                (since_ts_hour,)
            ).fetchall()]

        total_bytes    = sum(r["bytes"] for r in by_host)
        total_requests = sum(r["requests"] for r in by_host)
        days_with_data = max(1, len(by_day))
        avg_daily_gb   = total_bytes / 1e9 / days_with_data
        proj_month_gb  = round(avg_daily_gb * 30, 2)

        # Cost recommendation: cheapest provider whose plan still covers
        # our projected monthly volume. ISP plans (no cap) always qualify.
        recommendations = []
        for p in _PROXY_PRICING:
            fits = p["limit_gb"] is None or proj_month_gb <= p["limit_gb"]
            recommendations.append({
                **p,
                "fits_projected_usage": fits,
                "headroom_gb": (p["limit_gb"] - proj_month_gb) if p["limit_gb"] else None,
            })
        recommendations.sort(key=lambda x: (not x["fits_projected_usage"], x["price_eur"]))

        return jsonify({
            "ok": True,
            "window_days": days,
            "totals": {
                "bytes":      int(total_bytes),
                "gb":         round(total_bytes / 1e9, 3),
                "requests":   int(total_requests),
                "avg_per_request_kb": round((total_bytes / 1024 / total_requests) if total_requests else 0, 2),
            },
            "projection": {
                "avg_daily_gb":     round(avg_daily_gb, 3),
                "projected_monthly_gb": proj_month_gb,
                "projected_yearly_gb":  round(avg_daily_gb * 365, 1),
            },
            "by_host":     by_host,
            "by_day":      by_day,
            "by_day_host": by_day_host,
            "proxy_recommendations": recommendations,
            "note": (
                "Counter started at deploy; first 48h are still warming. "
                "ISP plans recommended (unlimited bandwidth, predictable cost) "
                "unless projected_monthly_gb is well under 25."
            ),
        })
    except Exception as e:
        log.exception("bandwidth stats endpoint failed")
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
#  Short-link redirector — pretty URLs for Telegram CTA buttons
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/r/<code>")
@app.route("/betradar/<code>")
def r_short_redirect(code: str):
    """302 to the stored target_url for this short code. Bumps click count
    so we have basic CTR analytics per Telegram pick CTA.

    Two aliases serve the same table:
      /r/<code>         — generic short links
      /betradar/<code>  — Telegram BetRadar AI bot CTAs (branded path)
    Same lookup behaviour either way.
    """
    try:
        # Reject obviously malformed codes early — tokens are 6 chars,
        # URL-safe base64, so >12 chars or weird chars = not ours.
        if not code or len(code) > 12 or not all(c.isalnum() or c in "-_" for c in code):
            return redirect("https://webpronos.com/", code=302)
        with _db() as conn:
            row = conn.execute(
                "SELECT target_url FROM short_links WHERE code = ?", (code,)
            ).fetchone()
        if not row:
            return redirect("https://webpronos.com/", code=302)
        # Best-effort click bump — never block redirect on the write.
        try:
            with _db() as conn:
                conn.execute(
                    "UPDATE short_links SET clicks = clicks + 1 WHERE code = ?",
                    (code,)
                )
        except Exception:
            pass
        return redirect(row["target_url"], code=302)
    except Exception as e:
        log.exception(f"r_short_redirect failed for {code}: {e}")
        return redirect("https://webpronos.com/", code=302)


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
    """robots.txt — points crawlers to the sitemap index, blocks API/admin paths.

    Both the master index and the legacy flat sitemap are declared. Crawlers
    that recognise sitemap indexes (Google, Bing) will follow /sitemap_index.xml
    and discover each sub-sitemap with per-category lastmod, saving crawl
    budget. The legacy /sitemap.xml stays declared so older crawlers and
    cached Search Console state don't break.
    """
    body = (
        "User-agent: *\n"
        "Disallow: /api/\n"
        "Disallow: /admin/\n"
        "Disallow: /telegram/\n"
        "Disallow: /prerender\n"
        "Disallow: /proxy/\n"
        "Allow: /\n"
        "\n"
        # Single canonical entry point — the index references every sub-sitemap.
        # Legacy /sitemap.xml is still served at the URL for backwards-compat
        # with any externally cached references, but is NOT advertised here
        # (boss feedback: avoid confusing crawlers with multiple top-level
        # sitemaps to submit).
        f"Sitemap: {SITE_URL}/sitemap_index.xml\n"
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


def _sm_urls_matches_for_locale(locale: str) -> list[str]:
    """Locale-aware variant of _sm_urls_matches. Same selection rules
    (live + scheduled next-30d + finished-with-picks last-7d) but each
    URL is prefixed with the locale (/br, /pt, /es) and declares hreflang
    alternates to its EN/BR/PT/ES siblings (same match id, different
    locale prefix). Match slugs don't translate — only the URL prefix
    differs across locales."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    next_30d = now_ts + 30 * 86400
    last_7d  = now_ts - 7 * 86400
    prefix = _LOCALE_TO_PREFIX.get(locale, "")
    urls: list[str] = []
    seen: set[int] = set()

    def _emit(mid: int, home: str, away: str, lastmod: str, cf: str, pri: str):
        slug_part = f"{_slug(home)}-{_slug(away)}"
        en_path  = f"/match/{mid}/{slug_part}"
        # The current locale's URL uses its dynamic prefix
        # (/jogo, /partido). Same for hreflang siblings: each locale's
        # alternate uses its own translated prefix so Google indexes
        # the same content under the right canonical per language.
        loc_path = _localized_dynamic_path(en_path, locale)
        urls.append(_xml_url_i18n(
            loc        = f"{SITE_URL}{prefix}{loc_path}",
            lastmod    = lastmod,
            changefreq = cf,
            priority   = pri,
            alternates = {
                "en":        f"{SITE_URL}{en_path}",
                "pt-br":     f"{SITE_URL}/br{_localized_dynamic_path(en_path, 'pt-br')}",
                "pt-pt":     f"{SITE_URL}/pt{_localized_dynamic_path(en_path, 'pt-pt')}",
                "es":        f"{SITE_URL}/es{_localized_dynamic_path(en_path, 'es')}",
                "x-default": f"{SITE_URL}{en_path}",
            },
        ))

    try:
        with _state_lock:
            for entry in _live_state.values():
                m = entry.get("match", {})
                mid = m.get("id")
                if mid and m.get("statusType") == "inprogress":
                    _emit(mid, m.get("homeTeam", "home"), m.get("awayTeam", "away"),
                          now.strftime("%Y-%m-%d"), "always", "0.95")
                    seen.add(mid)
    except Exception:
        pass

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
                _emit(r["id"], r["home_team"], r["away_team"],
                      now.strftime("%Y-%m-%d"), "daily", "0.8")
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
                lastmod = (datetime.fromtimestamp(r["last_ts"], tz=timezone.utc).strftime("%Y-%m-%d")
                           if r["last_ts"] else now.strftime("%Y-%m-%d"))
                _emit(r["id"], r["home_team"], r["away_team"], lastmod, "monthly", "0.5")
                seen.add(r["id"])
    except Exception as e:
        log.warning(f"sitemap-matches[{locale}]: {e}")
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


def _sm_urls_blog(lang: str = "en", url_prefix: str = "") -> list[str]:
    """Emit blog post URLs for the given lang. The URLs are constructed
    with `url_prefix` (eg "/br") so callers can build sitemap-br.xml etc.
    Default behaviour (lang='en', url_prefix='') preserves the original
    output exactly — backward compatible for sitemap.xml + index."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    urls: list[str] = []
    try:
        import urllib.request as _ur
        supa_url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?select=slug,published_at"
            f"&lang=eq.{lang}"
            f"&order=published_at.desc&limit=500"
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
                urls.append(_xml_url(f"{SITE_URL}{url_prefix}/blog/{slug}", pub, "monthly", "0.6"))
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


def _sm_envelope_i18n(urls: list[str]) -> str:
    """Sitemap envelope with the xhtml namespace declared so `<xhtml:link
    rel="alternate" hreflang="...">` annotations are valid. Used for the
    per-language sitemaps (sitemap-br.xml, future sitemap-pt.xml, etc.)
    where each URL declares its locale siblings."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n'
        '        xmlns:xhtml="http://www.w3.org/1999/xhtml">\n'
        + "\n".join(urls) + "\n"
        '</urlset>'
    )


def _xml_url_i18n(loc: str, lastmod: str, changefreq: str, priority: str,
                   alternates: dict[str, str]) -> str:
    """Render a <url> entry that declares hreflang alternates inline.

    `alternates` maps hreflang code → absolute URL. Include the current
    URL too (Google requires self-reference in the alternate set) plus
    an "x-default" entry for the EN canonical.
    """
    alt_lines = [
        f'    <xhtml:link rel="alternate" hreflang="{lang}" href="{url}"/>'
        for lang, url in alternates.items()
    ]
    return (
        f"  <url>\n"
        f"    <loc>{loc}</loc>\n"
        f"    <lastmod>{lastmod}</lastmod>\n"
        f"    <changefreq>{changefreq}</changefreq>\n"
        f"    <priority>{priority}</priority>\n"
        + "\n".join(alt_lines) + "\n"
        f"  </url>"
    )


def _sm_br_alt_set(en_path: str) -> dict[str, str]:
    """Hreflang alternate set for a static BR sitemap entry.
    EN keeps the canonical path; each non-EN locale uses its slug
    translation (passthrough if no translation registered)."""
    return {
        "en":        f"{SITE_URL}{en_path}",
        "pt-br":     f"{SITE_URL}/br{_localized_slug(en_path, 'pt-br')}",
        "pt-pt":     f"{SITE_URL}/pt{_localized_slug(en_path, 'pt-pt')}",
        "es":        f"{SITE_URL}/es{_localized_slug(en_path, 'es')}",
        "x-default": f"{SITE_URL}{en_path}",
    }


# Locale prefix used in URLs (no slash on either side).
# pt-br → "br", pt-pt → "pt", es → "es"
_LOCALE_URL_KEY = {"pt-br": "br", "pt-pt": "pt", "es": "es"}


def _sm_urls_locale_static_pages(locale: str) -> list[str]:
    """Locale-generic sitemap entries for STATIC pages (homepage +
    legal/info + blog index). Each URL uses the locale's localized slug
    and declares hreflang alternates to its EN/BR/PT/ES siblings.

    Pass locale ∈ {pt-br, pt-pt, es}. The prefix in the URL is derived
    from `_LOCALE_URL_KEY` (/br, /pt, /es).
    """
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix_key = _LOCALE_URL_KEY[locale]
    pages = [
        ("/",                     today, "daily",   "1.0"),
        ("/about",                today, "monthly", "0.5"),
        ("/responsible-gambling", today, "yearly",  "0.3"),
        ("/terms",                today, "yearly",  "0.3"),
        ("/privacy",              today, "yearly",  "0.3"),
        ("/blog",                 today, "weekly",  "0.7"),
    ]
    urls = []
    for en_stripped, lastmod, cf, pri in pages:
        local_slug = _localized_slug(en_stripped, locale)
        loc = (f"{SITE_URL}/{prefix_key}/" if en_stripped == "/"
               else f"{SITE_URL}/{prefix_key}{local_slug}")
        urls.append(_xml_url_i18n(
            loc        = loc,
            lastmod    = lastmod,
            changefreq = cf,
            priority   = pri,
            alternates = _sm_br_alt_set(en_stripped),
        ))
    return urls


def _sm_urls_locale_blog(locale: str) -> list[str]:
    """Locale-generic sitemap entries for BLOG POSTS. Walks _BLOG_SLUG_I18N,
    looks up each row's published_at from Supabase (filtered to `lang=locale`),
    and emits the locale-specific slug as canonical with EN/BR/PT/ES alts.

    A post is included only if it has a translation registered for the
    requested locale. Posts without a translation are skipped (the locale's
    sitemap should never list URLs that don't exist).
    """
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix_key = _LOCALE_URL_KEY[locale]
    urls: list[str] = []
    try:
        import urllib.request as _ur, urllib.parse as _up
        for en_slug, langs in _BLOG_SLUG_I18N.items():
            local_slug = langs.get(locale)
            if not local_slug:
                continue
            try:
                supa_url = (
                    f"{SUPABASE_URL}/rest/v1/blog_posts"
                    f"?slug=eq.{_up.quote(local_slug)}"
                    f"&lang=eq.{locale}"
                    f"&select=published_at&limit=1"
                )
                req = _ur.Request(supa_url, headers={
                    "apikey":        SUPABASE_ANON,
                    "Authorization": f"Bearer {SUPABASE_ANON}",
                })
                with _ur.urlopen(req, timeout=4) as r:
                    rows = json.loads(r.read())
                pub = (rows[0].get("published_at") if rows else today)[:10]
            except Exception:
                pub = today
            blog_alts = {
                "en":        f"{SITE_URL}/blog/{en_slug}",
                "pt-br":     f"{SITE_URL}/br/blog/{_localized_blog_slug(en_slug, 'pt-br')}",
                "pt-pt":     f"{SITE_URL}/pt/blog/{_localized_blog_slug(en_slug, 'pt-pt')}",
                "es":        f"{SITE_URL}/es/blog/{_localized_blog_slug(en_slug, 'es')}",
                "x-default": f"{SITE_URL}/blog/{en_slug}",
            }
            urls.append(_xml_url_i18n(
                loc        = f"{SITE_URL}/{prefix_key}/blog/{local_slug}",
                lastmod    = pub,
                changefreq = "monthly",
                priority   = "0.6",
                alternates = blog_alts,
            ))
    except Exception as e:
        log.debug(f"sitemap-{prefix_key} blog fetch failed: {e}")
    return urls


def _sm_lastmod_locale_blog(locale: str) -> str:
    """Freshest published_at across blog posts translated to `locale`.
    Used by the sitemap index to drive conditional crawls."""
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        import urllib.request as _ur
        url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?select=published_at"
            f"&lang=eq.{locale}"
            f"&order=published_at.desc&limit=1"
        )
        req = _ur.Request(url, headers={
            "apikey":        SUPABASE_ANON,
            "Authorization": f"Bearer {SUPABASE_ANON}",
        })
        with _ur.urlopen(req, timeout=3) as r:
            arr = json.loads(r.read())
        if arr:
            return (arr[0].get("published_at") or "")[:10] or today
    except Exception as e:
        log.debug(f"_sm_lastmod_locale_blog({locale}): {e}")
    return today


# Backward-compatible thin wrappers — older callers in this file still
# reference the BR-named helpers. Same behaviour, locale fixed to pt-br.
def _sm_urls_br_static_pages() -> list[str]:
    return _sm_urls_locale_static_pages("pt-br")


def _sm_urls_br_blog() -> list[str]:
    return _sm_urls_locale_blog("pt-br")


def _sm_lastmod_br_blog() -> str:
    return _sm_lastmod_locale_blog("pt-br")


# ─── Per-category lastmod helpers ────────────────────────────────────────────
# These power the sitemap-index <sitemap><lastmod> element so Google can tell
# at a glance which sub-sitemaps changed since its last crawl and skip the
# rest. Each helper runs ONE cheap query (or returns today's date for static
# pages). Results are not cached because they're already O(1)–O(log N).
def _sm_lastmod_today() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _sm_lastmod_max_tip_ts() -> str:
    """Freshest tip timestamp — used as lastmod for teams & leagues (both
    surface the same underlying tip stream)."""
    from datetime import datetime, timezone
    try:
        with _db() as conn:
            row = conn.execute("SELECT MAX(wall_ts) AS m FROM tips").fetchone()
        m = row["m"] if row and row["m"] else None
        if m:
            return datetime.fromtimestamp(m, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception as e:
        log.warning(f"_sm_lastmod_max_tip_ts: {e}")
    return _sm_lastmod_today()


def _sm_lastmod_matches() -> str:
    """Freshest of: max game start_ts (newly scheduled) OR max tip wall_ts."""
    from datetime import datetime, timezone
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT MAX(ts) AS m FROM ("
                "  SELECT MAX(start_ts) AS ts FROM games "
                "  UNION ALL "
                "  SELECT MAX(wall_ts) AS ts FROM tips"
                ")").fetchone()
        m = row["m"] if row and row["m"] else None
        if m:
            return datetime.fromtimestamp(m, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception as e:
        log.warning(f"_sm_lastmod_matches: {e}")
    return _sm_lastmod_today()


def _sm_lastmod_blog() -> str:
    """Freshest published_at across blog posts (Supabase). Falls back to today
    if Supabase is unreachable — better to over-report than block the index."""
    try:
        import urllib.request as _ur
        url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?select=published_at&order=published_at.desc&limit=1"
        )
        req = _ur.Request(url, headers={
            "apikey":        SUPABASE_ANON,
            "Authorization": f"Bearer {SUPABASE_ANON}",
        })
        with _ur.urlopen(req, timeout=3) as r:
            arr = json.loads(r.read())
        if arr:
            return (arr[0].get("published_at") or "")[:10] or _sm_lastmod_today()
    except Exception as e:
        log.debug(f"_sm_lastmod_blog: {e}")
    return _sm_lastmod_today()


def _sm_index_entry(loc: str, lastmod: str) -> str:
    return (
        f"  <sitemap>\n"
        f"    <loc>{loc}</loc>\n"
        f"    <lastmod>{lastmod}</lastmod>\n"
        f"  </sitemap>"
    )


def _sm_index_envelope(entries: list[str]) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries) + "\n"
        '</sitemapindex>'
    )


@app.route("/sitemap.xml")
def r_sitemap():
    """
    Legacy flat sitemap with ALL URLs (~1000) — pages, leagues, teams,
    matches, blog. Kept for backwards compatibility because Google has it
    cached and Search Console references it. New crawlers should follow
    /sitemap_index.xml (apwin-style hierarchy with per-category lastmod).
    The Cloudflare worker (cloudflare-worker.js) routes /^\\/sitemap.*\\.xml$/
    directly to Flask so the sub-sitemap URLs resolve correctly.
    """
    urls = (
        _sm_urls_pages()
        + _sm_urls_leagues()
        + _sm_urls_teams()
        + _sm_urls_matches()
        + _sm_urls_blog()
    )
    return _xml_response(_sm_envelope(urls), len(urls))


@app.route("/sitemap_index.xml")
def r_sitemap_index():
    """
    Master sitemap index — preferred entry point for crawlers.

    Each sub-sitemap has its own <lastmod> derived from the freshest URL
    inside it, so Google can re-crawl only the parts that changed since the
    previous visit instead of pulling one monolithic ~1000-URL file.

    Categories:
      pages    — static + market hub pages           (~15)
      leagues  — one URL per monitored league        (~100)
      teams    — one URL per team that ever had a tip(~500)
      matches  — live + 30-day window of scheduled & recently finished
      blog     — Supabase-backed blog posts
    """
    pages_lastmod    = _sm_lastmod_today()
    tips_lastmod     = _sm_lastmod_max_tip_ts()
    matches_lastmod  = _sm_lastmod_matches()
    blog_lastmod     = _sm_lastmod_blog()
    br_blog_lastmod  = _sm_lastmod_locale_blog("pt-br")
    pt_blog_lastmod  = _sm_lastmod_locale_blog("pt-pt")
    es_blog_lastmod  = _sm_lastmod_locale_blog("es")

    entries = [
        _sm_index_entry(f"{SITE_URL}/sitemap-pages.xml",       pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-leagues.xml",     tips_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-teams.xml",       tips_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-matches.xml",     matches_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-blog.xml",        blog_lastmod),
        # Per-locale sub-sitemaps. Listed flat (not via per-locale indexes)
        # because sitemap-index files cannot reference other index files
        # per the sitemaps.org spec — must point at urlset documents.
        _sm_index_entry(f"{SITE_URL}/sitemap-br-pages.xml",    pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-br-blog.xml",     br_blog_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-br-matches.xml",  matches_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-pt-pages.xml",    pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-pt-blog.xml",     pt_blog_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-pt-matches.xml",  matches_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-es-pages.xml",    pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-es-blog.xml",     es_blog_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-es-matches.xml",  matches_lastmod),
    ]
    body = _sm_index_envelope(entries)
    return Response(body, mimetype="application/xml", headers={
        "Cache-Control":   "public, max-age=3600, s-maxage=3600",
        "X-Sitemap-Index": str(len(entries)),
    })


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


@app.route("/sitemap-br.xml")
def r_sitemap_br():
    """BR-locale sitemap INDEX — references the per-category sub-sitemaps
    for the BR side. Mirrors the EN /sitemap_index.xml structure so the
    BR ecosystem scales the same way (each category gets independent
    lastmod, Google re-crawls only what changed).

    Sub-sitemaps:
      sitemap-br-pages.xml  — static legal/info + homepage + blog index
      sitemap-br-blog.xml   — translated blog posts (slug-localized)

    Add new sub-sitemaps here as dynamic pages (today, history, team,
    league, match, tips) get BR chrome translations.
    """
    pages_lastmod   = _sm_lastmod_today()
    blog_lastmod    = _sm_lastmod_br_blog()
    matches_lastmod = _sm_lastmod_matches()
    entries = [
        _sm_index_entry(f"{SITE_URL}/sitemap-br-pages.xml",   pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-br-blog.xml",    blog_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-br-matches.xml", matches_lastmod),
    ]
    body = _sm_index_envelope(entries)
    return Response(body, mimetype="application/xml", headers={
        "Cache-Control":    "public, max-age=3600, s-maxage=3600",
        "X-Sitemap-Index":  str(len(entries)),
        "X-Sitemap-Locale": "pt-br",
    })


@app.route("/sitemap-br-pages.xml")
def r_sitemap_br_pages():
    """BR static pages — homepage, legal/info, blog index."""
    urls = _sm_urls_br_static_pages()
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "pt-br",
        },
    )


@app.route("/sitemap-br-blog.xml")
def r_sitemap_br_blog():
    """BR blog posts — one URL per (slug, lang='pt-br') row."""
    urls = _sm_urls_br_blog()
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "pt-br",
        },
    )


# ─── PT-PT sitemaps ──────────────────────────────────────────────────────
# Mirror of the BR routes for the Portugal locale. Same structure (index +
# pages + blog), same generators (locale-parameterised helpers above).

@app.route("/sitemap-pt.xml")
def r_sitemap_pt():
    """PT-PT-locale sitemap INDEX — references the per-category sub-sitemaps
    for the PT side. Discovery endpoint for robots.txt / manual submission.
    The master `/sitemap_index.xml` references the leaf urlsets directly."""
    pages_lastmod   = _sm_lastmod_today()
    blog_lastmod    = _sm_lastmod_locale_blog("pt-pt")
    matches_lastmod = _sm_lastmod_matches()
    entries = [
        _sm_index_entry(f"{SITE_URL}/sitemap-pt-pages.xml",   pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-pt-blog.xml",    blog_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-pt-matches.xml", matches_lastmod),
    ]
    body = _sm_index_envelope(entries)
    return Response(body, mimetype="application/xml", headers={
        "Cache-Control":    "public, max-age=3600, s-maxage=3600",
        "X-Sitemap-Index":  str(len(entries)),
        "X-Sitemap-Locale": "pt-pt",
    })


@app.route("/sitemap-pt-pages.xml")
def r_sitemap_pt_pages():
    """PT-PT static pages — homepage, legal/info, blog index."""
    urls = _sm_urls_locale_static_pages("pt-pt")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "pt-pt",
        },
    )


@app.route("/sitemap-pt-blog.xml")
def r_sitemap_pt_blog():
    """PT-PT blog posts — one URL per (slug, lang='pt-pt') row."""
    urls = _sm_urls_locale_blog("pt-pt")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "pt-pt",
        },
    )


# ─── ES sitemaps ─────────────────────────────────────────────────────────
# Mirror of the BR/PT routes for the Spanish locale.

@app.route("/sitemap-es.xml")
def r_sitemap_es():
    """ES-locale sitemap INDEX — references the per-category sub-sitemaps
    for the ES side. Discovery endpoint for robots.txt / manual submission."""
    pages_lastmod   = _sm_lastmod_today()
    blog_lastmod    = _sm_lastmod_locale_blog("es")
    matches_lastmod = _sm_lastmod_matches()
    entries = [
        _sm_index_entry(f"{SITE_URL}/sitemap-es-pages.xml",   pages_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-es-blog.xml",    blog_lastmod),
        _sm_index_entry(f"{SITE_URL}/sitemap-es-matches.xml", matches_lastmod),
    ]
    body = _sm_index_envelope(entries)
    return Response(body, mimetype="application/xml", headers={
        "Cache-Control":    "public, max-age=3600, s-maxage=3600",
        "X-Sitemap-Index":  str(len(entries)),
        "X-Sitemap-Locale": "es",
    })


@app.route("/sitemap-es-pages.xml")
def r_sitemap_es_pages():
    """ES static pages — homepage, legal/info, blog index."""
    urls = _sm_urls_locale_static_pages("es")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "es",
        },
    )


@app.route("/sitemap-es-blog.xml")
def r_sitemap_es_blog():
    """ES blog posts — one URL per (slug, lang='es') row."""
    urls = _sm_urls_locale_blog("es")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "es",
        },
    )


# ─── Per-locale matches sitemaps ──────────────────────────────────────────
# Each match has 4 URL variants (/match/X, /br/match/X, /pt/match/X, /es/match/X)
# all serving the same backend content with different locale chrome. We list
# each variant in its own locale sitemap with hreflang alternates pointing to
# the others — Google's canonical signal then groups them as translated
# duplicates instead of treating them as competing URLs.

@app.route("/sitemap-br-matches.xml")
def r_sitemap_br_matches():
    """BR matches — locale-prefixed (/br/match/...) with hreflang alternates."""
    urls = _sm_urls_matches_for_locale("pt-br")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "pt-br",
        },
    )


@app.route("/sitemap-pt-matches.xml")
def r_sitemap_pt_matches():
    """PT-PT matches — locale-prefixed (/pt/match/...) with hreflang alternates."""
    urls = _sm_urls_matches_for_locale("pt-pt")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "pt-pt",
        },
    )


@app.route("/sitemap-es-matches.xml")
def r_sitemap_es_matches():
    """ES matches — locale-prefixed (/es/match/...) with hreflang alternates."""
    urls = _sm_urls_matches_for_locale("es")
    return Response(
        _sm_envelope_i18n(urls),
        mimetype="application/xml",
        headers={
            "Cache-Control":   "public, max-age=3600, s-maxage=3600",
            "X-Sitemap-Urls":  str(len(urls)),
            "X-Sitemap-Locale": "es",
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


# ════════════════════════════════════════════════════════════════════════════
#  PWA + Web Push notifications
# ════════════════════════════════════════════════════════════════════════════
# Lets users install webpronos.com as a Progressive Web App on iOS/Android/
# desktop and receive push notifications even when the tab is closed (or the
# phone is locked). Complements the existing in-page SSE/toast bell — that
# one fires when the tab is focused; this one fires anytime.
#
# Layers:
#   • /manifest.json   — PWA install metadata (name, icons, theme, scope)
#   • /sw.js           — Service Worker (handles push, click, install events)
#   • /api/push/*      — subscribe / unsubscribe / public-key endpoints
#   • push_subscriptions DB table (one row per (endpoint, p256dh, auth))
#   • Hook in _broadcast_pick → fan-out via pywebpush to all eligible rows
#
# Secrets (all 3 required for push to actually fire):
#   VAPID_PUBLIC_KEY      — base64url-encoded ECDSA public key (also exposed
#                            to the browser via /api/push/vapid-public-key)
#   VAPID_PRIVATE_KEY     — PEM-encoded ECDSA private key (server-only)
#   VAPID_CONTACT_EMAIL   — "mailto:..." string included in VAPID JWT claim
#                            (FCM/APNs use this to contact us re: abuse)
VAPID_PUBLIC_KEY     = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY    = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_CONTACT_EMAIL  = os.environ.get("VAPID_CONTACT_EMAIL", "mailto:hi@webpronos.com")


def _push_configured() -> bool:
    return bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)


# ── pywebpush import is lazy ────────────────────────────────────────────────
# We don't crash boot if the lib isn't installed yet (e.g. local dev). The
# fan-out path will skip cleanly with a warning. Production has the package
# pinned in requirements.txt.
try:
    from pywebpush import webpush, WebPushException  # type: ignore
    _PYWEBPUSH_AVAILABLE = True
except Exception as _pp_err:
    _PYWEBPUSH_AVAILABLE = False
    log.warning(f"pywebpush import failed (push disabled): {_pp_err}")


# ── PWA manifest ────────────────────────────────────────────────────────────
# The browser fetches this once when the user taps "Add to Home Screen" or
# Chrome's install prompt fires. Locale is read from ?lang= so the installed
# app name matches what the user sees in the SPA. Defaults to EN.
_PWA_MANIFEST_COPY = {
    "en":    {"name": "WebPronos — Live AI Picks", "short": "WebPronos"},
    "br":    {"name": "WebPronos — Palpites IA ao vivo", "short": "WebPronos"},
    "pt-br": {"name": "WebPronos — Palpites IA ao vivo", "short": "WebPronos"},
    "pt-pt": {"name": "WebPronos — Picks IA em direto", "short": "WebPronos"},
    "es":    {"name": "WebPronos — Picks IA en vivo", "short": "WebPronos"},
}


# ── PWA icons (generated on-the-fly from static/logo.png) ──────────────────
# Avoids committing binary variants. Pillow resizes once per size and caches
# the bytes in memory. The maskable variant pads the logo so the safe zone
# (inner 80%) contains the full logo — required by the maskable spec.
_PWA_ICON_CACHE: dict[str, bytes] = {}
_PWA_ICON_LOCK = threading.Lock()


def _build_pwa_icon(kind: str) -> bytes:
    """kind ∈ {'192','512','mask','badge'}."""
    from PIL import Image
    from io import BytesIO

    base_path = os.path.join(os.path.dirname(__file__), "static", "logo.png")
    if not os.path.exists(base_path):
        raise FileNotFoundError(base_path)
    src = Image.open(base_path).convert("RGBA")

    if kind == "192":
        out = src.resize((192, 192), Image.LANCZOS)
    elif kind == "512":
        out = src.resize((512, 512), Image.LANCZOS)
    elif kind == "badge":
        # Monochrome 72x72 silhouette for Android lockscreen badge.
        out = src.resize((72, 72), Image.LANCZOS)
    elif kind == "mask":
        # 512x512 canvas with the logo at 80% so adaptive masks crop safely.
        canvas = Image.new("RGBA", (512, 512), (15, 23, 42, 255))  # bg=#0f172a
        safe = int(512 * 0.72)
        inner = src.resize((safe, safe), Image.LANCZOS)
        off = (512 - safe) // 2
        canvas.paste(inner, (off, off), inner if inner.mode == "RGBA" else None)
        out = canvas
    else:
        raise ValueError(kind)

    buf = BytesIO()
    out.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


@app.route("/pwa-icon/<kind>.png", methods=["GET"])
def pwa_icon(kind: str):
    kind = kind.lower()
    if kind not in ("192", "512", "mask", "badge"):
        return Response("not found", status=404)
    with _PWA_ICON_LOCK:
        png = _PWA_ICON_CACHE.get(kind)
        if png is None:
            try:
                png = _build_pwa_icon(kind)
                _PWA_ICON_CACHE[kind] = png
            except Exception as e:
                log.error(f"pwa_icon({kind}) failed: {e}")
                return Response("icon generation failed", status=500)
    resp = Response(png, mimetype="image/png")
    resp.headers["Cache-Control"] = "public, max-age=604800, immutable"
    return resp


@app.route("/manifest.json", methods=["GET"])
def pwa_manifest():
    """PWA install manifest. Locale-aware via ?lang= query param."""
    lang = (flask_request.args.get("lang") or "en").lower()
    copy = _PWA_MANIFEST_COPY.get(lang) or _PWA_MANIFEST_COPY["en"]
    manifest = {
        "name":             copy["name"],
        "short_name":       copy["short"],
        "description":      "Live AI football picks powered by xG model",
        "start_url":        "/?utm_source=pwa",
        "scope":            "/",
        "display":          "standalone",
        "orientation":      "portrait-primary",
        "background_color": "#0f172a",
        "theme_color":      "#10b981",
        "lang":             lang if lang in ("en","es","pt-pt","pt-br","br") else "en",
        "icons": [
            {"src": "/pwa-icon/192.png",  "sizes": "192x192", "type": "image/png"},
            {"src": "/pwa-icon/512.png",  "sizes": "512x512", "type": "image/png"},
            {"src": "/pwa-icon/mask.png", "sizes": "512x512", "type": "image/png",
             "purpose": "maskable"},
        ],
        "categories": ["sports", "news"],
    }
    resp = jsonify(manifest)
    resp.headers["Cache-Control"] = "public, max-age=3600"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# ── Service Worker ──────────────────────────────────────────────────────────
# Served from the site root (NOT /static/) so its scope can be "/" — required
# by the Service Worker spec. Handles 3 events:
#   • install / activate — claim clients immediately so first-paint works
#   • push               — show notification with title/body/icon from payload
#   • notificationclick  — open the match page (or focus existing tab)
_SERVICE_WORKER_JS = r"""
// WebPronos Service Worker — Web Push + PWA shell
// version: 1
const SW_VERSION = 'wp-sw-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) {}

  const title = data.title || 'New pick — WebPronos';
  const opts  = {
    body:    data.body  || '',
    icon:    data.icon  || '/pwa-icon/192.png',
    badge:   data.badge || '/pwa-icon/badge.png',
    tag:     data.tag   || ('pick-' + Date.now()),
    data:    { url: data.url || '/' },
    vibrate: [120, 60, 120],
    requireInteraction: false,
  };
  event.waitUntil(self.registration.showNotification(title, opts));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const target = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil((async () => {
    const list = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of list) {
      if (c.url.includes(target) && 'focus' in c) return c.focus();
    }
    if (self.clients.openWindow) return self.clients.openWindow(target);
  })());
});
"""


@app.route("/sw.js", methods=["GET"])
def service_worker():
    """Serve the service worker from root scope. Cache 1 hour."""
    resp = Response(_SERVICE_WORKER_JS, mimetype="application/javascript")
    # Browser must re-check periodically so SW updates roll out.
    resp.headers["Cache-Control"] = "public, max-age=3600"
    resp.headers["Service-Worker-Allowed"] = "/"
    return resp


# ── In-page bootstrap helper ────────────────────────────────────────────────
# Tiny JS injected by the Cloudflare Worker into HTML responses. Exposes
# `window.wpEnablePush()` so the user can opt-in from the SPA bell or via
# DevTools console while we're still pre-Lovable.
_PUSH_BOOTSTRAP_JS = r"""
(function () {
  if (!('serviceWorker' in navigator)) return;
  // Register once at idle so it doesn't compete with first paint.
  const reg = () => navigator.serviceWorker.register('/sw.js', { scope: '/' })
    .catch(e => console.warn('[wp-push] sw register failed', e));
  if (document.readyState === 'complete') reg();
  else window.addEventListener('load', reg);

  function urlBase64ToUint8Array(b64) {
    const padding = '='.repeat((4 - b64.length % 4) % 4);
    const base64 = (b64 + padding).replace(/-/g, '+').replace(/_/g, '/');
    const raw = atob(base64);
    const out = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
    return out;
  }

  window.wpEnablePush = async function () {
    if (!('PushManager' in window)) { console.warn('[wp-push] PushManager unsupported'); return false; }
    const perm = await Notification.requestPermission();
    if (perm !== 'granted') { console.warn('[wp-push] permission denied'); return false; }
    const sw = await navigator.serviceWorker.ready;
    const keyResp = await fetch('/api/push/vapid-public-key');
    const { key } = await keyResp.json();
    if (!key) { console.warn('[wp-push] no VAPID key from server'); return false; }
    let sub = await sw.pushManager.getSubscription();
    if (!sub) {
      sub = await sw.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(key),
      });
    }
    const json = sub.toJSON();
    const lang = (document.documentElement.lang || 'en').toLowerCase();
    await fetch('/api/push/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        endpoint: json.endpoint,
        p256dh:   json.keys && json.keys.p256dh,
        auth:     json.keys && json.keys.auth,
        locale:   lang,
      }),
    });
    console.info('[wp-push] subscribed');
    return true;
  };

  window.wpDisablePush = async function () {
    const sw = await navigator.serviceWorker.ready;
    const sub = await sw.pushManager.getSubscription();
    if (!sub) return true;
    const endpoint = sub.endpoint;
    await sub.unsubscribe();
    await fetch('/api/push/unsubscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint }),
    });
    console.info('[wp-push] unsubscribed');
    return true;
  };
})();
"""


@app.route("/push-bootstrap.js", methods=["GET"])
def push_bootstrap_js():
    """Bootstrap script that the Cloudflare Worker injects into HTML pages."""
    resp = Response(_PUSH_BOOTSTRAP_JS, mimetype="application/javascript")
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/api/push/vapid-public-key", methods=["GET"])
def push_vapid_public_key():
    """Expose the VAPID public key so the browser can request a subscription."""
    resp = jsonify({"key": VAPID_PUBLIC_KEY or None,
                    "configured": _push_configured()})
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/api/push/subscribe", methods=["POST"])
def push_subscribe():
    """Record a new (or refreshed) browser push subscription."""
    if not _push_configured():
        return jsonify({"ok": False, "error": "push_not_configured"}), 503
    try:
        body = flask_request.get_json(force=True, silent=True) or {}
        endpoint = (body.get("endpoint") or "").strip()
        p256dh   = (body.get("p256dh") or "").strip()
        auth_k   = (body.get("auth") or "").strip()
        locale   = (body.get("locale") or "en").lower()[:8]
        if not endpoint or not p256dh or not auth_k:
            return jsonify({"ok": False, "error": "missing_fields"}), 400
        ua = (flask_request.headers.get("User-Agent") or "")[:300]
        now = int(time.time())
        with _db() as conn:
            # UPSERT: if endpoint already known, refresh keys + reactivate.
            conn.execute("""
                INSERT INTO push_subscriptions
                    (endpoint, p256dh, auth, locale, user_agent,
                     created_at, last_seen_at, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(endpoint) DO UPDATE SET
                    p256dh       = excluded.p256dh,
                    auth         = excluded.auth,
                    locale       = excluded.locale,
                    user_agent   = excluded.user_agent,
                    last_seen_at = excluded.last_seen_at,
                    active       = 1
            """, (endpoint, p256dh, auth_k, locale, ua, now, now))
        return jsonify({"ok": True})
    except Exception as e:
        log.error(f"push_subscribe error: {e}")
        return jsonify({"ok": False, "error": "server_error"}), 500


@app.route("/api/push/unsubscribe", methods=["POST"])
def push_unsubscribe():
    """Mark a subscription inactive (we keep the row for audit)."""
    try:
        body = flask_request.get_json(force=True, silent=True) or {}
        endpoint = (body.get("endpoint") or "").strip()
        if not endpoint:
            return jsonify({"ok": False, "error": "missing_endpoint"}), 400
        with _db() as conn:
            conn.execute(
                "UPDATE push_subscriptions SET active = 0 WHERE endpoint = ?",
                (endpoint,)
            )
        return jsonify({"ok": True})
    except Exception as e:
        log.error(f"push_unsubscribe error: {e}")
        return jsonify({"ok": False, "error": "server_error"}), 500


# ── pywebpush send helper ───────────────────────────────────────────────────
# Wraps the actual HTTP call to the push service (FCM / APNs / Mozilla / etc.)
# behind a stable API. Returns (ok, status_code, error_str).
def _push_send_one(endpoint: str, p256dh: str, auth_k: str,
                   payload: dict) -> tuple[bool, int, str]:
    if not _PYWEBPUSH_AVAILABLE or not _push_configured():
        return False, 0, "push_unavailable"
    try:
        webpush(
            subscription_info={
                "endpoint": endpoint,
                "keys": {"p256dh": p256dh, "auth": auth_k},
            },
            data=json.dumps(payload),
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_CONTACT_EMAIL},
            ttl=900,  # 15 min — picks go stale fast
        )
        return True, 201, ""
    except WebPushException as we:  # type: ignore
        status = getattr(we.response, "status_code", 0) if getattr(we, "response", None) else 0
        return False, status, str(we)[:200]
    except Exception as e:
        return False, 0, str(e)[:200]


# ── Per-locale notification copy ────────────────────────────────────────────
_PUSH_LOCALE_COPY = {
    "en":    {"new_pick": "New pick", "edge": "edge", "min": "min"},
    "br":    {"new_pick": "Novo palpite", "edge": "edge", "min": "min"},
    "pt-br": {"new_pick": "Novo palpite", "edge": "edge", "min": "min"},
    "pt-pt": {"new_pick": "Nova pick", "edge": "edge", "min": "min"},
    "es":    {"new_pick": "Nuevo pick", "edge": "edge", "min": "min"},
}


def _broadcast_push_pick(match: dict, pick: dict, minute: int | None) -> None:
    """
    Fan-out a new pick to all active push subscribers.
    Called from _broadcast_pick alongside SSE / Telegram / X fan-out.
    No-ops cleanly if push is not configured.
    """
    if not _push_configured() or not _PYWEBPUSH_AVAILABLE:
        return
    try:
        home = match.get("homeTeam", "")
        away = match.get("awayTeam", "")
        market = pick.get("market", "")
        label = pick.get("label", "")
        odds = pick.get("odds") or 0
        edge = pick.get("edge") or 0
        match_id = match.get("id") or match.get("match_id")
        flag = _country_flag(match.get("country", ""))
        min_str = f" · {minute}'" if minute is not None else ""

        # Body shared across locales (numbers + team names are language-agnostic).
        body_core = f"{home} vs {away}{min_str}\n{market}: {label} @ {odds:.2f} · +{edge:.1f}%"

        # Build per-locale payload variants.
        payloads_by_lang = {}
        for lang, copy in _PUSH_LOCALE_COPY.items():
            payloads_by_lang[lang] = {
                "title": f"{flag} {copy['new_pick']}",
                "body":  body_core,
                "url":   f"/match/{match_id}" if match_id else "/",
                "tag":   f"pick-{match_id}",
            }
        default_payload = payloads_by_lang["en"]

        # Load active subs.
        with _db() as conn:
            rows = conn.execute("""
                SELECT id, endpoint, p256dh, auth, locale
                FROM push_subscriptions
                WHERE active = 1
            """).fetchall()

        if not rows:
            return

        sent = 0
        failed = 0
        purged = 0
        now_ts = int(time.time())
        for sub_id, endpoint, p256dh, auth_k, locale in rows:
            payload = payloads_by_lang.get((locale or "en").lower(), default_payload)
            ok, status, err = _push_send_one(endpoint, p256dh, auth_k, payload)
            if ok:
                sent += 1
                try:
                    with _db() as conn:
                        conn.execute("""
                            UPDATE push_subscriptions
                            SET send_count = send_count + 1, last_sent_at = ?
                            WHERE id = ?
                        """, (now_ts, sub_id))
                except Exception:
                    pass
            else:
                failed += 1
                # 404/410 = gone, mark inactive permanently.
                if status in (404, 410):
                    purged += 1
                    try:
                        with _db() as conn:
                            conn.execute(
                                "UPDATE push_subscriptions SET active = 0 WHERE id = ?",
                                (sub_id,)
                            )
                    except Exception:
                        pass
                else:
                    try:
                        with _db() as conn:
                            conn.execute("""
                                UPDATE push_subscriptions
                                SET fail_count = fail_count + 1
                                WHERE id = ?
                            """, (sub_id,))
                    except Exception:
                        pass
        log.info(f"push fan-out: sent={sent} failed={failed} purged={purged} total={len(rows)}")
    except Exception as e:
        log.error(f"_broadcast_push_pick error: {e}")


@app.route("/api/admin/push/test", methods=["POST"])
def push_admin_test():
    """
    Admin-only: send a test push to all active subscribers (or a single endpoint
    if `endpoint` is provided in the JSON body).
    """
    if not _check_admin_auth():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if not _push_configured():
        return jsonify({"ok": False, "error": "push_not_configured"}), 503
    body = flask_request.get_json(force=True, silent=True) or {}
    target_endpoint = (body.get("endpoint") or "").strip() or None
    title = body.get("title") or "WebPronos test"
    msg   = body.get("body")  or "If you see this, push works ✅"
    payload = {"title": title, "body": msg, "url": "/", "tag": "wp-test"}

    with _db() as conn:
        if target_endpoint:
            rows = conn.execute("""
                SELECT id, endpoint, p256dh, auth FROM push_subscriptions
                WHERE active = 1 AND endpoint = ?
            """, (target_endpoint,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT id, endpoint, p256dh, auth FROM push_subscriptions
                WHERE active = 1
            """).fetchall()

    sent, failed = 0, 0
    for _id, endpoint, p256dh, auth_k in rows:
        ok, status, err = _push_send_one(endpoint, p256dh, auth_k, payload)
        if ok: sent += 1
        else:  failed += 1
    return jsonify({"ok": True, "sent": sent, "failed": failed, "total": len(rows)})


@app.route("/api/admin/push/stats", methods=["GET"])
def push_admin_stats():
    """Admin-only: show subscription counts by status."""
    if not _check_admin_auth():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    with _db() as conn:
        active = conn.execute(
            "SELECT COUNT(*) FROM push_subscriptions WHERE active = 1"
        ).fetchone()[0]
        inactive = conn.execute(
            "SELECT COUNT(*) FROM push_subscriptions WHERE active = 0"
        ).fetchone()[0]
        by_locale = conn.execute("""
            SELECT locale, COUNT(*) FROM push_subscriptions
            WHERE active = 1 GROUP BY locale
        """).fetchall()
    return jsonify({
        "ok":          True,
        "active":      active,
        "inactive":    inactive,
        "by_locale":   {(l or "en"): n for l, n in by_locale},
        "configured":  _push_configured(),
        "pywebpush":   _PYWEBPUSH_AVAILABLE,
    })


# ════════════════════════════════════════════════════════════════════════════
#  In-app Sofascore watchdog + auto-failover
# ════════════════════════════════════════════════════════════════════════════
# Replaces the GitHub Actions-based watchdog whose */30 cron was being
# throttled to ~2h gaps on the free tier (real-world: 2026-05-22 incident
# where Sofascore blocked the bom region between 18:14 UTC and ~20:18 UTC
# and no GH Actions run fired in that window).
#
# Architecture:
#   • APScheduler job every 5 min calls _failover_probe() — a curl_cffi call
#     to Sofascore's /events/live endpoint from inside this machine.
#   • In-memory counter `_FAILOVER_STATE['consecutive_failures']` increments
#     on HTTP != 200 (403/429/timeout). Resets to 0 on first success.
#   • After 3 consecutive failures (~15 min of confirmed block) AND the
#     last failover was > 30 min ago, fire _failover_execute(target_region).
#   • _failover_execute uses the Fly Machines REST API + GraphQL to:
#       1. Get our own machine_id + region + volume_id (via REST)
#       2. Pick next region from FAILOVER_REGION_POOL (excluding current)
#       3. Fork our volume to target region (GraphQL `forkVolume` mutation)
#       4. Clone our machine to target region with the forked volume attached
#       5. Poll new machine until its /api/health/sofascore returns healthy
#       6. Telegram alert + write to failover_audit table
#       7. Destroy current machine (this kills the process executing this!)
#
# Safety guards:
#   • 30-min cooldown between failovers (prevent loops on flaky regions)
#   • Region pool excludes the current region (no self-migrate no-op)
#   • If fork or clone API call fails, abort + alert (no destroy)
#   • Manual trigger via /api/admin/failover/trigger?to=lhr (admin-token-gated)
#
# Required Fly secret:
#   FLY_API_TOKEN  — deploy-scoped token for this app (api.machines.dev +
#                    api.fly.io/graphql access). Created with
#                    `fly tokens create deploy --app livexgmodel-pt`.

FLY_API_TOKEN = os.environ.get("FLY_API_TOKEN", "")
FLY_APP_NAME  = os.environ.get("FLY_APP_NAME", "livexgmodel-pt")

# Priority-ordered candidate regions. First reachable + Sofascore-unblocked
# wins. Excludes the current region at runtime. Europe-first because most
# users are EU; falls back to NA/global if Cloudflare blocks the whole EU.
FAILOVER_REGION_POOL = ["lhr", "ams", "cdg", "fra", "dub", "arn",
                        "mad", "ewr", "iad", "yyz", "sjc"]

# In-memory state. Lives only in this process — reset on every machine
# restart. The failover_audit SQLite table persists the historical record.
_FAILOVER_STATE: dict = {
    "consecutive_failures":  0,
    "last_probe_ts":         0,
    "last_probe_status":     None,   # "ok" / "blocked" / "error"
    "last_probe_http":       0,
    "last_failover_ts":      0,
    "currently_failing_over": False,
}
_FAILOVER_LOCK = threading.Lock()

# Cooldown so we never fire two failovers back-to-back.
FAILOVER_COOLDOWN_S = 1800          # 30 min
FAILOVER_FAILURE_THRESHOLD = 3       # 3 × 5 min = ~15 min sustained block


def _failover_probe() -> tuple[bool, int, str]:
    """
    Probe Sofascore from this machine using curl_cffi + chrome fingerprint
    (same setup as the scraper, so this proves end-to-end reachability).
    Returns (ok, http_status, error_or_empty).
    """
    try:
        from curl_cffi import requests as crq  # type: ignore
        r = crq.get(
            "https://api.sofascore.com/api/v1/sport/football/events/live",
            impersonate="chrome120",
            timeout=15,
        )
        return (r.status_code == 200, r.status_code, "")
    except Exception as e:
        return (False, 0, type(e).__name__ + ": " + str(e)[:120])


# ── Fly API wrappers ────────────────────────────────────────────────────────
# We use 2 endpoints:
#   • Machines REST API (api.machines.dev) for list/clone/destroy machines
#   • GraphQL (api.fly.io/graphql) for volume fork (no REST endpoint exists
#     for fork as of 2026).
def _fly_rest(method: str, path: str, body: dict | None = None,
              timeout: int = 30) -> tuple[int, dict | str]:
    """REST call to api.machines.dev. Returns (status, parsed_json_or_text)."""
    if not FLY_API_TOKEN:
        return (0, "FLY_API_TOKEN not configured")
    import urllib.request as _u
    import urllib.error as _ue
    url = f"https://api.machines.dev/v1{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = _u.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {FLY_API_TOKEN}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    })
    try:
        with _u.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", errors="replace")
            try:
                return (r.status, json.loads(raw) if raw else {})
            except Exception:
                return (r.status, raw)
    except _ue.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="replace")
            return (e.code, json.loads(raw) if raw else {"error": str(e)})
        except Exception:
            return (e.code, {"error": str(e)})
    except Exception as e:
        return (0, {"error": str(e)})


def _fly_graphql(query: str, variables: dict | None = None,
                 timeout: int = 30) -> tuple[int, dict]:
    """GraphQL call to api.fly.io. Returns (status, parsed_json)."""
    if not FLY_API_TOKEN:
        return (0, {"error": "FLY_API_TOKEN not configured"})
    import urllib.request as _u
    import urllib.error as _ue
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = _u.Request("https://api.fly.io/graphql", data=body, method="POST",
                     headers={
                         "Authorization": f"Bearer {FLY_API_TOKEN}",
                         "Content-Type":  "application/json",
                     })
    try:
        with _u.urlopen(req, timeout=timeout) as r:
            return (r.status, json.loads(r.read()))
    except _ue.HTTPError as e:
        try:
            return (e.code, json.loads(e.read()))
        except Exception:
            return (e.code, {"error": str(e)})
    except Exception as e:
        return (0, {"error": str(e)})


def _fly_current_machine() -> dict | None:
    """
    Return {id, region, image, name, mount_path, volume_id} for the machine
    running this code, or None if undetectable. Uses Fly's FLY_MACHINE_ID
    env var (set by Fly on every machine) + cross-references the list API
    to get volume + region.
    """
    my_id = os.environ.get("FLY_MACHINE_ID", "")
    if not my_id:
        return None
    status, data = _fly_rest("GET", f"/apps/{FLY_APP_NAME}/machines")
    if status != 200 or not isinstance(data, list):
        return None
    me = next((m for m in data if m.get("id") == my_id), None)
    if not me:
        return None
    cfg = me.get("config") or {}
    mounts = cfg.get("mounts") or []
    mount = mounts[0] if mounts else {}
    return {
        "id":         my_id,
        "name":       me.get("name"),
        "region":     me.get("region"),
        "image":      cfg.get("image"),
        "mount_path": mount.get("path") or "/data",
        "volume_id":  mount.get("volume"),
    }


def _fly_fork_volume(source_volume_id: str, target_region: str,
                     name: str) -> tuple[str | None, str]:
    """
    Fork a volume into target_region via GraphQL. Returns (new_volume_id, error).
    """
    # The mutation name is `forkVolume` on api.fly.io/graphql.
    query = """
    mutation ForkVolume($input: ForkVolumeInput!) {
      forkVolume(input: $input) {
        volume { id name region sizeGb }
      }
    }
    """
    # Resolve the app's GraphQL ID. The Fly GraphQL API requires the app's
    # GraphQL ID (not the slug) for some mutations — but forkVolume accepts
    # source volume ID directly which carries app context.
    variables = {
        "input": {
            "sourceVolId": source_volume_id,
            "destinationRegion": target_region,
            "name": name,
            "machinesOnly": True,
        }
    }
    status, data = _fly_graphql(query, variables, timeout=120)
    if status != 200:
        return (None, f"http={status} body={str(data)[:200]}")
    errors = data.get("errors") or []
    if errors:
        return (None, f"gql_errors={str(errors)[:300]}")
    vid = (((data.get("data") or {}).get("forkVolume") or {})
           .get("volume") or {}).get("id")
    if not vid:
        return (None, f"no_id_in_response={str(data)[:200]}")
    return (vid, "")


def _fly_clone_machine(source_machine_id: str, target_region: str,
                       attach_volume_id: str, mount_path: str) -> tuple[str | None, str]:
    """
    Create a new machine in target_region by copying the source's config and
    attaching the given (already-forked) volume. Returns (new_machine_id, err).
    """
    # Fetch source machine's full config so we replicate identically.
    status, src = _fly_rest("GET", f"/apps/{FLY_APP_NAME}/machines/{source_machine_id}")
    if status != 200 or not isinstance(src, dict):
        return (None, f"get_source_failed http={status} body={str(src)[:200]}")
    src_cfg = src.get("config") or {}
    # Replace mounts with the forked volume.
    new_cfg = dict(src_cfg)
    new_cfg["mounts"] = [{
        "volume": attach_volume_id,
        "path":   mount_path,
    }]
    body = {
        "region": target_region,
        "config": new_cfg,
        # name omitted → Fly auto-generates a unique haiku name
    }
    status, data = _fly_rest("POST", f"/apps/{FLY_APP_NAME}/machines",
                              body=body, timeout=60)
    if status not in (200, 201):
        return (None, f"clone_failed http={status} body={str(data)[:300]}")
    new_id = (data or {}).get("id") if isinstance(data, dict) else None
    if not new_id:
        return (None, f"no_id_in_response={str(data)[:200]}")
    return (new_id, "")


def _fly_destroy_machine(machine_id: str) -> tuple[bool, str]:
    """Stop + delete a machine. Returns (ok, error)."""
    # Stop first (best-effort).
    _fly_rest("POST", f"/apps/{FLY_APP_NAME}/machines/{machine_id}/stop",
              timeout=20)
    time.sleep(3)
    status, data = _fly_rest("DELETE",
                              f"/apps/{FLY_APP_NAME}/machines/{machine_id}?force=true",
                              timeout=20)
    if status in (200, 204):
        return (True, "")
    return (False, f"http={status} body={str(data)[:200]}")


def _failover_wait_new_machine_healthy(timeout_s: int = 240) -> bool:
    """
    Poll our public /api/health/sofascore until it reports healthy=True with
    a recent cycle. Used after a clone to confirm the new machine is alive
    AND its scrape is unblocked. Returns True on success, False on timeout.
    """
    import urllib.request as _u
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with _u.urlopen("https://livexgmodel-pt.fly.dev/api/health/sofascore",
                            timeout=10) as r:
                body = json.loads(r.read())
                if body.get("healthy") is True:
                    return True
        except Exception:
            pass
        time.sleep(8)
    return False


def _failover_alert(text: str) -> None:
    """Send Telegram alert to admin chat (best-effort)."""
    try:
        admin_chat = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
        if admin_chat and TELEGRAM_BOT_TOKEN:
            _send_telegram(text, chat_id=int(admin_chat))
    except Exception as e:
        log.warning(f"_failover_alert failed: {e}")


def _failover_audit(started_at: int, from_region: str, to_region: str,
                    reason: str, status: str,
                    new_machine_id: str | None = None,
                    new_volume_id: str | None = None,
                    error: str | None = None) -> None:
    """Persist a failover attempt to the audit table."""
    try:
        with _db() as conn:
            conn.execute("""
                INSERT INTO failover_audit
                  (started_at, finished_at, from_region, to_region, reason,
                   status, new_machine_id, new_volume_id, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (started_at, int(time.time()), from_region, to_region,
                  reason, status, new_machine_id, new_volume_id, error))
    except Exception as e:
        log.warning(f"_failover_audit insert failed: {e}")


def _failover_execute(target_region: str | None = None,
                      reason: str = "auto: 3 consecutive probe failures") -> dict:
    """
    Execute the full failover dance. Synchronous — the calling thread will
    likely be killed (along with the machine) at the very end.
    Returns a dict with the outcome, but caller may never see it.
    """
    with _FAILOVER_LOCK:
        if _FAILOVER_STATE.get("currently_failing_over"):
            return {"ok": False, "error": "already_in_progress"}
        _FAILOVER_STATE["currently_failing_over"] = True

    started_at = int(time.time())
    try:
        if not FLY_API_TOKEN:
            err = "FLY_API_TOKEN not configured"
            _failover_audit(started_at, "?", target_region or "?", reason,
                            "failed", error=err)
            _failover_alert(f"🔴 Auto-failover ABORTED — {err}")
            return {"ok": False, "error": err}

        me = _fly_current_machine()
        if not me:
            err = "could not detect current machine via Fly API"
            _failover_audit(started_at, "?", target_region or "?", reason,
                            "failed", error=err)
            _failover_alert(f"🔴 Auto-failover ABORTED — {err}")
            return {"ok": False, "error": err}

        cur_region = me["region"]
        cur_machine = me["id"]
        cur_volume = me["volume_id"]
        mount_path = me["mount_path"]

        if not target_region:
            target_region = next((r for r in FAILOVER_REGION_POOL
                                  if r != cur_region), None)
        if not target_region or target_region == cur_region:
            err = f"no candidate region (cur={cur_region}, target={target_region})"
            _failover_audit(started_at, cur_region, target_region or "?",
                            reason, "failed", error=err)
            _failover_alert(f"🔴 Auto-failover ABORTED — {err}")
            return {"ok": False, "error": err}

        log.warning(f"[failover] STARTING {cur_region} → {target_region} "
                    f"(machine={cur_machine}, volume={cur_volume})")
        _failover_alert(
            f"⚠️ <b>Auto-failover STARTED</b>\n"
            f"From: <code>{cur_region}</code> → <code>{target_region}</code>\n"
            f"Reason: {reason}\n"
            f"Machine: <code>{cur_machine}</code>\n"
            f"Volume: <code>{cur_volume}</code>"
        )

        # 1. Fork volume
        new_vol, fork_err = _fly_fork_volume(
            source_volume_id=cur_volume,
            target_region=target_region,
            name=f"tips_data_{target_region}",
        )
        if not new_vol:
            err = f"fork_volume failed: {fork_err}"
            _failover_audit(started_at, cur_region, target_region, reason,
                            "failed", error=err)
            _failover_alert(f"🔴 Auto-failover FAILED at fork — {err}")
            return {"ok": False, "error": err}
        log.warning(f"[failover] forked volume → {new_vol}")

        # 2. Clone machine with new volume attached
        new_mid, clone_err = _fly_clone_machine(
            source_machine_id=cur_machine,
            target_region=target_region,
            attach_volume_id=new_vol,
            mount_path=mount_path,
        )
        if not new_mid:
            err = f"clone_machine failed: {clone_err}"
            _failover_audit(started_at, cur_region, target_region, reason,
                            "failed", new_volume_id=new_vol, error=err)
            _failover_alert(f"🔴 Auto-failover FAILED at clone — {err}")
            return {"ok": False, "error": err}
        log.warning(f"[failover] cloned machine → {new_mid}")

        # 3. Wait for new machine to report healthy via the public health URL
        healthy = _failover_wait_new_machine_healthy(timeout_s=240)
        if not healthy:
            err = "new machine never reported healthy within 4 min"
            _failover_audit(started_at, cur_region, target_region, reason,
                            "failed", new_machine_id=new_mid,
                            new_volume_id=new_vol, error=err)
            _failover_alert(
                f"🔴 Auto-failover FAILED — new machine in {target_region} "
                f"never went healthy. Manual cleanup needed:\n"
                f"machine: <code>{new_mid}</code>\nvolume: <code>{new_vol}</code>"
            )
            return {"ok": False, "error": err}
        log.warning(f"[failover] new machine healthy")

        # 4. Success — record + alert BEFORE we destroy ourselves
        _failover_audit(started_at, cur_region, target_region, reason,
                        "success", new_machine_id=new_mid,
                        new_volume_id=new_vol)
        _FAILOVER_STATE["last_failover_ts"] = int(time.time())
        _failover_alert(
            f"✅ <b>Auto-failover SUCCESS</b>\n"
            f"{cur_region} → {target_region}\n"
            f"New machine: <code>{new_mid}</code>\n"
            f"New volume: <code>{new_vol}</code>\n"
            f"Destroying old machine <code>{cur_machine}</code> now..."
        )

        # 5. Destroy self (this kills the process — code after may not run)
        time.sleep(2)
        _fly_destroy_machine(cur_machine)
        return {"ok": True, "new_machine": new_mid, "new_volume": new_vol}

    except Exception as e:
        log.error(f"_failover_execute fatal: {e}", exc_info=True)
        _failover_audit(started_at, "?", target_region or "?", reason,
                        "failed", error=str(e)[:300])
        _failover_alert(f"🔴 Auto-failover EXCEPTION — {str(e)[:200]}")
        return {"ok": False, "error": str(e)}
    finally:
        with _FAILOVER_LOCK:
            _FAILOVER_STATE["currently_failing_over"] = False


def _failover_probe_job():
    """
    APScheduler entry-point. Probes Sofascore. Increments/resets counters.
    Fires a failover when threshold is hit and cooldown is over.
    Safe to run from any worker — uses an in-process lock to avoid
    duplicate fires within the same machine.
    """
    try:
        ok, http, err = _failover_probe()
        now_ts = int(time.time())
        with _FAILOVER_LOCK:
            _FAILOVER_STATE["last_probe_ts"]   = now_ts
            _FAILOVER_STATE["last_probe_http"] = http
            if ok:
                if _FAILOVER_STATE["consecutive_failures"] > 0:
                    log.info(f"[failover] probe recovered after "
                             f"{_FAILOVER_STATE['consecutive_failures']} failures")
                _FAILOVER_STATE["consecutive_failures"] = 0
                _FAILOVER_STATE["last_probe_status"]    = "ok"
                return
            _FAILOVER_STATE["consecutive_failures"] += 1
            _FAILOVER_STATE["last_probe_status"]     = "blocked" if http in (403,429) else "error"
            count = _FAILOVER_STATE["consecutive_failures"]
            last_fo = _FAILOVER_STATE["last_failover_ts"]
            in_progress = _FAILOVER_STATE["currently_failing_over"]

        log.warning(f"[failover] probe failed #{count} (http={http}, err={err[:80]})")

        # Trigger conditions
        if count >= FAILOVER_FAILURE_THRESHOLD and not in_progress:
            cooldown_left = (last_fo + FAILOVER_COOLDOWN_S) - now_ts
            if cooldown_left > 0:
                log.warning(f"[failover] threshold hit but cooldown "
                            f"{cooldown_left}s remaining — skipping")
                return
            log.warning(f"[failover] triggering auto-failover "
                        f"(count={count}, last_http={http})")
            # Spawn in a background thread so the scheduler tick returns
            # quickly. The thread will be killed when self-destroy lands.
            t = threading.Thread(
                target=_failover_execute,
                kwargs={"reason": f"auto: {count} consecutive failures (http={http})"},
                daemon=True,
            )
            t.start()
    except Exception as e:
        log.error(f"_failover_probe_job error: {e}")


@app.route("/api/admin/failover/status", methods=["GET"])
def r_admin_failover_status():
    """Admin: current watchdog state + recent audit rows."""
    if not _check_admin_auth():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT id, started_at, finished_at, from_region, to_region,
                       reason, status, new_machine_id, new_volume_id,
                       error_message
                FROM failover_audit
                ORDER BY id DESC LIMIT 20
            """).fetchall()
            audit = [dict(r) for r in rows]
    except Exception:
        audit = []
    me = _fly_current_machine() or {}
    return jsonify({
        "ok":         True,
        "configured": bool(FLY_API_TOKEN),
        "current": {
            "machine_id": me.get("id"),
            "region":     me.get("region"),
            "volume_id":  me.get("volume_id"),
        },
        "state":  _FAILOVER_STATE,
        "thresholds": {
            "failure_count":   FAILOVER_FAILURE_THRESHOLD,
            "cooldown_s":      FAILOVER_COOLDOWN_S,
            "region_pool":     FAILOVER_REGION_POOL,
        },
        "audit":  audit,
    })


@app.route("/api/admin/failover/trigger", methods=["POST"])
def r_admin_failover_trigger():
    """
    Admin: manually trigger a failover. Bypasses the threshold + cooldown.
    Optional JSON body: {"to": "ams"}. If omitted, picks next region.
    """
    if not _check_admin_auth():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    body = flask_request.get_json(force=True, silent=True) or {}
    target = (body.get("to") or "").strip().lower() or None
    # Run synchronously so the admin sees the outcome in the HTTP response —
    # but the destroy-self step will likely cut the response short.
    out = _failover_execute(target_region=target,
                            reason=f"manual via admin trigger (target={target})")
    return jsonify(out)


# ════════════════════════════════════════════════════════════════════════════
#  inbet.io membership-status sync + Telegram fan-out
# ════════════════════════════════════════════════════════════════════════════
# All endpoints in this block authenticate with the shared secret
# INBET_SYNC_SECRET (Fly.io secret). Inbet's backend uses this to push
# member status changes to us, and we use it to query their member API.

INBET_SYNC_SECRET    = os.environ.get("INBET_SYNC_SECRET", "")
INBET_BOT_TOKEN      = os.environ.get("INBET_BOT_TOKEN", "")
INBET_BOT_USERNAME   = os.environ.get("INBET_BOT_USERNAME", "")
INBET_MEMBER_STATUS_URL = os.environ.get(
    "INBET_MEMBER_STATUS_URL",
    "https://app.inbet.io/api/internal/members/{member_uuid}/status",
)

# Plan statuses that receive Telegram pick alerts.
INBET_ELIGIBLE_STATUSES = {"premium", "trial", "demo"}


def _check_inbet_sync_auth() -> bool:
    """Validate the X-InBetIO-Sync-Secret header against the shared secret."""
    if not INBET_SYNC_SECRET:
        return False
    header_val = flask_request.headers.get("X-InBetIO-Sync-Secret", "").strip()
    return header_val == INBET_SYNC_SECRET


def _send_inbet_telegram(text: str, chat_id: int,
                         buttons: list | None = None) -> bool:
    """
    Send a Telegram message via the DEDICATED inbet bot (NOT the WebPronos bot).
    Uses INBET_BOT_TOKEN. Returns True on 2xx response, False otherwise.

    `buttons` (optional) is an inline_keyboard structure — a list of rows where
    each row is a list of {text, callback_data|url} dicts. Used for the
    language picker and any future quick-action UIs.
    """
    if not INBET_BOT_TOKEN:
        log.warning("_send_inbet_telegram: INBET_BOT_TOKEN not configured, skipping")
        return False
    try:
        url = f"https://api.telegram.org/bot{INBET_BOT_TOKEN}/sendMessage"
        body: dict = {
            "chat_id":                chat_id,
            "text":                   text,
            "parse_mode":             "HTML",
            "disable_web_page_preview": True,
        }
        if buttons:
            body["reply_markup"] = {"inline_keyboard": buttons}
        payload = json.dumps(body).encode()
        import urllib.request as _u
        req = _u.Request(url, data=payload, headers={"Content-Type": "application/json"})
        _u.urlopen(req, timeout=10)
        return True
    except Exception as e:
        log.error(f"_send_inbet_telegram failed for chat {chat_id}: {e}")
        return False


def _answer_inbet_callback(callback_query_id: str, text: str = "") -> None:
    """Acknowledge a callback_query so the loading spinner clears on the user's
    side. Safe to call without text. No-op if token isn't configured."""
    if not INBET_BOT_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{INBET_BOT_TOKEN}/answerCallbackQuery"
        payload = json.dumps({
            "callback_query_id": callback_query_id,
            "text":              text,
        }).encode()
        import urllib.request as _u
        req = _u.Request(url, data=payload, headers={"Content-Type": "application/json"})
        _u.urlopen(req, timeout=5)
    except Exception as e:
        log.warning(f"_answer_inbet_callback failed: {e}")


# ── Language picker ─────────────────────────────────────────────────────────
# Same 4 codes the rest of the site uses. Sent as inline buttons after /start
# and on /lang. Tapping a button sets inbet_subscribers.locale for that chat.
_INBET_LANG_BUTTONS = [
    [
        {"text": "🇬🇧 English",    "callback_data": "lang:en"},
        {"text": "🇪🇸 Español",    "callback_data": "lang:es"},
    ],
    [
        {"text": "🇵🇹 Português",  "callback_data": "lang:pt-pt"},
        {"text": "🇧🇷 Brasileiro", "callback_data": "lang:pt-br"},
    ],
]

_INBET_LANG_PROMPT = {
    "en":    "🌐 Choose your language for pick alerts:",
    "es":    "🌐 Elige tu idioma para los avisos de picks:",
    "pt-pt": "🌐 Escolhe o teu idioma para os alertas de picks:",
    "pt-br": "🌐 Escolha seu idioma para os alertas de palpites:",
}

_INBET_LANG_CONFIRMED = {
    "en":    "✅ Language set to English.",
    "es":    "✅ Idioma cambiado a Español.",
    "pt-pt": "✅ Idioma alterado para Português.",
    "pt-br": "✅ Idioma alterado para Português (BR).",
}


def _broadcast_inbet_pick(match: dict, pick: dict, minute: int | None):
    """
    Fan-out a new pick to every ELIGIBLE inbet subscriber via the dedicated
    Telegram bot. Called alongside the SSE broadcast in _broadcast_pick.

    Eligibility = active=1 AND plan_status IN (premium|trial|demo).
    Tournament restricted to WC 2026 matches only (this is a WC-scoped product).
    No per-member throttling: paid members receive every value pick the model
    surfaces — the algorithm itself is what bounds the frequency (typically
    1-4 picks per match), so artificial caps would just hide tips users paid
    for. Members who want fewer notifications can /stop the bot.
    """
    if not INBET_BOT_TOKEN:
        return  # bot not configured — silently skip

    # Only WC 2026 matches
    tourn = match.get("tournament", "") or ""
    variants = {_normalize_tournament(v) for v in _wc_tournament_variants()}
    if _normalize_tournament(tourn) not in variants:
        return

    try:
        with _db() as conn:
            subs = conn.execute(
                "SELECT chat_id, member_uuid, locale, plan_status "
                "FROM inbet_subscribers "
                "WHERE active = 1 AND plan_status IN ('premium','trial','demo')"
            ).fetchall()
    except Exception as e:
        log.warning(f"_broadcast_inbet_pick: subs query failed: {e}")
        return

    if not subs:
        return

    home  = match.get("homeTeam", "")
    away  = match.get("awayTeam", "")
    flag_h = _country_flag(match.get("homeCountry", "") or match.get("country", ""))
    flag_a = _country_flag(match.get("awayCountry", "") or "")
    market = pick.get("market", "")
    label  = pick.get("label", "")
    odds   = pick.get("odds") or 0
    edge   = pick.get("edge") or 0
    minute_str = f"{minute}'" if minute is not None else ""

    sent = 0
    for s in subs:
        locale = s["locale"] or "en"
        msg_lines = [
            f"🏆 <b>FIFA World Cup 2026</b>",
            f"{flag_h} <b>{home}</b> vs <b>{away}</b> {flag_a} · {minute_str}",
            "",
            f"📊 <b>{market}:</b> {label}",
            f"💰 <b>{_t(locale, 'min_entered') if False else 'Odds'}:</b> @{odds:.2f}",
            f"📈 <b>Edge:</b> +{edge:.1f}%",
        ]
        msg = "\n".join(msg_lines)
        if _send_inbet_telegram(msg, s["chat_id"]):
            sent += 1
    log.info(f"_broadcast_inbet_pick: sent to {sent}/{len(subs)} subscribers")


def _fetch_inbet_member_status(member_uuid: str) -> dict | None:
    """Call inbet's API to get current plan status. Returns {plan_status, active, locale} or None."""
    if not INBET_SYNC_SECRET:
        return None
    try:
        import urllib.request as _u
        url = INBET_MEMBER_STATUS_URL.format(member_uuid=member_uuid)
        req = _u.Request(url, headers={
            "X-InBetIO-Sync-Secret": INBET_SYNC_SECRET,
            "Accept": "application/json",
        })
        with _u.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
        return {
            "plan_status": (data.get("plan_status") or "").lower(),
            "active":      bool(data.get("active", True)),
            "locale":      _widget_locale(data.get("locale")),
        }
    except Exception as e:
        log.warning(f"_fetch_inbet_member_status({member_uuid}) failed: {e}")
        return None


def _upsert_inbet_subscriber(chat_id: int, member_uuid: str,
                              plan_status: str, locale: str, active: bool,
                              source: str = "webhook") -> None:
    """Insert / update a subscriber row and write an audit entry on status change."""
    now_ts = int(time.time())
    try:
        with _db() as conn:
            prior = conn.execute(
                "SELECT plan_status, active FROM inbet_subscribers WHERE member_uuid = ?",
                (member_uuid,)
            ).fetchone()
            conn.execute("""
                INSERT INTO inbet_subscribers
                  (chat_id, member_uuid, plan_status, locale, status_checked_at,
                   linked_at, active, paused_by_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(member_uuid) DO UPDATE SET
                  chat_id           = excluded.chat_id,
                  plan_status       = excluded.plan_status,
                  locale            = excluded.locale,
                  status_checked_at = excluded.status_checked_at,
                  active            = excluded.active
            """, (chat_id, member_uuid, plan_status, locale, now_ts, now_ts, 1 if active else 0))
            if prior is not None and (
                prior["plan_status"] != plan_status or bool(prior["active"]) != active
            ):
                conn.execute("""
                    INSERT INTO inbet_status_audit
                      (member_uuid, old_status, new_status, old_active, new_active,
                       changed_at, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (member_uuid, prior["plan_status"], plan_status,
                      int(bool(prior["active"])), int(active), now_ts, source))
    except Exception as e:
        log.error(f"_upsert_inbet_subscriber failed: {e}")


@app.route("/api/inbet/member-status", methods=["POST"])
def r_inbet_member_status():
    """
    Webhook called by inbet's backend whenever a member's plan status changes.
    Body: {member_uuid, plan_status, active, locale?}

    Auth: shared secret in X-InBetIO-Sync-Secret header.
    """
    if not _check_inbet_sync_auth():
        return jsonify({"error": "unauthorized"}), 401
    try:
        payload = flask_request.get_json(force=True) or {}
        member_uuid = (payload.get("member_uuid") or "").strip()
        if not member_uuid:
            return jsonify({"error": "member_uuid required"}), 400
        plan_status = (payload.get("plan_status") or "").lower().strip()
        active      = bool(payload.get("active", True))
        locale      = _widget_locale(payload.get("locale"))

        with _db() as conn:
            prior = conn.execute(
                "SELECT chat_id, plan_status, active FROM inbet_subscribers "
                "WHERE member_uuid = ?",
                (member_uuid,)
            ).fetchone()
            if prior is None:
                # Member hasn't bound to the bot yet — nothing to do, just ack
                return jsonify({"ok": True, "linked": False, "member_uuid": member_uuid})

            _upsert_inbet_subscriber(
                chat_id     = prior["chat_id"],
                member_uuid = member_uuid,
                plan_status = plan_status,
                locale      = locale,
                active      = active,
                source      = "webhook",
            )
        return jsonify({
            "ok":          True,
            "linked":      True,
            "member_uuid": member_uuid,
            "eligible":    active and plan_status in INBET_ELIGIBLE_STATUSES,
        })
    except Exception as e:
        log.error(f"r_inbet_member_status error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/inbet/unlink", methods=["POST"])
def r_inbet_unlink():
    """
    Mark an inbet member as inactive (stop receiving Telegram alerts).
    Body: {member_uuid}
    Auth: X-InBetIO-Sync-Secret header.
    """
    if not _check_inbet_sync_auth():
        return jsonify({"error": "unauthorized"}), 401
    try:
        payload = flask_request.get_json(force=True) or {}
        member_uuid = (payload.get("member_uuid") or "").strip()
        if not member_uuid:
            return jsonify({"error": "member_uuid required"}), 400
        now_ts = int(time.time())
        with _db() as conn:
            prior = conn.execute(
                "SELECT plan_status, active FROM inbet_subscribers WHERE member_uuid = ?",
                (member_uuid,)
            ).fetchone()
            if prior is None:
                return jsonify({"ok": True, "found": False})
            conn.execute(
                "UPDATE inbet_subscribers SET active = 0, status_checked_at = ? "
                "WHERE member_uuid = ?",
                (now_ts, member_uuid)
            )
            conn.execute("""
                INSERT INTO inbet_status_audit
                  (member_uuid, old_status, new_status, old_active, new_active,
                   changed_at, source)
                VALUES (?, ?, ?, ?, 0, ?, 'unlink')
            """, (member_uuid, prior["plan_status"], prior["plan_status"],
                  int(bool(prior["active"])), now_ts))
        return jsonify({"ok": True, "found": True, "member_uuid": member_uuid})
    except Exception as e:
        log.error(f"r_inbet_unlink error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/inbet/status", methods=["GET"])
def r_inbet_status():
    """
    Read endpoint (admin auth) — current state for a given member_uuid or
    aggregate counters. Useful for support / debugging.
    """
    if not _check_admin_auth():
        return jsonify({"error": "unauthorized"}), 401
    member_uuid = (flask_request.args.get("member_uuid") or "").strip()
    try:
        with _db() as conn:
            if member_uuid:
                row = conn.execute(
                    "SELECT chat_id, member_uuid, plan_status, locale, active, "
                    "       paused_by_user, linked_at, status_checked_at "
                    "FROM inbet_subscribers WHERE member_uuid = ?",
                    (member_uuid,)
                ).fetchone()
                if not row:
                    return jsonify({"found": False, "member_uuid": member_uuid})
                return jsonify({"found": True, **dict(row)})
            # Aggregate
            counts = conn.execute("""
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active,
                  SUM(CASE WHEN plan_status = 'premium' THEN 1 ELSE 0 END) AS premium,
                  SUM(CASE WHEN plan_status = 'trial'   THEN 1 ELSE 0 END) AS trial,
                  SUM(CASE WHEN plan_status = 'demo'    THEN 1 ELSE 0 END) AS demo
                FROM inbet_subscribers
            """).fetchone()
        return jsonify(dict(counts))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Dedicated inbet Telegram bot — webhook handler ─────────────────────────
@app.route("/telegram/inbet/webhook", methods=["POST"])
def r_inbet_telegram_webhook():
    """
    Webhook for the DEDICATED inbet Telegram bot (separate from the WebPronos
    one). Set this URL with Telegram via setWebhook using INBET_BOT_TOKEN.

    We only handle:
      - /start <member_uuid> → bind chat_id ↔ member_uuid (calls inbet API for status)
      - /stop                → pause sends (paused_by_user=1, active=0)
      - /resume              → un-pause if previously paused
    """
    try:
        update = flask_request.get_json(force=True) or {}

        # ── callback_query (inline button tap) ──────────────────────────────
        # Used by the language picker. Format: callback_data="lang:<code>".
        cb = update.get("callback_query")
        if cb:
            cb_id   = cb.get("id")
            cb_data = (cb.get("data") or "").strip()
            cb_chat = ((cb.get("message") or {}).get("chat") or {}).get("id")
            if cb_id and cb_chat and cb_data.startswith("lang:"):
                new_lang = cb_data.split(":", 1)[1].strip().lower()
                if new_lang in ("en", "es", "pt-pt", "pt-br"):
                    now_ts = int(time.time())
                    with _db() as conn:
                        conn.execute(
                            "UPDATE inbet_subscribers SET locale = ?, "
                            "status_checked_at = ? WHERE chat_id = ?",
                            (new_lang, now_ts, cb_chat),
                        )
                    _answer_inbet_callback(cb_id, _INBET_LANG_CONFIRMED[new_lang])
                    _send_inbet_telegram(_INBET_LANG_CONFIRMED[new_lang], cb_chat)
                else:
                    _answer_inbet_callback(cb_id, "Invalid language")
            else:
                _answer_inbet_callback(cb_id or "", "")
            return jsonify({"ok": True})

        msg = update.get("message") or update.get("edited_message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        text = (msg.get("text") or "").strip()
        if not chat_id or not text:
            return jsonify({"ok": True})

        # /start [<payload>]
        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            payload = parts[1].strip() if len(parts) > 1 else ""
            if not payload:
                _send_inbet_telegram(
                    "⚠️ Use the 'Get Alerts' button inside your inbet account to link this Telegram chat.",
                    chat_id,
                )
                return jsonify({"ok": True})

            # payload is the member_uuid
            member_uuid = payload
            status = _fetch_inbet_member_status(member_uuid)
            if status is None:
                # inbet API unreachable / member not found — store with unknown status
                status = {"plan_status": "unknown", "active": False, "locale": "en"}
                _send_inbet_telegram(
                    "⚠️ Couldn't reach inbet to confirm your membership right now. "
                    "We've recorded your Telegram and will start alerts once "
                    "the status is confirmed.",
                    chat_id,
                )
            _upsert_inbet_subscriber(
                chat_id     = int(chat_id),
                member_uuid = member_uuid,
                plan_status = status["plan_status"],
                locale      = status["locale"],
                active      = status["active"],
                source      = "telegram_start",
            )
            if status["active"] and status["plan_status"] in INBET_ELIGIBLE_STATUSES:
                locale = status["locale"]
                replies = {
                    "en":    "✅ Linked! You'll get live picks during the World Cup.",
                    "es":    "✅ ¡Vinculado! Recibirás picks en directo durante el Mundial.",
                    "pt-pt": "✅ Ligado! Vais receber picks ao vivo durante o Mundial.",
                    "pt-br": "✅ Vinculado! Você vai receber picks ao vivo durante a Copa.",
                }
                _send_inbet_telegram(replies.get(locale, replies["en"]), chat_id)
            # Always offer the language picker after /start — locale from inbet
            # may be wrong (member preference can differ from account default).
            # User can confirm or override with a single tap.
            _send_inbet_telegram(
                _INBET_LANG_PROMPT.get(status["locale"], _INBET_LANG_PROMPT["en"]),
                chat_id,
                buttons=_INBET_LANG_BUTTONS,
            )
            return jsonify({"ok": True})

        # /lang [<code>]
        # Either bare /lang → show the 4-button picker, or /lang en|es|pt-pt|pt-br
        # → set directly. Both update inbet_subscribers.locale for this chat_id.
        if text.startswith("/lang"):
            parts = text.split(maxsplit=1)
            arg = parts[1].strip().lower() if len(parts) > 1 else ""

            # Look up current locale so the prompt itself is localised.
            cur_locale = "en"
            try:
                with _db() as conn:
                    row = conn.execute(
                        "SELECT locale FROM inbet_subscribers WHERE chat_id = ?",
                        (chat_id,),
                    ).fetchone()
                    if row and row["locale"]:
                        cur_locale = row["locale"]
            except Exception:
                pass

            if arg in ("en", "es", "pt-pt", "pt-br"):
                now_ts = int(time.time())
                with _db() as conn:
                    conn.execute(
                        "UPDATE inbet_subscribers SET locale = ?, "
                        "status_checked_at = ? WHERE chat_id = ?",
                        (arg, now_ts, chat_id),
                    )
                _send_inbet_telegram(_INBET_LANG_CONFIRMED[arg], chat_id)
            else:
                _send_inbet_telegram(
                    _INBET_LANG_PROMPT.get(cur_locale, _INBET_LANG_PROMPT["en"]),
                    chat_id,
                    buttons=_INBET_LANG_BUTTONS,
                )
            return jsonify({"ok": True})

        if text.startswith("/stop"):
            now_ts = int(time.time())
            with _db() as conn:
                conn.execute(
                    "UPDATE inbet_subscribers "
                    "SET active = 0, paused_by_user = 1, status_checked_at = ? "
                    "WHERE chat_id = ?",
                    (now_ts, chat_id)
                )
            _send_inbet_telegram(
                "🔕 Alerts paused. Send /resume to start receiving picks again.",
                chat_id,
            )
            return jsonify({"ok": True})

        if text.startswith("/resume"):
            now_ts = int(time.time())
            with _db() as conn:
                # Only resume if previously paused-by-user AND still eligible per inbet
                row = conn.execute(
                    "SELECT member_uuid, paused_by_user FROM inbet_subscribers WHERE chat_id = ?",
                    (chat_id,)
                ).fetchone()
                if row and row["paused_by_user"]:
                    status = _fetch_inbet_member_status(row["member_uuid"]) or {
                        "plan_status": "unknown", "active": False, "locale": "en"
                    }
                    eligible = status["active"] and status["plan_status"] in INBET_ELIGIBLE_STATUSES
                    conn.execute(
                        "UPDATE inbet_subscribers "
                        "SET active = ?, paused_by_user = 0, plan_status = ?, "
                        "    locale = ?, status_checked_at = ? "
                        "WHERE chat_id = ?",
                        (1 if eligible else 0, status["plan_status"],
                         status["locale"], now_ts, chat_id)
                    )
                    if eligible:
                        _send_inbet_telegram("🔔 Alerts resumed. You're back on the list.", chat_id)
                    else:
                        _send_inbet_telegram(
                            "ℹ️ Your inbet membership isn't active right now — "
                            "no alerts will be sent until it is.",
                            chat_id,
                        )
            return jsonify({"ok": True})

        # Unknown command — ignore silently
        return jsonify({"ok": True})

    except Exception as e:
        log.error(f"r_inbet_telegram_webhook error: {e}", exc_info=True)
        return jsonify({"ok": True})  # always 200 to Telegram


@app.route("/api/admin/send-daily-preview", methods=["POST"])
def r_admin_send_daily_preview():
    """
    Manually trigger the daily 12:00 Lisbon teaser message. Same content
    the cron job sends. Useful for sending today's preview if the daily
    cron missed it, or for previewing the wording on demand.

    Query params:
      preview=1  → return the message text without sending (dry-run)
    """
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        msg = _build_daily_preview_message()
        if not msg:
            return jsonify({"ok": False, "reason": "no eligible matches"}), 200
        if flask_request.args.get("preview") == "1":
            return jsonify({"ok": True, "preview": msg})
        _send_telegram(msg)
        return jsonify({"ok": True, "subscribers": len(_tg_subscribers())})
    except Exception as e:
        log.error(f"r_admin_send_daily_preview error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/send-admin-stats", methods=["POST"])
def r_admin_send_admin_stats():
    """
    Manually trigger the daily admin-stats Telegram message. Same content
    as the cron job that runs at 09:00 Lisbon. Useful for verifying the
    message format / contents on demand.
    """
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    if not TELEGRAM_ADMIN_CHAT_IDS:
        return jsonify({"error": "TELEGRAM_ADMIN_CHAT_IDS not configured"}), 400
    try:
        report = _tg_admin_stats()
        sent_to = []
        for cid in TELEGRAM_ADMIN_CHAT_IDS:
            try:
                _send_telegram(report, chat_id=cid)
                sent_to.append(cid)
            except Exception as e:
                log.error(f"send-admin-stats: send to {cid} failed: {e}")
        return jsonify({"ok": True, "sent_to": sent_to, "count": len(sent_to)})
    except Exception as e:
        log.error(f"r_admin_send_admin_stats error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


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
# Service-role key — bypasses RLS. Optional in this setup because
# `tips_archive` runs with RLS DISABLED (see SQL migration comment for
# rationale). If a service-role key happens to be set we'll use it for
# writes, but anon works fine too.
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_ANON)
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


def _build_meta_tags(match: dict, odds: dict | None, override: dict | None,
                      locale: str = "en") -> dict:
    """Generate SEO meta fields for a match page.

    Locale-aware: when locale is "pt-br" we use Brazilian vocabulary
    ("palpites" instead of "tips", "x" instead of "vs", localized league
    name). Other locales fall back to EN until translations are added.
    """
    home   = match.get("homeTeam", "Home")
    away   = match.get("awayTeam", "Away")
    tourn  = match.get("tournament", "")
    tourn_loc = _localized_league_name(tourn, locale)
    sep    = _matchup_separator(locale)
    matchup = f"{home}{sep}{away}"
    status = match.get("statusType", "notstarted")
    h_gls  = match.get("homeGoals", 0) or 0
    a_gls  = match.get("awayGoals", 0) or 0

    # SEO STABILITY RULE: title/description MUST stay identical from
    # creation (48h pre-kickoff) through the end of the live game.
    # Only `finished` status changes the meta — once the result is set,
    # the title/description rewrite themselves with the final score and
    # never change again. This protects Google's cached snippet.
    if override and override.get("meta_title"):
        title = override["meta_title"]
    elif locale == "pt-br":
        if status == "finished":
            title = f"{home} {h_gls}–{a_gls} {away} – Resultado Final & Análise xG | {SITE_NAME}"
        else:
            tourn_part = f" ({tourn_loc})" if tourn_loc else ""
            title = f"{matchup}{tourn_part} – Palpites & Previsões xG | {SITE_NAME}"
    elif locale == "pt-pt":
        # PT-PT keeps "Palpites" (also the main PT search keyword) but
        # uses " vs " (not " x "), "Análise" (not "Análise"), "Golos"
        # (not "Gols"), and the European Portuguese phrasings.
        if status == "finished":
            title = f"{home} {h_gls}–{a_gls} {away} – Resultado Final & Análise xG | {SITE_NAME}"
        else:
            tourn_part = f" ({tourn_loc})" if tourn_loc else ""
            title = f"{matchup}{tourn_part} – Palpites & Previsões xG | {SITE_NAME}"
    elif locale == "es":
        if status == "finished":
            title = f"{home} {h_gls}–{a_gls} {away} – Resultado Final & Análisis xG | {SITE_NAME}"
        else:
            tourn_part = f" ({tourn_loc})" if tourn_loc else ""
            title = f"{matchup}{tourn_part} – Pronósticos y Predicciones xG | {SITE_NAME}"
    else:
        # Default EN.
        if status == "finished":
            title = f"{home} {h_gls}–{a_gls} {away} – Final Result & xG Analysis | {SITE_NAME}"
        elif tourn:
            title = f"{home} vs {away} ({tourn}) – Picks & xG Predictions | {SITE_NAME}"
        else:
            title = f"{home} vs {away} – Picks & xG Predictions | {SITE_NAME}"

    if override and override.get("meta_description"):
        desc = override["meta_description"]
    elif locale == "pt-br":
        if status == "finished":
            desc = (
                f"Análise xG completa de {home} {h_gls}–{a_gls} {away}. "
                f"Gols Esperados, probabilidades de vitória e palpites de "
                f"valor gerados pelo algoritmo do WebPronos."
            )
        else:
            desc = (
                f"Previsões xG e palpites de valor para {matchup}"
                + (f" – {tourn_loc}" if tourn_loc else "")
                + f". Picks, probabilidades em tempo real e análise do jogo em {SITE_NAME}."
            )
    elif locale == "pt-pt":
        if status == "finished":
            desc = (
                f"Análise xG completa de {home} {h_gls}–{a_gls} {away}. "
                f"Golos Esperados, probabilidades de vitória e palpites de "
                f"valor gerados pelo algoritmo da WebPronos."
            )
        else:
            desc = (
                f"Previsões xG e palpites de valor para {matchup}"
                + (f" – {tourn_loc}" if tourn_loc else "")
                + f". Picks, probabilidades em tempo real e análise do jogo em {SITE_NAME}."
            )
    elif locale == "es":
        if status == "finished":
            desc = (
                f"Análisis xG completo de {home} {h_gls}–{a_gls} {away}. "
                f"Goles Esperados, probabilidades de victoria y pronósticos de "
                f"valor generados por el algoritmo de WebPronos."
            )
        else:
            desc = (
                f"Predicciones xG y pronósticos de valor para {matchup}"
                + (f" – {tourn_loc}" if tourn_loc else "")
                + f". Picks, probabilidades en tiempo real y análisis del partido en {SITE_NAME}."
            )
    else:
        if status == "finished":
            desc = (
                f"Full xG analysis for {home} {h_gls}–{a_gls} {away}. "
                f"Expected Goals, win probabilities and value bets generated by the WebPronos algorithm."
            )
        else:
            desc = (
                f"xG predictions and value bets for {home} vs {away}"
                + (f" – {tourn}" if tourn else "")
                + f". Picks, real-time probabilities and match analysis on {SITE_NAME}."
            )

    # NOTE: live odds are intentionally NOT appended to the description.
    # Odds drift constantly and would change the meta on every crawl,
    # defeating the SEO-stability rule above. Odds belong in the page
    # body, not in <meta>.

    # Prefer (in order):
    #   1. Manually-set override image from Supabase admin
    #   2. Home-team crest — recognizable to fans, gives match cards real identity
    #   3. Generic site default
    home = match.get("homeTeam", "")
    og_image = (
        (override or {}).get("og_image")
        or (_quick_logo(home) if home else None)
        or "https://webpronos.com/og-default.png"
    )

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


def _supabase_get_blog_post(slug: str, lang: str = "en") -> dict | None:
    """Fetch a single blog post from Supabase by (slug, lang).

    Locale fallback: tries the requested `lang` first; if no row exists
    for that lang, falls back to EN so non-translated posts remain
    discoverable. The returned dict is annotated with `_served_lang`
    and `_requested_lang` so callers (Flask renderer or SPA) can show
    a "translation not available" banner.
    """
    if not SUPABASE_ANON:
        return None
    try:
        import urllib.request as _ur, urllib.parse as _up
        # Lang fallback chain — requested first, then EN.
        chain = (lang,) if lang == "en" else (lang, "en")
        for try_lang in chain:
            url = (
                f"{SUPABASE_URL}/rest/v1/blog_posts"
                f"?slug=eq.{_up.quote(slug)}"
                f"&lang=eq.{try_lang}"
                f"&select=*"
                f"&limit=1"
            )
            req = _ur.Request(url, headers={
                "apikey":        SUPABASE_ANON,
                "Authorization": f"Bearer {SUPABASE_ANON}",
            })
            with _ur.urlopen(req, timeout=5) as r:
                rows = json.loads(r.read())
                if rows:
                    post = rows[0]
                    post["_served_lang"]    = try_lang
                    post["_requested_lang"] = lang
                    return post
        return None
    except Exception as e:
        log.warning(f"[prerender/blog] Supabase fetch failed for slug={slug}: {e}")
        return None


def _inject_blog_content(html: str, meta: dict, canonical: str, article_html: str,
                          published_at: str, author: str, jsonld: str,
                          locale: str = "en", en_slug: str | None = None) -> str:
    """
    Inject blog meta tags AND full article body into the SPA shell.
    Replaces the <div id="root">...</div> with the rendered article.

    `locale` drives <html lang>, og:locale and the hreflang alternate set.
    `en_slug` is the EN-canonical slug used to compute hreflangs (each
    locale gets its own translated blog slug via `_BLOG_SLUG_I18N`). If
    omitted we just emit hreflang for the current canonical (no alternates).
    """
    import re

    # ── 1. Inject head meta ──────────────────────────────────────────────────
    desc_content = meta["description"].replace('"', '&quot;')
    og_image     = meta.get("og_image", f"{SITE_URL}/og/default.png")
    title_escaped = meta["title"].replace('<', '&lt;').replace('>', '&gt;')
    html_lang    = _LOCALE_TO_HTML_LANG.get(locale, "en")
    og_locale    = html_lang.replace('-', '_')

    # Strip ALL existing dynamic meta (data-rh="true" tags from react-helmet)
    # They come as one long concatenated line — wipe the entire block
    html = re.sub(r'<meta\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<script\s+data-rh=["\']true["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)

    # Replace title tag (may have data-rh attribute)
    html = re.sub(r'<title[^>]*>[^<]*</title>', f'<title>{title_escaped}</title>', html)

    # Strip any remaining og/twitter/canonical/hreflang/jsonld tags
    html = re.sub(r'<meta\s+(?:property|name)=["\'](?:og:|twitter:)[^"\']*["\'][^>]*/?>',  '', html)
    html = re.sub(r'<meta\s+name=["\']description["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+rel=["\']canonical["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+rel=["\']alternate["\'][^>]*hreflang=[^>]*/?>', '', html)
    html = re.sub(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)

    # Overwrite <html lang="..."> in the SPA shell to the active locale.
    html = re.sub(r'<html\s+[^>]*lang=["\'][^"\']*["\']', f'<html lang="{html_lang}"', html, count=1)
    if '<html lang=' not in html[:200]:
        html = re.sub(r'<html(\s*[^>]*)>', f'<html lang="{html_lang}"\\1>', html, count=1)

    # Build hreflang alternates for each locale's translated blog slug.
    if en_slug:
        alt_tags = []
        for loc_code, prefix in _LOCALE_TO_PREFIX.items():
            slug = _localized_blog_slug(en_slug, loc_code)
            alt_tags.append(
                f'<link rel="alternate" hreflang="{loc_code}" '
                f'href="{SITE_URL}{prefix}/blog/{slug}">'
            )
        alt_tags.append(
            f'<link rel="alternate" hreflang="x-default" '
            f'href="{SITE_URL}/blog/{en_slug}">'
        )
        hreflang_block = "\n    ".join(alt_tags)
    else:
        hreflang_block = ""

    new_head = (
        f'<meta name="description" content="{desc_content}">\n'
        f'    <meta property="og:title" content="{title_escaped}">\n'
        f'    <meta property="og:description" content="{desc_content}">\n'
        f'    <meta property="og:image" content="{og_image}">\n'
        f'    <meta property="og:url" content="{canonical}">\n'
        f'    <meta property="og:type" content="article">\n'
        f'    <meta property="og:locale" content="{og_locale}">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{title_escaped}">\n'
        f'    <meta name="twitter:description" content="{desc_content}">\n'
        f'    <meta name="twitter:image" content="{og_image}">\n'
        f'    <link rel="canonical" href="{canonical}">\n'
        + (f'    {hreflang_block}\n' if hreflang_block else '')
        + f'    <script type="application/ld+json">{jsonld}</script>'
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
def prerender_blog(slug: str, locale: str = "en"):
    """
    SEO prerender for blog posts.
    Called by the Cloudflare Worker when a bot requests /blog/<slug>
    or /br/blog/<slug>. Returns the full article HTML — title, meta,
    JSON-LD BlogPosting + body text in the requested locale (with EN
    fallback if no translation exists).
    """
    try:
        post = _supabase_get_blog_post(slug, locale)

        if not post:
            # Return a minimal 404 — don't serve the SPA shell for missing posts
            lang_prefix = _LOCALE_TO_PREFIX.get(locale, "")
            return (
                f'<!DOCTYPE html><html><head><title>Not Found — WebPronos Blog</title>'
                f'<meta name="robots" content="noindex"></head>'
                f'<body><h1>Article not found</h1>'
                f'<p><a href="{SITE_URL}{lang_prefix}/blog">Back to Blog</a></p></body></html>',
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
        # Canonical points to the locale that ACTUALLY served the content
        # (so /br/blog/foo where only EN exists canonicalises to /blog/foo).
        served_lang   = post.get("_served_lang", "en")
        served_prefix = _LOCALE_TO_PREFIX.get(served_lang, "")
        canonical     = f"{SITE_URL}{served_prefix}/blog/{slug}"

        # ── Markdown → HTML ─────────────────────────────────────────────────
        article_html = _md_to_html(content_md) if content_md else "<p>Article coming soon.</p>"

        # ── JSON-LD BlogPosting + BreadcrumbList ────────────────────────────
        jsonld = json.dumps([
            {
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
            },
            _breadcrumb_jsonld([("WebPronos", "/"), ("Blog", "/blog"), (title, f"/blog/{slug}")]),
        ], ensure_ascii=False)

        meta = {"title": title, "description": description, "og_image": og_image}

        # ── Get SPA shell & inject ──────────────────────────────────────────
        # Resolve the EN-canonical slug so hreflang alternates can fan out
        # to every locale's translated slug. If the request slug IS the EN
        # canonical we just use it; otherwise reverse-lookup via the map.
        en_slug = _canonical_blog_slug(slug, served_lang)
        base_html = _get_base_html()
        if base_html:
            rendered = _inject_blog_content(
                base_html, meta, canonical, article_html, published_at,
                author, jsonld, locale=served_lang, en_slug=en_slug,
            )
            # Best-effort Last-Modified: parse published_at if available
            try:
                from email.utils import parsedate_to_datetime
                blog_lm = int(parsedate_to_datetime(published_at).timestamp()) if published_at else _BUILD_TIME_TS
            except Exception:
                blog_lm = _BUILD_TIME_TS
            return rendered, 200, {
                "Content-Type":  "text/html; charset=utf-8",
                "Cache-Control": "public, max-age=3600",   # cache 1h — articles don't change often
                "Last-Modified": _http_date(blog_lm),
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


def prerender_match(match_id: int, locale: str = "en"):
    """
    SEO prerender endpoint for match pages.
    Called by the Cloudflare Worker when a bot (Googlebot, Twitterbot, etc.) requests /match/:id.
    Returns fully-rendered HTML with meta tags + body content for the match.

    Locale is forwarded from `prerender_dispatch` (extracted from the
    /pt|/br|/es URL prefix). Only meta tags + canonical URL are localized
    here today — body content stays EN until a full i18n pass.

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

        # 6. Meta + canonical (locale-aware — BR users on /br/match/X get
        # BR-style title/description; EN canonical of the same page stays
        # the EN string).
        meta = _build_meta_tags(event, odds, override, locale=locale)
        slug = event.get("slug", "")
        lang_prefix = _LOCALE_TO_PREFIX.get(locale, "")
        match_path = f"/match/{match_id}/{slug}" if slug else f"/match/{match_id}"
        canonical = f"{SITE_URL}{lang_prefix}{match_path}"

        # 7. Body
        body_html = _render_match_body(event, odds, match_id)

        # 8. SportsEvent + BreadcrumbList JSON-LD for rich results
        try:
            from datetime import datetime as _dt, timezone as _tz
            ts = event.get("startTimestamp", 0)
            iso_start = _dt.fromtimestamp(ts, tz=_tz.utc).isoformat() if ts else ""
            home = event.get('homeTeam', '')
            away = event.get('awayTeam', '')
            tourn_raw      = event.get('tournament', '')
            # Collapse split-format tournaments ('Pro League, Conference
            # League Playoffs', 'Eredivisie, Championship Round', etc.)
            # into the canonical league name so the breadcrumb and the
            # /league/<slug> link both point to the unified page.
            tourn_canonical = _normalize_tournament_pretty(tourn_raw) or tourn_raw
            jsonld = json.dumps([
                {
                    "@context":  "https://schema.org",
                    "@type":     "SportsEvent",
                    "name":      f"{home} vs {away}",
                    "startDate": iso_start,
                    "sport":     "Soccer",
                    "url":       canonical,
                    "homeTeam":  {"@type": "SportsTeam", "name": home},
                    "awayTeam":  {"@type": "SportsTeam", "name": away},
                    "location":  {"@type": "Place", "name": tourn_canonical},
                },
                _breadcrumb_jsonld([
                    ("WebPronos", "/"),
                    *([(tourn_canonical, f"/league/{_slug(tourn_canonical)}")] if tourn_canonical else []),
                    (f"{home} vs {away}", canonical.replace(SITE_URL, "")),
                ]),
            ], ensure_ascii=False)
        except Exception:
            jsonld = ""

        # 9. Render — pass locale so <html lang>, og:locale and hreflang
        # tags reflect the requested locale. `stripped_path` is the EN-
        # canonical path (no /pt|/br|/es prefix) which hreflang siblings
        # are built from.
        html = _build_html_page(
            title         = meta["title"],
            description   = meta["description"],
            canonical     = canonical,
            body_html     = body_html,
            jsonld        = jsonld,
            og_image      = meta.get("og_image"),
            locale        = locale,
            stripped_path = match_path,
        )
        # Shorter cache on fallback so we retry live data sooner once Sofascore
        # comes back. Live path keeps the original 2-min cache.
        cache_max_age = 600 if used_fallback else 120
        last_mod_ts = _newest_pick_ts("t.match_id = ?", (match_id,))
        return html, 200, {
            "Content-Type":  "text/html; charset=utf-8",
            "Cache-Control": f"public, max-age={cache_max_age}",
            "Last-Modified": _http_date(last_mod_ts),
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
                      og_image: str | None = None,
                      locale: str = "en",
                      stripped_path: str | None = None) -> str:
    """
    Build a complete HTML page for prerender.
    Tries to inject into the Lovable SPA shell so visual hydration still works
    for users who somehow hit this endpoint. Falls back to standalone if needed.

    i18n params (default values keep backward compatibility with EN callers):
      - locale:        "en" | "pt-br" | "pt-pt" | "es"  → sets <html lang>
                        and the OG `og:locale` tag.
      - stripped_path: the canonical path WITHOUT the lang prefix (eg
                        "/about" not "/br/about"). Used to build the
                        hreflang tag set. If omitted, we attempt to
                        recover it by stripping a known prefix from the
                        canonical URL.
    """
    import re
    og_image = og_image or f"{SITE_URL}/og/default.png"
    title_escaped = title.replace('<', '&lt;').replace('>', '&gt;')
    desc_escaped  = description.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
    html_lang = _LOCALE_TO_HTML_LANG.get(locale, "en")

    # Resolve hreflang path: prefer explicit `stripped_path` from caller;
    # otherwise infer by removing the locale's prefix from the canonical.
    if stripped_path is None:
        prefix = _LOCALE_TO_PREFIX.get(locale, "")
        try:
            from urllib.parse import urlparse
            cpath = urlparse(canonical).path or "/"
        except Exception:
            cpath = "/"
        if prefix and cpath.startswith(prefix):
            stripped_path = cpath[len(prefix):] or "/"
        else:
            stripped_path = cpath
    hreflang_block = _hreflang_tags(stripped_path)

    base_html = _get_base_html()
    if not base_html:
        # Standalone fallback
        return f"""<!DOCTYPE html>
<html lang="{html_lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="robots" content="index,follow">
<title>{title_escaped}</title>
<meta name="description" content="{desc_escaped}">
<meta property="og:type" content="website">
<meta property="og:locale" content="{html_lang.replace('-', '_')}">
<meta property="og:title" content="{title_escaped}">
<meta property="og:description" content="{desc_escaped}">
<meta property="og:image" content="{og_image}">
<meta property="og:url" content="{canonical}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{title_escaped}">
<meta name="twitter:description" content="{desc_escaped}">
<meta name="twitter:image" content="{og_image}">
<link rel="canonical" href="{canonical}">
{hreflang_block}
{('<script type="application/ld+json">' + jsonld + '</script>') if jsonld else ''}
{_PRERENDER_CSS}
</head>
<body>
<div class="pr-wrap">{body_html}</div>
</body>
</html>"""

    # Inject into Lovable SPA shell
    html = base_html

    # Set <html lang="..."> to the correct locale (overwrites Lovable's default).
    html = re.sub(r'<html\s+[^>]*lang=["\'][^"\']*["\']', f'<html lang="{html_lang}"', html, count=1)
    if '<html lang=' not in html[:200]:
        # Lovable shell might not have a lang attribute at all — inject it.
        html = re.sub(r'<html(\s*[^>]*)>', f'<html lang="{html_lang}"\\1>', html, count=1)

    # Strip react-helmet dynamic tags
    html = re.sub(r'<meta\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+data-rh=["\']true["\'][^>]*/?>',  '', html)
    html = re.sub(r'<script\s+data-rh=["\']true["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)
    # Replace title (may have data-rh)
    html = re.sub(r'<title[^>]*>[^<]*</title>', f'<title>{title_escaped}</title>', html)
    # Strip existing og/twitter/canonical/hreflang/jsonld
    html = re.sub(r'<meta\s+(?:property|name)=["\'](?:og:|twitter:)[^"\']*["\'][^>]*/?>', '', html)
    html = re.sub(r'<meta\s+name=["\']description["\'][^>]*/?>', '', html)
    html = re.sub(r'<link\s+rel=["\']canonical["\'][^>]*/?>',  '', html)
    html = re.sub(r'<link\s+rel=["\']alternate["\'][^>]*hreflang=[^>]*/?>', '', html)
    html = re.sub(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>.*?</script>', '', html, flags=re.DOTALL)

    new_head = (
        f'<meta name="description" content="{desc_escaped}">\n'
        f'    <meta property="og:title" content="{title_escaped}">\n'
        f'    <meta property="og:description" content="{desc_escaped}">\n'
        f'    <meta property="og:image" content="{og_image}">\n'
        f'    <meta property="og:url" content="{canonical}">\n'
        f'    <meta property="og:type" content="website">\n'
        f'    <meta property="og:locale" content="{html_lang.replace("-", "_")}">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{title_escaped}">\n'
        f'    <meta name="twitter:description" content="{desc_escaped}">\n'
        f'    <meta name="twitter:image" content="{og_image}">\n'
        f'    <link rel="canonical" href="{canonical}">\n'
        f'    {hreflang_block}\n'
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


def _render_pr_footer(locale: str = "en") -> str:
    """Locale-aware footer for SSR pages. Each link is built from the EN
    canonical path through `_localized_slug` so the user lands on the
    locale-specific URL (/br/historico not /br/history)."""
    prefix = _LOCALE_TO_PREFIX.get(locale, "")
    def _href(en_path: str) -> str:
        return f"{SITE_URL}{prefix}{_localized_slug(en_path, locale)}"
    return f"""
    <div class="pr-footer">
      <a href="{_href("/")}">{_t_site(locale, "nav_home")}</a> ·
      <a href="{_href("/blog")}">{_t_site(locale, "nav_blog")}</a> ·
      <a href="{_href("/history")}">{_t_site(locale, "nav_history")}</a> ·
      <a href="{_href("/tomorrow")}">{_t_site(locale, "nav_tomorrow")}</a> ·
      <a href="{_href("/about")}">{_t_site(locale, "nav_about")}</a>
      <p style="margin-top:1rem;font-size:.75rem">{_t_site(locale, "footer_disclaimer")}</p>
    </div>
    """


# ── Blog listing ──────────────────────────────────────────────────────────
def _supabase_get_all_blog_posts(limit: int = 50, lang: str = "en") -> list:
    """Fetch all published blog posts for a specific lang (lightweight).

    Posts without a translation in the requested lang are NOT shown in
    that locale's listing — the CMS treats each (slug, lang) pair as a
    distinct row. If you want a partially-translated blog to show all
    EN posts plus the translated ones with priority, you'd UNION the
    two queries deduplicating by slug; not needed for v1.
    """
    if not SUPABASE_ANON:
        return []
    try:
        import urllib.request as _ur
        url = (
            f"{SUPABASE_URL}/rest/v1/blog_posts"
            f"?select=*"
            f"&lang=eq.{lang}"
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


def _render_blog_listing(locale: str = "en") -> str:
    """SSR for /blog — list of all articles in the requested locale."""
    posts = _supabase_get_all_blog_posts(limit=50, lang=locale)
    lang_prefix = _LOCALE_TO_PREFIX.get(locale, "")
    site_name = _t_site(locale, "breadcrumb_root")

    # i18n strings for the blog index chrome. Falls back to EN values
    # if locale is missing from the inline map (eg PT-PT, ES not done yet).
    blog_chrome = {
        "en":    {"h1": "WebPronos Blog", "breadcrumb": "Blog",
                  "lead": "In-depth guides on xG, live betting strategy, value detection and how AI improves football predictions. Updated regularly.",
                  "empty": "No articles published yet. Check back soon.",
                  "title": "Blog — Live Betting Strategy, xG & AI Predictions | WebPronos",
                  "description": "Free in-depth guides on xG, live betting timing, value detection and edge calculation. Learn how the WebPronos AI model finds positive-EV bets.",
                  "schema_description": "Guides on xG, live betting strategy and AI football predictions."},
        "pt-br": {"h1": "Blog WebPronos", "breadcrumb": "Blog",
                  "lead": "Guias completos sobre xG, estratégia de apostas ao vivo, detecção de valor e como a IA melhora as previsões de futebol. Atualizado regularmente.",
                  "empty": "Nenhum artigo publicado ainda. Volte em breve.",
                  "title": "Blog — Estratégia de Apostas Ao Vivo, xG & Previsões com IA | WebPronos",
                  "description": "Guias gratuitos e completos sobre xG, timing de apostas ao vivo, detecção de valor e cálculo de edge. Aprenda como o modelo de IA do WebPronos encontra apostas de EV positivo.",
                  "schema_description": "Guias sobre xG, estratégia de apostas ao vivo e previsões de futebol com IA."},
    }.get(locale) or {"h1": "WebPronos Blog", "breadcrumb": "Blog",
                       "lead": "In-depth guides on xG, live betting strategy, value detection and how AI improves football predictions. Updated regularly.",
                       "empty": "No articles published yet. Check back soon.",
                       "title": "Blog — Live Betting Strategy, xG & AI Predictions | WebPronos",
                       "description": "Free in-depth guides on xG, live betting timing, value detection and edge calculation. Learn how the WebPronos AI model finds positive-EV bets.",
                       "schema_description": "Guides on xG, live betting strategy and AI football predictions."}

    if not posts:
        articles_html = f'<p>{blog_chrome["empty"]}</p>'
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
              <a href="{SITE_URL}{lang_prefix}/blog/{slug}" itemprop="url">
                <h3 itemprop="headline">{title}</h3>
                <p itemprop="description">{excerpt}</p>
                <p class="pr-meta" style="margin-top:.5rem">
                  <span itemprop="author">{author}</span> · <time itemprop="datePublished" datetime="{pub}">{pub}</time>
                </p>
              </a>
            </article>""")
        articles_html = "\n".join(items)

    canonical = f"{SITE_URL}{lang_prefix}/blog"
    body = f"""
    <nav class="pr-nav">
      <a href="{SITE_URL}{lang_prefix}/">{site_name}</a> › {blog_chrome["breadcrumb"]}
    </nav>
    <h1 class="pr-h1">{blog_chrome["h1"]}</h1>
    <p class="pr-lead">{blog_chrome["lead"]}</p>
    <div class="pr-grid">
      {articles_html}
    </div>
    {_render_pr_footer(locale)}
    """

    # JSON-LD: ItemList of articles
    items_jsonld = []
    for i, p in enumerate(posts[:20]):
        items_jsonld.append({
            "@type": "ListItem",
            "position": i + 1,
            "url": f"{SITE_URL}{lang_prefix}/blog/{p.get('slug','')}",
            "name": p.get("title", ""),
        })
    jsonld = json.dumps([
        {
            "@context": "https://schema.org",
            "@type":    "Blog",
            "name":     blog_chrome["h1"],
            "url":      canonical,
            "description": blog_chrome["schema_description"],
            "inLanguage":  _LOCALE_TO_HTML_LANG.get(locale, "en"),
            "blogPost": items_jsonld,
        },
        _breadcrumb_jsonld([(site_name, f"{lang_prefix}/"), (blog_chrome["breadcrumb"], f"{lang_prefix}/blog")]),
    ], ensure_ascii=False)

    return _build_html_page(
        title         = blog_chrome["title"],
        description   = blog_chrome["description"],
        canonical     = canonical,
        body_html     = body,
        jsonld        = jsonld,
        locale        = locale,
        stripped_path = "/blog",
    )


# ── History ───────────────────────────────────────────────────────────────
def _render_history(locale: str = "en") -> str:
    """SSR for /history — last settled picks with results (locale-aware chrome)."""
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

            # Localise market + label for the current locale (Under → Menos de,
            # Draw → Empate, Handicap → Hándicap on ES, etc.). Team-name labels
            # like "Barcelona +0.5" stay as-is (proper nouns).
            mkt_loc = _xlate_market(r["market"], locale)
            lbl_loc = _xlate_pick_label(r["label"], locale)
            table_rows.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td class="pr-meta">{league}</td>
                <td><a href="{_match_url(r['match_id'], r['home_team'], r['away_team'])}" style="color:#fff">{match}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{mkt_loc} — {lbl_loc}</td>
                <td class="pr-meta">@{r['odd_entry']:.2f}</td>
                <td>{badge}</td>
                <td class="{profit_class}">{'+' if profit > 0 else ''}{profit:.0f}€</td>
              </tr>
            """)

        prefix = _LOCALE_TO_PREFIX.get(locale, "")
        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}{prefix}/">{_t_site(locale, 'breadcrumb_root')}</a> › {_t_site(locale, 'history_breadcrumb')}
        </nav>
        <h1 class="pr-h1">{_t_site(locale, 'history_h1')}</h1>
        <p class="pr-lead">{_t_site(locale, 'history_lead')}</p>

        <div style="margin:1.5rem 0">
          <span class="pr-stat"><strong>{total}</strong></span>
          <span class="pr-stat"><strong>{wins} / {losses}</strong></span>
          <span class="pr-stat"><strong>{winrate:.1f}%</strong></span>
          <span class="pr-stat"><strong>{'+' if pnl > 0 else ''}{pnl:.0f}€</strong></span>
          <span class="pr-stat"><strong>{'+' if roi > 0 else ''}{roi:.1f}%</strong></span>
        </div>

        <table class="pr-table">
          <tbody>
            {''.join(table_rows) if table_rows else '<tr><td colspan="8" style="text-align:center;padding:2rem">—</td></tr>'}
          </tbody>
        </table>

        {_render_pr_footer()}
        """

        jsonld = json.dumps([
            {
                "@context": "https://schema.org",
                "@type":    "Dataset",
                "name":     "WebPronos prediction track record",
                "description": f"Public history of {total} AI-generated football predictions with results and P&L.",
                "url":      f"{SITE_URL}{prefix}{_localized_slug('/history', locale)}",
                "creator":  {"@type": "Organization", "name": "WebPronos", "url": SITE_URL},
                # Required by Google's Dataset structured data spec — flagged in
                # Search Console as "Missing field 'license'". Points to the site
                # terms which describe permitted reuse of the prediction history.
                "license":  f"{SITE_URL}{prefix}{_localized_slug('/terms', locale)}",
                "isAccessibleForFree": True,
                "keywords": ["football predictions", "AI tips", "track record", "betting analytics", "xG model"],
            },
            _breadcrumb_jsonld([(_t_site(locale, 'breadcrumb_root'), "/"),
                                 (_t_site(locale, 'history_breadcrumb'), _localized_slug('/history', locale))]),
        ], ensure_ascii=False)

        canonical = f"{SITE_URL}{prefix}{_localized_slug('/history', locale)}"
        return _build_html_page(
            title         = _t_site(locale, "history_title"),
            description   = _t_site(locale, "history_desc"),
            canonical     = canonical,
            body_html     = body,
            jsonld        = jsonld,
            locale        = locale,
            stripped_path = "/history",
        )
    except Exception as e:
        log.exception(f"[prerender/history] Error: {e}")
        return _build_html_page(
            title         = _t_site(locale, "history_title"),
            description   = _t_site(locale, "history_desc"),
            canonical     = f"{SITE_URL}{_LOCALE_TO_PREFIX.get(locale,'')}{_localized_slug('/history', locale)}",
            body_html     = f'<h1>{_t_site(locale, "history_h1")}</h1><p>Loading… {_render_pr_footer()}</p>',
            locale        = locale,
            stripped_path = "/history",
        )


# ── Tomorrow's matches ────────────────────────────────────────────────────
def _render_tomorrow(locale: str = "en") -> str:
    """SSR for /tomorrow — list of matches scheduled for tomorrow (locale-aware chrome)."""
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

        # Group by tournament (with localized name)
        prefix = _LOCALE_TO_PREFIX.get(locale, "")
        sep    = _matchup_separator(locale)
        groups: dict = {}
        for m in matches:
            league = m.get("tournament") or m.get("country") or "Other"
            groups.setdefault(league, []).append(m)

        # Render groups
        groups_html = []
        for league, ms in sorted(groups.items()):
            league_loc = _localized_league_name(league, locale)
            rows = []
            for m in sorted(ms, key=lambda x: x.get("startTimestamp", 0)):
                ts = m.get("startTimestamp", 0)
                kickoff = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M") if ts else "—"
                rows.append(f"""
                  <div class="pr-row">
                    <a href="{_match_url(m['id'], m['homeTeam'], m['awayTeam'])}">{m['homeTeam']} <span class="pr-meta">{sep.strip()}</span> {m['awayTeam']}</a>
                    <span class="pr-meta">{kickoff} UTC</span>
                  </div>""")
            groups_html.append(f"""
            <div class="pr-card">
              <h3 class="pr-h3" style="margin-top:0">{league_loc}</h3>
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
          <a href="{SITE_URL}{prefix}/">{_t_site(locale, 'breadcrumb_root')}</a> › {_t_site(locale, 'tomorrow_breadcrumb')}
        </nav>
        <h1 class="pr-h1">{_t_site(locale, 'tomorrow_h1')}</h1>
        <p class="pr-lead">{_t_site(locale, 'tomorrow_lead')}</p>

        <div class="pr-grid">
          {''.join(groups_html) if groups_html else '<p>—</p>'}
        </div>

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
        jsonld = json.dumps([
            {
                "@context": "https://schema.org",
                "@type":    "ItemList",
                "name":     f"Tomorrow's football matches — {date_human}",
                "numberOfItems": len(matches),
                "itemListElement": [
                    {"@type": "ListItem", "position": i+1, "item": e}
                    for i, e in enumerate(events_jsonld)
                ],
            },
            _breadcrumb_jsonld([(_t_site(locale, 'breadcrumb_root'), "/"),
                                 (_t_site(locale, 'tomorrow_breadcrumb'), _localized_slug('/tomorrow', locale))]),
        ], ensure_ascii=False)

        canonical = f"{SITE_URL}{prefix}{_localized_slug('/tomorrow', locale)}"
        return _build_html_page(
            title         = _t_site(locale, "tomorrow_title"),
            description   = _t_site(locale, "tomorrow_desc"),
            canonical     = canonical,
            body_html     = body,
            jsonld        = jsonld,
            locale        = locale,
            stripped_path = "/tomorrow",
        )
    except Exception as e:
        log.exception(f"[prerender/tomorrow] Error: {e}")
        return _build_html_page(
            title         = _t_site(locale, "tomorrow_title"),
            description   = _t_site(locale, "tomorrow_desc"),
            canonical     = f"{SITE_URL}{_LOCALE_TO_PREFIX.get(locale,'')}{_localized_slug('/tomorrow', locale)}",
            body_html     = f'<h1>{_t_site(locale, "tomorrow_h1")}</h1><p>Loading…</p>{_render_pr_footer()}',
            locale        = locale,
            stripped_path = "/tomorrow",
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

    # Build internal-link URLs so the match page links out to the league
    # and to each team page (boosts crawl depth and PageRank flow).
    home_url  = f"{SITE_URL}/team/{_slug(home)}"   if home  else ""
    away_url  = f"{SITE_URL}/team/{_slug(away)}"   if away  else ""
    tourn_url = f"{SITE_URL}/league/{_slug(tourn)}" if tourn else ""

    # Make the league line clickable; teams clickable in the "About" copy
    league_meta = (f'{country} · <a href="{tourn_url}" style="color:#22d3ee">{tourn}</a>'
                   if tourn else f'{country}')

    body = f"""
    <nav class="pr-nav">
      <a href="{SITE_URL}/">WebPronos</a> ›
      {f'<a href="{tourn_url}" style="color:#22d3ee">{tourn}</a> › ' if tourn else ''}{home} vs {away}
    </nav>

    <p class="pr-meta">{league_meta}</p>
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
    <p><a href="{home_url}" style="color:#22d3ee">{home}</a> take on <a href="{away_url}" style="color:#22d3ee">{away}</a> in the <a href="{tourn_url}" style="color:#22d3ee">{tourn}</a>. WebPronos publishes live in-play tips for this fixture — every prediction is generated after kickoff, when the model can react to the actual flow of the game. Open the live page during kickoff to see real-time win probabilities, value bets and the full xG shot map.</p>

    <h2 class="pr-h2">Related pages</h2>
    <div class="pr-grid">
      <a href="{home_url}" class="pr-card" style="text-decoration:none;color:inherit;display:block">
        <div class="pr-meta">Team page</div>
        <div style="font-weight:700;color:#fff">{home}</div>
        <div class="pr-meta">Recent form, xG averages & full pick history</div>
      </a>
      <a href="{away_url}" class="pr-card" style="text-decoration:none;color:inherit;display:block">
        <div class="pr-meta">Team page</div>
        <div style="font-weight:700;color:#fff">{away}</div>
        <div class="pr-meta">Recent form, xG averages & full pick history</div>
      </a>
      <a href="{tourn_url}" class="pr-card" style="text-decoration:none;color:inherit;display:block">
        <div class="pr-meta">Competition page</div>
        <div style="font-weight:700;color:#fff">{tourn}</div>
        <div class="pr-meta">Upcoming fixtures & league-wide track record</div>
      </a>
    </div>

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


def _render_team(slug: str, locale: str = "en") -> tuple:
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
            mkt_loc = _xlate_market(p["market"], locale)
            lbl_loc = _xlate_pick_label(p["label"], locale)
            rows_html.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td><a href="{_match_url(p['match_id'], p['home_team'], p['away_team'])}" style="color:#fff">{p['home_team']} vs {p['away_team']}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{mkt_loc} — {lbl_loc}</td>
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

        # Title in the active locale; team name itself stays as-is (proper noun).
        if locale == "pt-br":
            title_str = f"{name} — Palpites de IA & Análise xG | WebPronos"
        elif locale == "pt-pt":
            title_str = f"{name} — Tips de IA & Análise xG | WebPronos"
        elif locale == "es":
            title_str = f"{name} — Pronósticos de IA & Análisis xG | WebPronos"
        else:
            title_str = f"{name} — AI Football Predictions & xG Analysis | WebPronos"

        loc_prefix = _LOCALE_TO_PREFIX.get(locale, "")
        canonical = f"{SITE_URL}{loc_prefix}{_localized_dynamic_path('/team/' + slug, locale)}"
        html = _build_html_page(
            title         = title_str,
            description   = meta_desc,
            canonical     = canonical,
            body_html     = body,
            jsonld        = jsonld,
            og_image      = logo_url or None,
            locale        = locale,
            stripped_path = f"/team/{slug}",
        )
        _seo_cache_put(cache_key, html)
        last_mod_ts = _newest_pick_ts("(g.home_team = ? OR g.away_team = ?)", (name, name))
        return html, 200, {"Content-Type": "text/html; charset=utf-8",
                            "Cache-Control": "public, max-age=600",
                            "Last-Modified": _http_date(last_mod_ts),
                            "X-Prerender": "webpronos-team"}
    except Exception as e:
        log.exception(f"[prerender/team] Error for slug={slug}: {e}")
        return _render_passthrough(f"/team/{slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}


def _render_league(slug: str, locale: str = "en") -> tuple:
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
            mkt_loc = _xlate_market(p["market"], locale)
            lbl_loc = _xlate_pick_label(p["label"], locale)
            pick_rows.append(f"""
              <tr>
                <td class="pr-meta">{date_str}</td>
                <td><a href="{_match_url(p['match_id'], p['home_team'], p['away_team'])}" style="color:#fff">{match}</a></td>
                <td class="pr-meta">{score}</td>
                <td class="pr-meta">{mkt_loc} — {lbl_loc}</td>
                <td class="pr-meta">@{(p['odd_entry'] or 0):.2f}</td>
                <td>{badge}</td>
              </tr>""")

        logo_url = _league_logo(name) or ""
        logo_img = (f'<img src="{logo_url}" alt="{name} logo" '
                    f'style="width:64px;height:64px;vertical-align:middle;margin-right:.75rem">'
                    if logo_url else "")

        # ── Build rich SEO intro paragraph from real DB data ─────────────
        perf = _league_performance(variants, recent_days=30, locale=locale)
        intro_bits = []
        country_phrase = f" ({country})" if country else ""

        if perf["matches_tracked"] >= 5:
            # We have enough data to be specific
            intro_lead = (
                f"WebPronos publishes AI-generated value picks for {name}{country_phrase} "
                f"using a live xG model. We've tracked {perf['matches_tracked']} "
                f"completed matches"
            )
            facts = []
            if perf["goals_per_match"] is not None:
                facts.append(f"averaging {perf['goals_per_match']} goals per game")
            if perf["avg_xg_per_match"] is not None:
                facts.append(f"and {perf['avg_xg_per_match']} total xG")
            if facts:
                intro_lead += " — " + " ".join(facts)
            intro_lead += "."
            intro_bits.append(intro_lead)

            tr = perf.get("track_record") or {}
            if tr and tr.get("settled", 0) >= 5:
                roi_str = f"{tr['roi']:+.1f}% ROI" if tr['roi'] != 0 else "break-even ROI"
                pnl_str = f"€{tr['pnl']:+.0f}" if tr['pnl'] else "€0"
                intro_bits.append(
                    f"Our settled track record in this competition: {tr['settled']} picks, "
                    f"{tr['wins']} winners and {tr['losses']} losers at {tr['avg_odds']:.2f} avg odds, "
                    f"closing at {pnl_str} ({roi_str})."
                )

            if perf["top_attack"]:
                ta = perf["top_attack"]
                top_str = ", ".join(
                    f'<a href="{SITE_URL}/team/{_slug(t["team"])}" style="color:#22d3ee">{t["team"]}</a> ({t["avg_goals_for"]} per game)'
                    for t in ta[:3]
                )
                intro_bits.append(f"Top attacks in our sample: {top_str}.")

            if perf["recent_form_text"]:
                # Locale-aware prefix for the "Recent activity:" wrapper
                if locale == "pt-br":
                    label = "Atividade recente"
                elif locale == "pt-pt":
                    label = "Atividade recente"
                elif locale == "es":
                    label = "Actividad reciente"
                else:
                    label = "Recent activity"
                intro_bits.append(f"{label}: {perf['recent_form_text']}.")
        elif perf["matches_tracked"] > 0:
            intro_bits.append(
                f"AI-driven live and pre-match football tips for {name}{country_phrase}. "
                f"We've tracked {perf['matches_tracked']} completed match"
                + ("es" if perf['matches_tracked'] != 1 else "")
                + " in this competition so far — more data accrues with every fixture."
            )
        else:
            intro_bits.append(
                f"AI-driven live and pre-match football tips for {name}{country_phrase}. "
                f"Coverage starts the moment the first monitored fixture kicks off."
            )

        intro_bits.append(
            f"Browse the {len(upcoming)} upcoming fixture"
            + ("s" if len(upcoming) != 1 else "")
            + f" we'll be live-modeling, plus the full audit of every pick we've issued in {name} below."
        )
        intro_html = " ".join(intro_bits)

        # ── Stat strip (track-record snapshot)
        tr = perf.get("track_record") or {}
        if tr and tr.get("settled", 0) > 0:
            stat_strip = (
                f'<span class="pr-stat">Picks tracked: <strong>{tr["settled"]}</strong></span>'
                f'<span class="pr-stat">Wins: <strong>{tr["wins"]}</strong></span>'
                f'<span class="pr-stat">Losses: <strong>{tr["losses"]}</strong></span>'
                + (f'<span class="pr-stat">Push: <strong>{tr["voids"]}</strong></span>' if tr.get("voids") else '')
                + f'<span class="pr-stat">Avg odds: <strong>@{tr["avg_odds"]:.2f}</strong></span>'
                + f'<span class="pr-stat">P&amp;L: <strong>{"+" if tr["pnl"] > 0 else ""}{tr["pnl"]:.0f}€</strong></span>'
            )
        else:
            stat_strip = ""

        # ── League stat strip (on-pitch averages, separate from pick track record)
        if perf["matches_tracked"] >= 3:
            pitch_bits = [f'<span class="pr-stat">Matches tracked: <strong>{perf["matches_tracked"]}</strong></span>']
            if perf["goals_per_match"] is not None:
                pitch_bits.append(f'<span class="pr-stat">Goals/match: <strong>{perf["goals_per_match"]}</strong></span>')
            if perf["avg_xg_per_match"] is not None:
                pitch_bits.append(f'<span class="pr-stat">xG/match: <strong>{perf["avg_xg_per_match"]}</strong></span>')
            pitch_strip = "".join(pitch_bits)
        else:
            pitch_strip = ""

        name_loc = _localized_league_name(name, locale)
        prefix   = _LOCALE_TO_PREFIX.get(locale, "")
        # H1 + nav use the localized competition name (eg "Campeonato Belga"
        # for /br/liga/pro-league) so the page heading aligns with how
        # PT-BR/PT-PT/ES audiences search for the league.
        if locale == "pt-br":
            h1_suffix = "Palpites de IA, Análise xG & Histórico"
        elif locale == "pt-pt":
            h1_suffix = "Tips de IA, Análise xG & Histórico"
        elif locale == "es":
            h1_suffix = "Pronósticos de IA, Análisis xG & Historial"
        else:
            h1_suffix = "AI Football Tips, xG Analysis & Track Record"
        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}{prefix}/">{_t_site(locale, 'breadcrumb_root')}</a> › {name_loc}
        </nav>
        <h1 class="pr-h1">{logo_img}{name_loc} — {h1_suffix}</h1>
        <p class="pr-lead">{intro_html}</p>

        {f'<div style="margin:1.5rem 0">{pitch_strip}</div>' if pitch_strip else ''}
        {f'<div style="margin:1rem 0 1.5rem">{stat_strip}</div>' if stat_strip else ''}

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

        jsonld = json.dumps([
            {
                "@context": "https://schema.org",
                "@type": "SportsLeague",
                "name": name,
                "sport": "Soccer",
                "url": f"{SITE_URL}/league/{slug}",
                **({"location": {"@type": "Country", "name": country}} if country else {}),
            },
            _breadcrumb_jsonld([("WebPronos", "/"), (name, f"/league/{slug}")]),
        ], ensure_ascii=False)

        # Meta description: prioritise the strongest factual signal we have
        # (xG/goal averages or track-record), fall back to generic if data thin.
        if perf["matches_tracked"] >= 5 and perf["goals_per_match"] is not None:
            meta_desc = (
                f"AI football tips & xG analysis for {name}. {perf['matches_tracked']} matches tracked, "
                f"averaging {perf['goals_per_match']} goals per game. "
                f"{len(upcoming)} upcoming fixtures with live picks and full audit trail."
            )
        elif tr and tr.get("settled", 0) >= 5:
            meta_desc = (
                f"AI football tips for {name}. {tr['settled']} settled picks at {tr['avg_odds']:.2f} avg odds, "
                f"running {'+' if tr['pnl'] > 0 else ''}{tr['pnl']:.0f}€ P&L ({tr['roi']:+.1f}% ROI). "
                f"{len(upcoming)} upcoming fixtures."
            )
        else:
            meta_desc = (
                f"AI football tips and predictions for {name}{country_phrase}. "
                f"{len(upcoming)} upcoming fixtures with live xG picks and full audit trail."
            )

        # Locale-aware title — uses localized league name + locale-native
        # phrasing for the descriptor suffix.
        if locale == "pt-br":
            title_str = f"{name_loc} — Palpites de IA, Análise xG & Previsões | WebPronos"
        elif locale == "pt-pt":
            title_str = f"{name_loc} — Tips de IA, Análise xG & Previsões | WebPronos"
        elif locale == "es":
            title_str = f"{name_loc} — Pronósticos de IA, Análisis xG & Predicciones | WebPronos"
        else:
            title_str = f"{name} — AI Football Tips, xG Analysis & Predictions | WebPronos"

        # Canonical uses the localized dynamic prefix (/liga vs /league)
        # so the canonical signal aligns with the user-visible URL.
        canonical = f"{SITE_URL}{prefix}{_localized_dynamic_path('/league/' + slug, locale)}"
        html = _build_html_page(
            title         = title_str,
            description   = meta_desc,
            canonical     = canonical,
            body_html     = body,
            jsonld        = jsonld,
            og_image      = _league_logo(name) or None,
            locale        = locale,
            stripped_path = f"/league/{slug}",
        )
        _seo_cache_put(cache_key, html)
        # Last-Modified = most recent pick in any tournament variant of this league
        variants_for_lm = _league_variants_for(name)
        ph_lm = ",".join("?" * len(variants_for_lm))
        last_mod_ts = _newest_pick_ts(f"g.tournament IN ({ph_lm})", tuple(variants_for_lm))
        return html, 200, {"Content-Type": "text/html; charset=utf-8",
                            "Cache-Control": "public, max-age=600",
                            "Last-Modified": _http_date(last_mod_ts),
                            "X-Prerender": "webpronos-league"}
    except Exception as e:
        log.exception(f"[prerender/league] Error for slug={slug}: {e}")
        return _render_passthrough(f"/league/{slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}


def _render_tips_market(market_slug: str, locale: str = "en") -> tuple:
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

        jsonld = json.dumps([
            {
                "@context": "https://schema.org",
                "@type": "CollectionPage",
                "name": f"{pretty_name} football tips — WebPronos",
                "url": f"{SITE_URL}/tips/{market_slug}",
                "about": pretty_name,
            },
            _breadcrumb_jsonld([("WebPronos", "/"), ("Tips", "/"), (pretty_name, f"/tips/{market_slug}")]),
        ], ensure_ascii=False)

        if locale == "pt-br":
            title_str = f"{pretty_name} — Palpites de IA Ao Vivo | WebPronos"
        elif locale == "pt-pt":
            title_str = f"{pretty_name} — Tips de IA Em Direto | WebPronos"
        elif locale == "es":
            title_str = f"{pretty_name} — Pronósticos de IA En Vivo | WebPronos"
        else:
            title_str = f"{pretty_name} Tips — Live AI Predictions | WebPronos"

        loc_prefix = _LOCALE_TO_PREFIX.get(locale, "")
        canonical = f"{SITE_URL}{loc_prefix}{_localized_dynamic_path('/tips/' + market_slug, locale)}"
        html = _build_html_page(
            title         = title_str,
            description   = f"All {pretty_name} football tips logged by the WebPronos AI model with entry odds, results and ROI. {total_settled} settled picks tracked.",
            canonical     = canonical,
            body_html     = body,
            jsonld        = jsonld,
            locale        = locale,
            stripped_path = f"/tips/{market_slug}",
        )
        _seo_cache_put(cache_key, html)
        # Last-Modified = newest pick matching ANY of this market's LIKE patterns
        like_where = " OR ".join("t.market LIKE ?" for _ in like_patterns)
        last_mod_ts = _newest_pick_ts(like_where, tuple(like_patterns))
        return html, 200, {"Content-Type": "text/html; charset=utf-8",
                            "Cache-Control": "public, max-age=600",
                            "Last-Modified": _http_date(last_mod_ts),
                            "X-Prerender": "webpronos-tips-market"}
    except Exception as e:
        log.exception(f"[prerender/tips-market] Error for {market_slug}: {e}")
        return _render_passthrough(f"/tips/{market_slug}"), 200, {"Content-Type": "text/html; charset=utf-8"}


def _render_today(locale: str = "en") -> str:
    """SSR for /today — today's monitored fixtures (locale-aware chrome)."""
    cache_key = f"page:today:{locale}"
    cached = _seo_cache_get(cache_key)
    if cached:
        return cached
    try:
        from datetime import datetime as _dt, timezone as _tz
        date_str = _dt.now(_tz.utc).strftime("%Y-%m-%d")
        cached_day = _upcoming_cache.get(date_str)
        matches = cached_day["matches"] if cached_day else []

        prefix = _LOCALE_TO_PREFIX.get(locale, "")
        sep    = _matchup_separator(locale)
        rows = []
        for m in matches:
            ko_ts = m.get("startTimestamp") or 0
            ko = _dt.fromtimestamp(ko_ts, tz=_tz.utc).strftime("%H:%M UTC") if ko_ts else "—"
            tourn_loc = _localized_league_name(m.get('tournament',''), locale)
            rows.append(f"""
              <tr>
                <td class="pr-meta">{ko}</td>
                <td class="pr-meta">{tourn_loc}</td>
                <td><a href="{_match_url(m['id'], m['homeTeam'], m['awayTeam'])}" style="color:#fff">{m['homeTeam']}{sep}{m['awayTeam']}</a></td>
              </tr>""")

        body = f"""
        <nav class="pr-nav">
          <a href="{SITE_URL}{prefix}/">{_t_site(locale, 'breadcrumb_root')}</a> › {_t_site(locale, 'today_breadcrumb')}
        </nav>
        <h1 class="pr-h1">{_t_site(locale, 'today_h1')}</h1>
        <p class="pr-lead">{_t_site(locale, 'today_lead')}</p>

        <table class="pr-table">
          <tbody>
            {''.join(rows) if rows else '<tr><td colspan="3" style="text-align:center;padding:2rem">—</td></tr>'}
          </tbody>
        </table>

        {_render_pr_footer()}
        """

        jsonld = json.dumps([
            {
                "@context": "https://schema.org",
                "@type": "ItemList",
                "name": f"Football matches today — {date_str}",
                "numberOfItems": len(matches),
                "itemListElement": [{
                    "@type": "ListItem",
                    "position": i + 1,
                    "url": _match_url(m['id'], m['homeTeam'], m['awayTeam']),
                    "name": f"{m['homeTeam']} vs {m['awayTeam']}",
                } for i, m in enumerate(matches[:50])],
            },
            _breadcrumb_jsonld([(_t_site(locale, 'breadcrumb_root'), "/"),
                                (_t_site(locale, 'today_breadcrumb'), _localized_slug('/today', locale))]),
        ], ensure_ascii=False)

        canonical = f"{SITE_URL}{prefix}{_localized_slug('/today', locale)}"
        html = _build_html_page(
            title         = _t_site(locale, "today_title"),
            description   = _t_site(locale, "today_desc"),
            canonical     = canonical,
            body_html     = body,
            jsonld        = jsonld,
            locale        = locale,
            stripped_path = "/today",
        )
        _seo_cache_put(cache_key, html)
        return html
    except Exception as e:
        log.exception(f"[prerender/today] Error: {e}")
        return _build_html_page(
            title         = _t_site(locale, "today_title"),
            description   = _t_site(locale, "today_desc"),
            canonical     = f"{SITE_URL}{_LOCALE_TO_PREFIX.get(locale,'')}{_localized_slug('/today', locale)}",
            body_html     = f'<h1>{_t_site(locale, "today_h1")}</h1><p>Loading…</p>{_render_pr_footer()}',
            locale        = locale,
            stripped_path = "/today",
        )


# ── Homepage ──────────────────────────────────────────────────────────────
def _render_homepage(locale: str = "en") -> str:
    """SSR for / — builds a semantically correct homepage with exactly ONE H1.

    Locale-aware: chrome (title, H1/H2, body copy, CTA) is translated via
    _SITE_I18N. Internal links are routed to the locale's prefix so the
    user stays in their language graph (eg /br/history not /history).
    """
    prefix = _LOCALE_TO_PREFIX.get(locale, "")
    history_href = f"{prefix}{_localized_slug('/history', locale)}"
    body_html = f"""
<section style="max-width:800px;margin:0 auto;padding:2rem 1rem">
  <h1 style="font-size:2.2rem;font-weight:900;color:#fff;line-height:1.2;margin:0 0 .75rem">
    {_t_site(locale, "home_h1")}
  </h1>
  <p style="color:#94a3b8;font-size:1.1rem;margin:0 0 2rem">
    {_t_site(locale, "home_lead")}
  </p>
  <a href="{history_href}" style="display:inline-block;background:#22d3ee;color:#0f172a;font-weight:700;padding:.75rem 1.5rem;border-radius:.5rem;text-decoration:none;margin-bottom:2rem;transition:background 200ms">
    {_t_site(locale, "home_cta_history")}
  </a>

  <h2 style="font-size:1.4rem;font-weight:700;color:#e2e8f0;margin:2rem 0 .5rem">
    {_t_site(locale, "home_h2_how")}
  </h2>
  <p style="color:#94a3b8;margin:0 0 1rem">
    {_t_site(locale, "home_p_how")}
  </p>

  <h2 style="font-size:1.4rem;font-weight:700;color:#e2e8f0;margin:2rem 0 .5rem">
    {_t_site(locale, "home_h2_why")}
  </h2>
  <p style="color:#94a3b8;margin:0 0 1rem">
    {_t_site(locale, "home_p_why")}
  </p>

  <h2 style="font-size:1.4rem;font-weight:700;color:#e2e8f0;margin:2rem 0 .5rem">
    {_t_site(locale, "home_h2_track")}
  </h2>
  <p style="color:#94a3b8;margin:0 0 2rem">
    {_t_site(locale, "home_p_track_pre")}
    <a href="{history_href}" style="color:#22d3ee;text-decoration:none">{_t_site(locale, "home_p_track_link")}</a>
    {_t_site(locale, "home_p_track_post")}
  </p>
</section>
{_render_pr_footer(locale)}"""

    canonical = f"{SITE_URL}{prefix}/"
    return _build_html_page(
        title         = _t_site(locale, "home_title"),
        description   = _t_site(locale, "home_desc"),
        canonical     = canonical,
        body_html     = body_html,
        locale        = locale,
        stripped_path = "/",
        jsonld=json.dumps({
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": "WebPronos",
            "url":  canonical,
            "description": _t_site(locale, "home_desc"),
            "inLanguage":  _LOCALE_TO_HTML_LANG.get(locale, "en"),
            "potentialAction": {
                "@type": "SearchAction",
                "target": f"{SITE_URL}{history_href}",
                "query-input": "required name=search_term_string"
            }
        }, ensure_ascii=False),
    )


# ── Static-content pages (about, terms, etc.) ─────────────────────────────
def _http_date(ts: int | float | None) -> str:
    """
    Format a unix timestamp as an RFC 1123 / HTTP-date string.
    Example: 'Mon, 11 May 2026 14:30:00 GMT'.

    Used to set the `Last-Modified` response header on prerendered pages
    so Googlebot can issue conditional `If-Modified-Since` requests on
    subsequent crawls — saves crawl budget on unchanged pages.

    Falls back to "now" if the timestamp is missing or invalid.
    """
    try:
        from email.utils import formatdate
        return formatdate(timeval=float(ts) if ts else None, usegmt=True)
    except Exception:
        from email.utils import formatdate
        return formatdate(usegmt=True)


# Build time: epoch when this Python process started. Used as a stable
# Last-Modified for pages that don't have per-row data freshness (homepage,
# legal pages, blog index, etc). Resets on each deploy, which is exactly
# what we want — Googlebot will refresh those pages then but not between.
_BUILD_TIME_TS = int(time.time())


def _newest_pick_ts(where_clause: str = "1=1", params: tuple = ()) -> int:
    """
    Return the wall_ts of the newest tip matching the optional WHERE
    clause (joined with `tips t JOIN games g ON g.id = t.match_id`).
    Falls back to _BUILD_TIME_TS if no rows match. Cheap query — fully
    indexed on tips(wall_ts) / tips(match_id).
    """
    try:
        with _db() as conn:
            row = conn.execute(
                f"SELECT MAX(t.wall_ts) AS m FROM tips t "
                f"JOIN games g ON g.id = t.match_id WHERE {where_clause}",
                params,
            ).fetchone()
        return int(row["m"]) if row and row["m"] else _BUILD_TIME_TS
    except Exception:
        return _BUILD_TIME_TS


def _breadcrumb_jsonld(items: list[tuple[str, str]]) -> dict:
    """
    Build a Schema.org BreadcrumbList JSON-LD dict.

    `items` is a list of (name, url_path) tuples in order from root to leaf.
    Example:
        _breadcrumb_jsonld([("WebPronos", "/"), ("Leagues", "/leagues"), ("Premier League", "/league/premier-league")])

    Use by combining with the page's primary JSON-LD into an array:
        jsonld = json.dumps([primary_dict, _breadcrumb_jsonld([...])])

    Returning a dict (not a JSON string) keeps the caller in control of
    serialization — most pages embed multiple JSON-LD types as an array.
    """
    return {
        "@context": "https://schema.org",
        "@type":    "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": i + 1, "name": name,
             "item": f"{SITE_URL}{path}" if not path.startswith("http") else path}
            for i, (name, path) in enumerate(items)
        ],
    }


def _render_passthrough(canonical_path: str = "/", locale: str = "en") -> str:
    """For unknown/fallback paths — serve Lovable shell unchanged.

    The Lovable SPA detects the URL's lang prefix client-side and renders
    in the matching locale via react-i18next. For SEO bots this means
    they see an empty SPA shell, which is suboptimal — covered routes
    (homepage, today, history, blog, team, league, tips, match, static
    pages) all have dedicated prerender functions and don't hit this.
    """
    base_html = _get_base_html()
    if base_html:
        return base_html
    lang_prefix = _LOCALE_TO_PREFIX.get(locale, "")
    return _build_html_page(
        title="WebPronos — Live Football Predictions",
        description="AI-powered football tips across 25 competitions.",
        canonical=f"{SITE_URL}{lang_prefix}{canonical_path}",
        body_html=f"<h1>WebPronos</h1>{_render_pr_footer(locale)}",
        locale=locale,
        stripped_path=canonical_path,
    )


# ════════════════════════════════════════════════════════════════════════
#  i18n PRIMITIVES — site-wide locale handling for SEO prerender
# ════════════════════════════════════════════════════════════════════════
# Background:
#   The Lovable SPA is the primary surface for real users — Lovable handles
#   its own i18n via react-i18next + a language switcher. The Flask backend
#   only renders HTML for: (a) search-engine bots via /prerender, and
#   (b) the affiliate interstitial /go/bet (already has _GO_BET_COPY).
#
#   For BR launch we add a single new dict (_SITE_I18N) and a tiny set of
#   helpers to:
#     - extract a /pt|/br|/es prefix from any URL path
#     - look up nav/footer/common strings per locale
#     - emit hreflang link tags + <html lang> in the prerendered HTML
#
#   Adding PT-PT or ES later is purely additive: append the locale to
#   _SITE_I18N + _STATIC_PAGES_I18N + _LOCALE_TO_PREFIX. No other code
#   changes needed.

# URL prefix → locale code (locale used as key in _SITE_I18N etc.)
_LANG_PREFIX_TO_LOCALE = {
    "br": "pt-br",
    "pt": "pt-pt",
    "es": "es",
}

# Inverse: locale → URL prefix segment (EN has no prefix). Used to build
# locale-specific canonical URLs and hreflang link tags.
_LOCALE_TO_PREFIX = {
    "en":    "",
    "pt-br": "/br",
    "pt-pt": "/pt",
    "es":    "/es",
}

# Locale → <html lang="..."> attribute value (BCP-47 short form).
_LOCALE_TO_HTML_LANG = {
    "en":    "en",
    "pt-br": "pt-BR",
    "pt-pt": "pt-PT",
    "es":    "es",
}

# Site-wide UI strings (nav, footer, generic labels) per locale. EN is the
# canonical source — any key missing in a non-EN locale falls back to EN.
# Only EN + PT-BR are populated for the BR launch; PT-PT and ES blocks
# can be added incrementally without code changes.
_SITE_I18N = {
    "en": {
        # Nav / footer
        "nav_home":          "Home",
        "nav_blog":          "Blog",
        "nav_history":       "History",
        "nav_tomorrow":      "Tomorrow",
        "nav_about":         "About",
        "footer_disclaimer": "WebPronos provides statistical predictions for informational purposes only. 18+ — please gamble responsibly.",
        "breadcrumb_root":   "WebPronos",

        # Homepage chrome (used by _render_homepage)
        "home_title":        "WebPronos — Live Football Tips & In-Play Predictions",
        "home_desc":         "AI-powered in-play football tips updated every 15 seconds. xG-based picks across 25+ competitions with full track record.",
        "home_h1":           "Live Football Tips &amp; In-Play Predictions — Updated Every 15 Seconds",
        "home_lead":         "AI-powered football picks across 25+ competitions. Our algorithm tracks xG, momentum shifts and live odds to fire tips during the match — not before it.",
        "home_cta_history":  "View Historical Results →",
        "home_h2_how":       "How it works",
        "home_p_how":        "Every match in our database is monitored minute-by-minute. When xG diverges from the scoreline and live odds offer value, the algorithm fires a pick — visible instantly on the live dashboard.",
        "home_h2_why":       "Why in-play betting?",
        "home_p_why":        "Pre-match odds are heavily efficient. In-play markets move fast and are often mis-priced for 2–3 minutes after a key event — that's the window our model exploits.",
        "home_h2_track":     "Track record",
        "home_p_track_pre":  "All picks are logged with entry time, odds and result. Check the",
        "home_p_track_link": "historical performance page",
        "home_p_track_post": "for full transparency.",

        # /today renderer chrome
        "today_title":       "Today's Football Tips & Live Predictions | WebPronos",
        "today_desc":        "All monitored football matches scheduled for today with live AI tips, xG model and value detection.",
        "today_h1":          "Today's Football Matches — Live AI Tips",
        "today_lead":        "Every monitored match scheduled for today. Click any fixture to follow the live xG model and AI tips in real time.",
        "today_breadcrumb":  "Today",

        # /tomorrow renderer chrome
        "tomorrow_title":    "Tomorrow's Football Matches — Live AI Predictions | WebPronos",
        "tomorrow_desc":     "Tomorrow's monitored football matches with our AI value detection ready for kickoff.",
        "tomorrow_h1":       "Tomorrow's Football Matches",
        "tomorrow_lead":     "Every match scheduled for tomorrow across the competitions we cover. Click any fixture to open its dedicated live page — once kickoff happens, the AI model starts publishing in-play tips, value bets and updated odds.",
        "tomorrow_breadcrumb": "Tomorrow",

        # /history renderer chrome
        "history_title":     "History — Track Record of AI Football Predictions | WebPronos",
        "history_desc":      "Full historical track record of every settled WebPronos AI prediction. Audit ROI, win rate and recent picks.",
        "history_h1":        "Historical Performance — Track Record",
        "history_lead":      "Every settled prediction by the WebPronos AI model is published openly. Audit the full track record below — no cherry-picking.",
        "history_breadcrumb": "History",
    },
    "pt-br": {
        # Nav / footer
        "nav_home":          "Início",
        "nav_blog":          "Blog",
        "nav_history":       "Histórico",
        "nav_tomorrow":      "Amanhã",
        "nav_about":         "Sobre",
        "footer_disclaimer": "WebPronos fornece previsões estatísticas apenas para fins informativos. 18+ — aposte com responsabilidade.",
        "breadcrumb_root":   "WebPronos",

        # Homepage chrome
        "home_title":        "WebPronos — Tips de Futebol Ao Vivo & Previsões In-Play",
        "home_desc":         "Tips de futebol in-play com IA atualizados a cada 15 segundos. Picks baseados em xG em mais de 25 competições com histórico completo.",
        "home_h1":           "Tips de Futebol Ao Vivo &amp; Previsões In-Play — Atualizados a Cada 15 Segundos",
        "home_lead":         "Picks de futebol com inteligência artificial em mais de 25 competições. Nosso algoritmo acompanha xG, mudanças de momentum e odds ao vivo para disparar tips durante o jogo — não antes dele.",
        "home_cta_history":  "Ver Resultados Históricos →",
        "home_h2_how":       "Como funciona",
        "home_p_how":        "Cada jogo em nossa base de dados é monitorado minuto a minuto. Quando o xG diverge do placar e as odds ao vivo oferecem valor, o algoritmo dispara um pick — visível instantaneamente no painel ao vivo.",
        "home_h2_why":       "Por que apostar in-play?",
        "home_p_why":        "As odds pré-jogo são altamente eficientes. Os mercados ao vivo se movem rápido e ficam mal precificados por 2 a 3 minutos após um evento chave — essa é a janela que nosso modelo explora.",
        "home_h2_track":     "Histórico de resultados",
        "home_p_track_pre":  "Todos os picks são registrados com horário de entrada, odd e resultado. Confira a",
        "home_p_track_link": "página de desempenho histórico",
        "home_p_track_post": "para total transparência.",

        # /today renderer chrome (BR)
        "today_title":       "Jogos de Hoje — Palpites Ao Vivo & Previsões com IA | WebPronos",
        "today_desc":        "Todos os jogos de futebol monitorados marcados para hoje com palpites in-play, modelo xG e detecção de valor.",
        "today_h1":          "Jogos de Futebol de Hoje — Palpites Ao Vivo com IA",
        "today_lead":        "Todos os jogos monitorados marcados para hoje. Clique em qualquer partida para acompanhar o modelo xG ao vivo e os palpites do algoritmo em tempo real.",
        "today_breadcrumb":  "Hoje",

        # /tomorrow renderer chrome (BR)
        "tomorrow_title":    "Jogos de Amanhã — Previsões Ao Vivo com IA | WebPronos",
        "tomorrow_desc":     "Os jogos de futebol monitorados marcados para amanhã, prontos para a nossa detecção de valor com IA quando começarem.",
        "tomorrow_h1":       "Jogos de Futebol de Amanhã",
        "tomorrow_lead":     "Todas as partidas marcadas para amanhã nas competições que cobrimos. Clique em qualquer jogo para abrir a página ao vivo dedicada — assim que começar, o algoritmo passa a publicar palpites in-play, apostas de valor e odds atualizadas.",
        "tomorrow_breadcrumb": "Amanhã",

        # /history renderer chrome (BR)
        "history_title":     "Histórico — Track Record dos Palpites de IA | WebPronos",
        "history_desc":      "Histórico completo de cada palpite liquidado pelo modelo de IA do WebPronos. Audite ROI, win rate e palpites recentes.",
        "history_h1":        "Desempenho Histórico — Track Record",
        "history_lead":      "Cada palpite liquidado pelo modelo de IA do WebPronos é publicado abertamente. Audite o track record completo abaixo — sem cherry-picking.",
        "history_breadcrumb": "Histórico",
    },
    "pt-pt": {
        # Nav / footer
        "nav_home":          "Início",
        "nav_blog":          "Blog",
        "nav_history":       "Histórico",
        "nav_tomorrow":      "Amanhã",
        "nav_about":         "Sobre",
        "footer_disclaimer": "A WebPronos fornece previsões estatísticas apenas para fins informativos. 18+ — aposte com responsabilidade.",
        "breadcrumb_root":   "WebPronos",

        # Homepage chrome — PT-PT uses "em direto" not "ao vivo", "monitorizado"
        # not "monitorado", "marcador" not "placar", and prefers definite
        # articles ("o nosso algoritmo", "as nossas tips").
        "home_title":        "WebPronos — Palpites de Futebol Em Direto & Previsões In-Play",
        "home_desc":         "Palpites de futebol in-play com IA atualizados a cada 15 segundos. Picks baseados em xG em mais de 25 competições com histórico completo.",
        "home_h1":           "Palpites de Futebol Em Direto &amp; Previsões In-Play — Atualizados a Cada 15 Segundos",
        "home_lead":         "Picks de futebol com inteligência artificial em mais de 25 competições. O nosso algoritmo acompanha o xG, as mudanças de momentum e as odds em direto para disparar palpites durante o jogo — não antes dele.",
        "home_cta_history":  "Ver Resultados Históricos →",
        "home_h2_how":       "Como funciona",
        "home_p_how":        "Cada jogo na nossa base de dados é monitorizado minuto a minuto. Quando o xG diverge do marcador e as odds em direto oferecem valor, o algoritmo dispara um pick — visível instantaneamente no painel em direto.",
        "home_h2_why":       "Porquê apostar in-play?",
        "home_p_why":        "As odds pré-jogo são muito eficientes. Os mercados em direto movem-se depressa e ficam mal cotados durante 2 a 3 minutos após um evento-chave — é essa a janela que o nosso modelo explora.",
        "home_h2_track":     "Histórico de resultados",
        "home_p_track_pre":  "Todos os picks são registados com hora de entrada, odd e resultado. Consulta a",
        "home_p_track_link": "página de desempenho histórico",
        "home_p_track_post": "para total transparência.",

        # /today renderer chrome (PT-PT)
        "today_title":       "Jogos de Hoje — Palpites Em Direto & Previsões com IA | WebPronos",
        "today_desc":        "Todos os jogos de futebol monitorizados marcados para hoje com palpites in-play, modelo xG e deteção de valor.",
        "today_h1":          "Jogos de Futebol de Hoje — Palpites Em Direto com IA",
        "today_lead":        "Todos os jogos monitorizados marcados para hoje. Clica em qualquer partida para acompanhar o modelo xG em direto e as tips do algoritmo em tempo real.",
        "today_breadcrumb":  "Hoje",

        # /tomorrow renderer chrome (PT-PT)
        "tomorrow_title":    "Jogos de Amanhã — Previsões Em Direto com IA | WebPronos",
        "tomorrow_desc":     "Os jogos de futebol monitorizados marcados para amanhã, prontos para a nossa deteção de valor com IA quando começarem.",
        "tomorrow_h1":       "Jogos de Futebol de Amanhã",
        "tomorrow_lead":     "Todas as partidas marcadas para amanhã nas competições que cobrimos. Clica em qualquer jogo para abrir a página em direto dedicada — assim que começar, o algoritmo passa a publicar tips in-play, apostas de valor e odds atualizadas.",
        "tomorrow_breadcrumb": "Amanhã",

        # /history renderer chrome (PT-PT)
        "history_title":     "Histórico — Track Record das Tips de IA | WebPronos",
        "history_desc":      "Histórico completo de cada tip liquidada pelo modelo de IA da WebPronos. Audita ROI, win rate e tips recentes.",
        "history_h1":        "Desempenho Histórico — Track Record",
        "history_lead":      "Cada tip liquidada pelo modelo de IA da WebPronos é publicada abertamente. Audita o track record completo abaixo — sem cherry-picking.",
        "history_breadcrumb": "Histórico",
    },
    "es": {
        # Nav / footer
        "nav_home":          "Inicio",
        "nav_blog":          "Blog",
        "nav_history":       "Historial",
        "nav_tomorrow":      "Mañana",
        "nav_about":         "Sobre",
        "footer_disclaimer": "WebPronos ofrece predicciones estadísticas únicamente con fines informativos. +18 — apuesta con responsabilidad.",
        "breadcrumb_root":   "WebPronos",

        # Homepage chrome — Spanish (Spain + LatAm). "Pronósticos" or "tips"
        # both work; we use both interchangeably for SEO coverage.
        "home_title":        "WebPronos — Pronósticos de Fútbol En Vivo y Predicciones In-Play",
        "home_desc":         "Pronósticos de fútbol in-play con IA actualizados cada 15 segundos. Picks basados en xG en más de 25 competiciones con histórico completo.",
        "home_h1":           "Pronósticos de Fútbol En Vivo &amp; Predicciones In-Play — Actualizados Cada 15 Segundos",
        "home_lead":         "Picks de fútbol con inteligencia artificial en más de 25 competiciones. Nuestro algoritmo sigue el xG, los cambios de momentum y las cuotas en vivo para disparar tips durante el partido — no antes.",
        "home_cta_history":  "Ver Resultados Históricos →",
        "home_h2_how":       "Cómo funciona",
        "home_p_how":        "Cada partido en nuestra base de datos se monitoriza minuto a minuto. Cuando el xG diverge del marcador y las cuotas en vivo ofrecen valor, el algoritmo dispara un pick — visible al instante en el panel en vivo.",
        "home_h2_why":       "¿Por qué apostar in-play?",
        "home_p_why":        "Las cuotas previas al partido son muy eficientes. Los mercados en vivo se mueven rápido y quedan mal cotizados durante 2 a 3 minutos después de un evento clave — esa es la ventana que explota nuestro modelo.",
        "home_h2_track":     "Histórico de resultados",
        "home_p_track_pre":  "Todos los picks se registran con hora de entrada, cuota y resultado. Consulta la",
        "home_p_track_link": "página de rendimiento histórico",
        "home_p_track_post": "para total transparencia.",

        # /today renderer chrome (ES)
        "today_title":       "Partidos de Hoy — Pronósticos En Vivo con IA | WebPronos",
        "today_desc":        "Todos los partidos de fútbol monitorizados programados para hoy con tips en vivo, modelo xG y detección de valor.",
        "today_h1":          "Partidos de Fútbol de Hoy — Tips En Vivo con IA",
        "today_lead":        "Todos los partidos monitorizados programados para hoy. Pulsa en cualquier partido para seguir el modelo xG en vivo y los tips del algoritmo en tiempo real.",
        "today_breadcrumb":  "Hoy",

        # /tomorrow renderer chrome (ES)
        "tomorrow_title":    "Partidos de Mañana — Predicciones En Vivo con IA | WebPronos",
        "tomorrow_desc":     "Los partidos de fútbol monitorizados programados para mañana, listos para nuestra detección de valor con IA cuando empiecen.",
        "tomorrow_h1":       "Partidos de Fútbol de Mañana",
        "tomorrow_lead":     "Todos los partidos programados para mañana en las competiciones que cubrimos. Pulsa en cualquier partido para abrir su página en vivo dedicada — en cuanto empiece, el algoritmo publica tips in-play, apuestas de valor y cuotas actualizadas.",
        "tomorrow_breadcrumb": "Mañana",

        # /history renderer chrome (ES)
        "history_title":     "Historial — Track Record de Predicciones de IA | WebPronos",
        "history_desc":      "Historial completo de cada predicción liquidada por el modelo de IA de WebPronos. Audita el ROI, el win rate y los picks recientes.",
        "history_h1":        "Rendimiento Histórico — Track Record",
        "history_lead":      "Cada predicción liquidada por el modelo de IA de WebPronos se publica abiertamente. Audita el track record completo abajo — sin cherry-picking.",
        "history_breadcrumb": "Historial",
    },
}


def _t_site(locale: str, key: str) -> str:
    """Lookup a site-wide string with EN fallback. Mirror of `_t` but for
    SEO/page chrome (separate namespace from the inbet widget's WIDGET_COPY).
    """
    bucket = _SITE_I18N.get(locale) or _SITE_I18N["en"]
    return bucket.get(key) or _SITE_I18N["en"].get(key, key)


# Per-locale slug translations for static routes. EN canonical paths map
# to the locale-specific URL slug. EN is implicit (no entry needed).
#
# Lookup pattern:
#   _localized_slug("/about", "pt-br")  → "/sobre"
#   _localized_slug("/about", "en")     → "/about"    (passthrough)
#   _canonical_slug("/sobre", "pt-br")  → "/about"    (reverse — for routing)
#   _canonical_slug("/about", "pt-br")  → "/about"    (no-op if already EN)
#
# To add a new BR static slug: append one entry. To add PT-PT/ES later:
# add a top-level bucket. Sitemap, footer nav, hreflang and prerender
# routing all read from this single source of truth.
_STATIC_SLUG_I18N = {
    "pt-br": {
        "/about":                "/sobre",
        "/terms":                "/termos",
        "/privacy":              "/privacidade",
        "/responsible-gambling": "/jogo-responsavel",
        "/today":                "/hoje",
        "/tomorrow":             "/amanha",
        "/history":              "/historico",
    },
    "pt-pt": {
        # PT-PT slugs are semantically identical to BR (both Portuguese)
        # but live under /pt/ instead of /br/. The CONTENT differs (PT
        # uses "em direto" vs BR "ao vivo", "marcador" vs "placar", etc).
        "/about":                "/sobre",
        "/terms":                "/termos",
        "/privacy":              "/privacidade",
        "/responsible-gambling": "/jogo-responsavel",
        "/today":                "/hoje",
        "/tomorrow":             "/amanha",
        "/history":              "/historico",
    },
    "es": {
        "/about":                "/sobre",
        "/terms":                "/terminos",
        "/privacy":              "/privacidad",
        "/responsible-gambling": "/juego-responsable",
        "/today":                "/hoy",
        "/tomorrow":             "/manana",
        "/history":              "/historial",
    },
}


# Per-locale slug translations for individual BLOG POSTS. Each EN slug
# maps to the localized slug used in the {lang}/blog/<slug> URL. Used by
# the sitemap (hreflang alternates between en/br/pt/es per post) and by
# any future EN→localized redirect logic in Flask.
#
# Worker has its own copy in BR_BLOG_SLUG_REDIRECTS — keep them in sync.
# Append when new posts are translated.
_BLOG_SLUG_I18N = {
    "live-betting-vs-pre-match-why-timing-matters": {
        "pt-br": "apostas-ao-vivo-vs-pre-jogo-por-que-o-timing-muda-tudo",
        "pt-pt": "apostas-em-direto-vs-pre-jogo-porque-o-timing-importa",
        "es":    "apuestas-en-vivo-vs-pre-partido-por-que-importa-el-timing",
    },
    "what-is-betting-edge-how-to-calculate": {
        "pt-br": "o-que-e-edge-em-apostas-como-calcular",
        "pt-pt": "o-que-e-o-edge-nas-apostas-como-calcular",
        "es":    "que-es-el-edge-en-apuestas-como-calcularlo",
    },
    "over-under-betting-strategy-xg": {
        "pt-br": "apostas-over-under-por-que-xg-torna-previsivel",
        "pt-pt": "apostas-over-under-porque-o-xg-as-torna-previsiveis",
        "es":    "apuestas-over-under-por-que-xg-las-hace-predecibles",
    },
    "how-real-time-xg-powers-our-betting-tips": {
        "pt-br": "como-xg-tempo-real-move-tips-apostas",
        "pt-pt": "como-o-xg-em-tempo-real-impulsiona-as-nossas-tips",
        "es":    "como-el-xg-en-tiempo-real-impulsa-nuestras-tips",
    },
    "what-is-xg-expected-goals-football": {
        "pt-br": "o-que-e-xg-gols-esperados-futebol",
        "pt-pt": "o-que-e-xg-golos-esperados-futebol",
        "es":    "que-es-xg-goles-esperados-futbol",
    },
}


# Competition / league name translations. The bookmaker data feed
# returns competitions in English (Sofascore is English-first), but
# BR audiences search "Campeonato Inglês", "Liga dos Campeões", etc.
# This map is consulted by meta-tag generation, league-page titles
# and the homepage SEO copy whenever rendering for a non-EN locale.
#
# Keys are matched case-insensitively against the raw tournament string.
# Extend as new competitions appear in our monitored set.
_LEAGUE_NAME_I18N = {
    "pt-br": {
        "premier league":          "Campeonato Inglês",
        "la liga":                 "Campeonato Espanhol",
        "laliga":                  "Campeonato Espanhol",
        "serie a":                 "Campeonato Italiano",
        "bundesliga":              "Campeonato Alemão",
        "ligue 1":                 "Campeonato Francês",
        "primeira liga":           "Campeonato Português",
        "liga portugal":           "Campeonato Português",
        "liga portugal betclic":   "Campeonato Português",
        "eredivisie":              "Campeonato Holandês",
        "vriendenloterij eredivisie": "Campeonato Holandês",
        "jupiler pro league":      "Campeonato Belga",
        "pro league":              "Campeonato Belga",
        "uefa champions league":   "Liga dos Campeões",
        "champions league":        "Liga dos Campeões",
        "uefa europa league":      "Liga Europa",
        "europa league":           "Liga Europa",
        "uefa europa conference league": "Liga Conferência",
        "conference league":       "Liga Conferência",
        "copa libertadores":       "Copa Libertadores",
        "conmebol libertadores":   "Copa Libertadores",
        "copa sudamericana":       "Copa Sul-Americana",
        "conmebol sudamericana":   "Copa Sul-Americana",
        "brasileirão":             "Brasileirão",
        "brasileirao":             "Brasileirão",
        "brasileirão série a":     "Brasileirão Série A",
        "campeonato brasileiro série a": "Brasileirão Série A",
        "mls":                     "MLS",
        "fifa world cup":          "Copa do Mundo",
        "world cup":               "Copa do Mundo",
        "fifa world cup 2026":     "Copa do Mundo 2026",
    },
    "pt-pt": {
        # Convenção PT-PT: "Liga Inglesa", "Liga Espanhola" — termos
        # comummente usados nas crónicas desportivas portuguesas. Para
        # competições internacionais usa-se o termo localizado natural
        # ("Liga dos Campeões", "Liga Europa").
        "premier league":          "Liga Inglesa",
        "la liga":                 "Liga Espanhola",
        "laliga":                  "Liga Espanhola",
        "serie a":                 "Liga Italiana",
        "bundesliga":              "Bundesliga",
        "ligue 1":                 "Ligue 1",
        "primeira liga":           "Liga Portuguesa",
        "liga portugal":           "Liga Portuguesa",
        "liga portugal betclic":   "Liga Portugal Betclic",
        "eredivisie":              "Eredivisie",
        "vriendenloterij eredivisie": "Eredivisie",
        "jupiler pro league":      "Pro League Belga",
        "pro league":              "Pro League Belga",
        "uefa champions league":   "Liga dos Campeões",
        "champions league":        "Liga dos Campeões",
        "uefa europa league":      "Liga Europa",
        "europa league":           "Liga Europa",
        "uefa europa conference league": "Liga Conferência",
        "conference league":       "Liga Conferência",
        "copa libertadores":       "Taça Libertadores",
        "conmebol libertadores":   "Taça Libertadores",
        "copa sudamericana":       "Taça Sul-Americana",
        "conmebol sudamericana":   "Taça Sul-Americana",
        "brasileirão":             "Brasileirão",
        "brasileirao":             "Brasileirão",
        "brasileirão série a":     "Brasileirão Série A",
        "campeonato brasileiro série a": "Brasileirão Série A",
        "mls":                     "MLS",
        "fifa world cup":          "Mundial",
        "world cup":               "Mundial",
        "fifa world cup 2026":     "Mundial 2026",
    },
    "es": {
        # Spanish (Spain + LatAm) — Spanish media commonly translates
        # european leagues: "Liga Inglesa", "Bundesliga" stays as-is.
        # International competitions: "Liga de Campeones", "Liga Europa".
        "premier league":          "Premier League",
        "la liga":                 "LaLiga",
        "laliga":                  "LaLiga",
        "serie a":                 "Serie A",
        "bundesliga":              "Bundesliga",
        "ligue 1":                 "Ligue 1",
        "primeira liga":           "Liga Portuguesa",
        "liga portugal":           "Liga Portuguesa",
        "liga portugal betclic":   "Liga Portugal Betclic",
        "eredivisie":              "Eredivisie",
        "vriendenloterij eredivisie": "Eredivisie",
        "jupiler pro league":      "Pro League Belga",
        "pro league":              "Pro League Belga",
        "uefa champions league":   "Liga de Campeones",
        "champions league":        "Liga de Campeones",
        "uefa europa league":      "Europa League",
        "europa league":           "Europa League",
        "uefa europa conference league": "Conference League",
        "conference league":       "Conference League",
        "copa libertadores":       "Copa Libertadores",
        "conmebol libertadores":   "Copa Libertadores",
        "copa sudamericana":       "Copa Sudamericana",
        "conmebol sudamericana":   "Copa Sudamericana",
        "brasileirão":             "Brasileirão",
        "brasileirao":             "Brasileirão",
        "brasileirão série a":     "Brasileirão Serie A",
        "campeonato brasileiro série a": "Brasileirão Serie A",
        "mls":                     "MLS",
        "fifa world cup":          "Mundial",
        "world cup":               "Mundial",
        "fifa world cup 2026":     "Mundial 2026",
    },
}


def _localized_league_name(name: str, locale: str) -> str:
    """Translate a competition name to the requested locale.

    Sofascore returns split-format names like "UEFA Europa League,
    Knockout stage" or "Pro League, Conference League Playoffs". We
    translate the LEAGUE-NAME prefix (before the comma) and keep the
    stage suffix as-is — adding stage translations later is a follow-up.

    Passthrough if locale has no entry or the name isn't in the map.
    """
    if not name or locale == "en":
        return name or ""
    bucket = _LEAGUE_NAME_I18N.get(locale) or {}
    key = name.strip().lower()
    if key in bucket:
        return bucket[key]
    # Try prefix before comma (handles "UEFA Europa League, Knockout stage")
    if "," in key:
        prefix, _, rest = key.partition(",")
        prefix = prefix.strip()
        if prefix in bucket:
            # Preserve original casing on the rest, just swap the prefix.
            _, _, orig_rest = name.partition(",")
            return f"{bucket[prefix]},{orig_rest}"
    return name


def _matchup_separator(locale: str) -> str:
    """BR audiences write 'Time A x Time B'. EN/PT-PT/ES use 'vs'."""
    return " x " if locale == "pt-br" else " vs "


def _localized_blog_slug(en_slug: str, locale: str) -> str:
    """Return the locale-specific blog slug for an EN canonical slug.
    Passthrough on EN or when no translation has been added yet."""
    if locale == "en":
        return en_slug
    return _BLOG_SLUG_I18N.get(en_slug, {}).get(locale, en_slug)


def _canonical_blog_slug(localized_slug: str, locale: str) -> str:
    """Reverse: given a localized blog slug, return the EN canonical slug.
    Returns the input unchanged if no mapping exists."""
    if locale == "en":
        return localized_slug
    for en, bucket in _BLOG_SLUG_I18N.items():
        if bucket.get(locale) == localized_slug:
            return en
    return localized_slug


def _localized_slug(en_path: str, locale: str) -> str:
    """Return the locale-specific slug for an EN canonical path.
    Passthrough if the locale has no entry for that path."""
    return _STATIC_SLUG_I18N.get(locale, {}).get(en_path, en_path)


def _canonical_slug(localized_path: str, locale: str) -> str:
    """Reverse of _localized_slug: given a localized URL slug, return the
    EN canonical path so downstream routing (which keys off /about,
    /history, etc.) can match. Returns the input unchanged if no mapping
    exists — supports both directions transparently.
    """
    bucket = _STATIC_SLUG_I18N.get(locale, {})
    for en, local in bucket.items():
        if local == localized_path:
            return en
    return localized_path


# Per-locale prefix translations for DYNAMIC routes. Unlike static slugs
# (which are exact paths), these prefix-match the URL and translate only
# the path-prefix segment. Used to canonicalize incoming localized URLs
# like /br/jogo/123/X → /match/123/X for downstream routing, and in the
# reverse direction when building outbound links / sitemaps.
#
# WHY THIS MATTERS: without this map, a Googlebot request to
# /br/jogo/16163439 would NOT match `^/match/\d+` in prerender_dispatch
# and would fall through to the Lovable SPA shell — meaning every BR
# match URL indexed by Google gets the homepage hero as H1 instead of
# the match-specific prerender. Same applies to /liga, /equipa, /palpites.
_DYNAMIC_PATH_PREFIXES_I18N = {
    "pt-br": {
        "/match/":   "/jogo/",
        "/league/":  "/liga/",
        "/team/":    "/equipa/",
        "/tips/":    "/palpites/",
    },
    "pt-pt": {
        # PT-PT shares Portuguese vocabulary with BR for these terms.
        "/match/":   "/jogo/",
        "/league/":  "/liga/",
        "/team/":    "/equipa/",
        "/tips/":    "/palpites/",
    },
    "es": {
        "/match/":   "/partido/",
        "/league/":  "/liga/",
        "/team/":    "/equipo/",
        "/tips/":    "/pronosticos/",
    },
}


def _canonical_dynamic_path(localized_path: str, locale: str) -> str:
    """Convert a localized dynamic path back to its EN canonical form.
       /jogo/123/slug → /match/123/slug  (pt-br, pt-pt)
       /partido/123    → /match/123       (es)
       /liga/foo       → /league/foo      (all non-EN)
    Returns the input unchanged if no mapping applies."""
    if locale == "en":
        return localized_path
    bucket = _DYNAMIC_PATH_PREFIXES_I18N.get(locale) or {}
    for en_prefix, local_prefix in bucket.items():
        if localized_path.startswith(local_prefix):
            return en_prefix + localized_path[len(local_prefix):]
    return localized_path


def _localized_dynamic_path(en_path: str, locale: str) -> str:
    """Forward of _canonical_dynamic_path. Used when generating outbound
    links or sitemap URLs that should carry the locale-native slug.
       /match/123/slug → /jogo/123/slug   (pt-br, pt-pt)
       /league/foo     → /liga/foo        (all non-EN)
    """
    if locale == "en":
        return en_path
    bucket = _DYNAMIC_PATH_PREFIXES_I18N.get(locale) or {}
    for en_prefix, local_prefix in bucket.items():
        if en_path.startswith(en_prefix):
            return local_prefix + en_path[len(en_prefix):]
    return en_path


def _extract_lang_prefix(path: str) -> tuple[str, str]:
    """Pull the lang prefix off a URL path. Returns (locale, EN-canonical-path).

    Also translates locale-specific slugs back to their EN canonical form
    so downstream route matching only needs to know the EN path. Examples:

        /br/match/123 → ("pt-br", "/match/123")
        /br/sobre     → ("pt-br", "/about")           ← BR slug normalized
        /br/historico → ("pt-br", "/history")
        /pt           → ("pt-pt", "/")
        /es/about     → ("es",    "/about")
        /match/123    → ("en",    "/match/123")
        /             → ("en",    "/")
    """
    import re as _re_lp
    m = _re_lp.match(r"^/(pt|br|es)(/.*|$)", path or "/")
    if not m:
        return "en", path or "/"
    locale = _LANG_PREFIX_TO_LOCALE[m.group(1)]
    stripped = m.group(2) or "/"
    if stripped == "":
        stripped = "/"
    # Normalize localized static slug → EN canonical (/sobre→/about etc).
    stripped = _canonical_slug(stripped, locale)
    # Normalize localized dynamic path prefix → EN canonical
    # (/jogo/123→/match/123, /liga/foo→/league/foo, etc.)
    stripped = _canonical_dynamic_path(stripped, locale)
    return locale, stripped


def _hreflang_tags(en_canonical_path: str) -> str:
    """Render the full set of <link rel="alternate" hreflang> tags for a
    given EN-canonical path. Each locale's href uses its own translated
    slug — eg the /about page emits hreflang="pt-br" with /br/sobre, not
    /br/about. Includes x-default → EN canonical (no prefix).
    """
    tags = []
    for locale, prefix in _LOCALE_TO_PREFIX.items():
        local_path = _localized_slug(en_canonical_path, locale)
        href = f"{SITE_URL}{prefix}{local_path}"
        tags.append(f'<link rel="alternate" hreflang="{locale}" href="{href}">')
    tags.append(f'<link rel="alternate" hreflang="x-default" href="{SITE_URL}{en_canonical_path}">')
    return "\n    ".join(tags)


# ── Static legal/info pages — proper SSR (not SPA shell) ────────────────────
# Each page has self-canonical, descriptive H1, factual body copy, and a
# BreadcrumbList JSON-LD. Previously these served the SPA shell with the
# homepage canonical, which signaled Google to deindex them as duplicates.
_STATIC_PAGES: dict = {
    "/about": {
        "title":       f"About WebPronos — How Our AI Football Prediction Model Works",
        "description": "WebPronos is an AI-powered football tips platform. Our xG model identifies positive-EV value bets in real time across 25+ leagues and publishes every pick with a full audit trail.",
        "h1":          "About WebPronos",
        "breadcrumb":  "About",
        "body":        """
            <p>WebPronos is a live football prediction platform powered by a proprietary AI model built around <strong>Expected Goals (xG)</strong>. We monitor live matches across more than 25 competitions, ingest shot-by-shot data, and identify positive-expected-value bets the moment the bookmaker odds diverge from the model's probabilities.</p>
            <h2 class="pr-h2">What we do</h2>
            <p>Every pick our model generates is logged the instant the value appears — never edited, never deleted. We publish the entry odds, the live in-game state at the moment of the pick, the running result, and a settled profit/loss summary. The full track record lives at <a href="/history">webpronos.com/history</a>.</p>
            <h2 class="pr-h2">How the model works</h2>
            <p>The core engine is an xG model trained on shot-level data. For every shot in a match, the model computes its expected goal probability based on position, body part, situation, and defensive pressure. These per-shot probabilities aggregate into live win/draw/loss, over/under, and handicap probabilities, which are then compared to the live bookmaker odds using a Benter-style edge calculation. Any market where the model's implied probability exceeds the bookmaker's (after vig adjustment) is flagged as a value pick.</p>
            <h2 class="pr-h2">Why publish everything</h2>
            <p>Most tipsters cherry-pick wins and hide losses. By publishing every single settled pick — including the bad ones — we let anyone audit our edge. If the long-term ROI stays positive across thousands of picks, the model is genuinely beating the market.</p>
            <p>Browse the <a href="/today">live picks today</a>, the <a href="/tomorrow">tomorrow's fixtures</a>, the <a href="/history">full audit history</a>, or read our <a href="/blog">blog</a> for deeper xG explainers.</p>
        """,
    },
    "/terms": {
        "title":       "Terms of Service | WebPronos",
        "description": "Terms of Service for WebPronos. By using our site you accept these terms. We provide statistical football predictions for informational purposes only — no outcome is guaranteed.",
        "h1":          "Terms of Service",
        "breadcrumb":  "Terms",
        "body":        """
            <p>By accessing or using WebPronos you agree to these terms. WebPronos publishes statistical football predictions and historical performance data <strong>for informational purposes only</strong>. We do not offer betting, gambling, or financial advice.</p>
            <h2 class="pr-h2">No guaranteed outcomes</h2>
            <p>Every prediction on WebPronos is a probabilistic estimate generated by an AI model based on publicly available match data. No outcome is guaranteed. Past performance — including any positive ROI shown in our <a href="/history">track record</a> — does not predict future results.</p>
            <h2 class="pr-h2">User responsibility</h2>
            <p>You are solely responsible for any betting decisions you make. WebPronos is not a bookmaker and accepts no liability for any financial loss arising from the use of our predictions.</p>
            <h2 class="pr-h2">Age restriction</h2>
            <p>You must be at least 18 years old to use this site. See our <a href="/responsible-gambling">responsible gambling page</a> for help and support resources.</p>
            <h2 class="pr-h2">Intellectual property</h2>
            <p>The predictions, datasets, and analytical commentary on WebPronos are the intellectual property of WebPronos. Reuse for non-commercial purposes is permitted with attribution and a backlink to the source page.</p>
            <h2 class="pr-h2">Changes to these terms</h2>
            <p>We may update these terms periodically. Material changes will be noted at the top of this page with the effective date.</p>
        """,
    },
    "/privacy": {
        "title":       "Privacy Policy | WebPronos",
        "description": "WebPronos privacy policy. We collect minimal analytics to improve the service. We do not sell user data and we comply with GDPR.",
        "h1":          "Privacy Policy",
        "breadcrumb":  "Privacy",
        "body":        """
            <p>WebPronos respects your privacy. This page explains what data we collect, why, and how we handle it.</p>
            <h2 class="pr-h2">What we collect</h2>
            <p>We collect minimal first-party analytics: page views, referrer, browser type, and approximate geographic region. We do not collect names, email addresses, or payment data unless you explicitly provide them (for example, by subscribing to the Telegram channel).</p>
            <h2 class="pr-h2">Third-party services</h2>
            <p>Some content is served via Cloudflare (CDN), Lovable (hosting), and Fly.io (backend). These providers may set technical cookies necessary for the site to function.</p>
            <h2 class="pr-h2">Your rights (GDPR)</h2>
            <p>If you are in the EU/EEA, you have the right to access, correct, or delete any data we hold about you. Contact us through the link in the footer.</p>
            <h2 class="pr-h2">Data retention</h2>
            <p>Analytics data is retained for a maximum of 12 months and then automatically aggregated and anonymized.</p>
        """,
    },
    "/responsible-gambling": {
        "title":       "Responsible Gambling — Bet Safely | WebPronos",
        "description": "Help and resources for safe betting. If gambling is no longer fun, please seek help. 18+ only. Resources include BeGambleAware, GamCare, and SOS Jogador.",
        "h1":          "Responsible Gambling",
        "breadcrumb":  "Responsible Gambling",
        "body":        """
            <p>WebPronos publishes football predictions for informational and entertainment purposes only. <strong>Betting carries risk.</strong> Never bet more than you can afford to lose.</p>
            <h2 class="pr-h2">Signs of a problem</h2>
            <ul>
              <li>Betting more than you intended, or chasing losses</li>
              <li>Hiding gambling activity from friends and family</li>
              <li>Borrowing money to gamble</li>
              <li>Feeling anxious or depressed about your gambling</li>
              <li>Gambling interfering with work, school, or relationships</li>
            </ul>
            <h2 class="pr-h2">Where to get help</h2>
            <ul>
              <li><strong>BeGambleAware</strong> (UK) — <a href="https://www.begambleaware.org" rel="noopener noreferrer">begambleaware.org</a> · 0808 8020 133</li>
              <li><strong>GamCare</strong> (UK) — <a href="https://www.gamcare.org.uk" rel="noopener noreferrer">gamcare.org.uk</a></li>
              <li><strong>SOS Jogador</strong> (Portugal) — <a href="https://www.sosjogador.org" rel="noopener noreferrer">sosjogador.org</a> · 813 211 311</li>
              <li><strong>National Council on Problem Gambling</strong> (US) — <a href="https://www.ncpgambling.org" rel="noopener noreferrer">ncpgambling.org</a> · 1-800-522-4700</li>
            </ul>
            <h2 class="pr-h2">Bet sensibly</h2>
            <p>Set a daily/weekly budget and stick to it. Take regular breaks. Treat betting as entertainment — not a way to make money. If you find yourself struggling, please reach out to one of the resources above.</p>
            <p><strong>18+ only.</strong> If you suspect you have a gambling problem, please stop immediately and seek help.</p>
        """,
    },
}

# Per-locale overrides of the static pages dict.
# Lookup order in `_render_static_page`:
#   1. _STATIC_PAGES_I18N[locale][path]   (full translation)
#   2. _STATIC_PAGES[path]                 (EN fallback)
# Adding a new locale = append one block here. No code change elsewhere.
_STATIC_PAGES_I18N: dict = {
    "pt-br": {
        "/about": {
            "title":       "Sobre o WebPronos — Como Funciona Nosso Modelo de IA de Previsões de Futebol",
            "description": "WebPronos é uma plataforma de tips de futebol com inteligência artificial. Nosso modelo xG identifica apostas de valor positivo em tempo real em mais de 25 ligas e publica cada pick com auditoria completa.",
            "h1":          "Sobre o WebPronos",
            "breadcrumb":  "Sobre",
            "body":        """
            <p>O WebPronos é uma plataforma de previsões de futebol ao vivo movida por um modelo de IA proprietário construído em torno do <strong>Expected Goals (xG)</strong>. Monitoramos jogos ao vivo em mais de 25 competições, ingerimos dados de cada chute e identificamos apostas de valor esperado positivo no momento em que as odds da casa de apostas divergem das probabilidades do modelo.</p>
            <h2 class="pr-h2">O que fazemos</h2>
            <p>Cada pick gerado pelo nosso modelo é registrado no instante em que o valor aparece — nunca editado, nunca apagado. Publicamos a odd de entrada, o estado ao vivo do jogo no momento do pick, o resultado em curso e o resumo de lucro/prejuízo já liquidado. O histórico completo está em <a href="/br/history">webpronos.com/br/history</a>.</p>
            <h2 class="pr-h2">Como o modelo funciona</h2>
            <p>O motor central é um modelo xG treinado em dados ao nível do chute. Para cada chute em um jogo, o modelo calcula a probabilidade de gol esperado com base na posição, parte do corpo, situação e pressão defensiva. Essas probabilidades por chute se agregam em probabilidades ao vivo de vitória/empate/derrota, over/under e handicap, que são então comparadas com as odds ao vivo da casa de apostas usando um cálculo de edge no estilo Benter. Qualquer mercado onde a probabilidade implícita do modelo excede a da casa (após ajuste de margem) é sinalizado como uma pick de valor.</p>
            <h2 class="pr-h2">Por que publicamos tudo</h2>
            <p>A maioria dos tipsters seleciona apenas as vitórias e esconde as derrotas. Ao publicar cada pick liquidado — incluindo as ruins — permitimos que qualquer pessoa audite nosso edge. Se o ROI de longo prazo se mantiver positivo ao longo de milhares de picks, o modelo está realmente batendo o mercado.</p>
            <p>Veja os <a href="/br/today">picks ao vivo de hoje</a>, os <a href="/br/tomorrow">jogos de amanhã</a>, o <a href="/br/history">histórico completo de auditoria</a> ou leia nosso <a href="/br/blog">blog</a> para explicações mais profundas sobre xG.</p>
        """,
        },
        "/terms": {
            "title":       "Termos de Uso | WebPronos",
            "description": "Termos de Uso do WebPronos. Ao usar nosso site você aceita estes termos. Fornecemos previsões estatísticas de futebol apenas para fins informativos — nenhum resultado é garantido.",
            "h1":          "Termos de Uso",
            "breadcrumb":  "Termos",
            "body":        """
            <p>Ao acessar ou usar o WebPronos você concorda com estes termos. O WebPronos publica previsões estatísticas de futebol e dados históricos de desempenho <strong>apenas para fins informativos</strong>. Não oferecemos apostas, jogos de azar ou aconselhamento financeiro.</p>
            <h2 class="pr-h2">Nenhum resultado garantido</h2>
            <p>Cada previsão no WebPronos é uma estimativa probabilística gerada por um modelo de IA com base em dados de jogos publicamente disponíveis. Nenhum resultado é garantido. O desempenho passado — incluindo qualquer ROI positivo mostrado em nosso <a href="/br/history">histórico</a> — não prevê resultados futuros.</p>
            <h2 class="pr-h2">Responsabilidade do usuário</h2>
            <p>Você é o único responsável por qualquer decisão de aposta que tomar. O WebPronos não é uma casa de apostas e não assume responsabilidade por qualquer perda financeira decorrente do uso de nossas previsões.</p>
            <h2 class="pr-h2">Restrição de idade</h2>
            <p>Você deve ter pelo menos 18 anos para usar este site. Consulte nossa <a href="/br/responsible-gambling">página de jogo responsável</a> para recursos de ajuda e suporte.</p>
            <h2 class="pr-h2">Propriedade intelectual</h2>
            <p>As previsões, conjuntos de dados e comentários analíticos no WebPronos são propriedade intelectual do WebPronos. A reutilização para fins não comerciais é permitida com atribuição e backlink para a página de origem.</p>
            <h2 class="pr-h2">Alterações nestes termos</h2>
            <p>Podemos atualizar estes termos periodicamente. Alterações materiais serão indicadas no topo desta página com a data de entrada em vigor.</p>
        """,
        },
        "/privacy": {
            "title":       "Política de Privacidade | WebPronos",
            "description": "Política de privacidade do WebPronos. Coletamos análises mínimas para melhorar o serviço. Não vendemos dados de usuários e cumprimos a LGPD/GDPR.",
            "h1":          "Política de Privacidade",
            "breadcrumb":  "Privacidade",
            "body":        """
            <p>O WebPronos respeita sua privacidade. Esta página explica quais dados coletamos, por quê e como os tratamos.</p>
            <h2 class="pr-h2">O que coletamos</h2>
            <p>Coletamos análises mínimas de primeira parte: visualizações de página, referenciador, tipo de navegador e região geográfica aproximada. Não coletamos nomes, endereços de e-mail ou dados de pagamento, a menos que você os forneça explicitamente (por exemplo, ao se inscrever no canal do Telegram).</p>
            <h2 class="pr-h2">Serviços de terceiros</h2>
            <p>Parte do conteúdo é servido via Cloudflare (CDN), Lovable (hospedagem) e Fly.io (backend). Esses provedores podem definir cookies técnicos necessários para o funcionamento do site.</p>
            <h2 class="pr-h2">Seus direitos (LGPD/GDPR)</h2>
            <p>Se você estiver no Brasil ou na UE/EEE, tem o direito de acessar, corrigir ou excluir quaisquer dados que tenhamos sobre você. Entre em contato pelo link no rodapé.</p>
            <h2 class="pr-h2">Retenção de dados</h2>
            <p>Os dados analíticos são retidos por no máximo 12 meses e depois agregados e anonimizados automaticamente.</p>
        """,
        },
        "/responsible-gambling": {
            "title":       "Jogo Responsável — Aposte com Segurança | WebPronos",
            "description": "Ajuda e recursos para apostas seguras. Se o jogo deixou de ser divertido, procure ajuda. Apenas 18+. Recursos incluem Jogadores Anônimos, BeGambleAware, GamCare e SOS Jogador.",
            "h1":          "Jogo Responsável",
            "breadcrumb":  "Jogo Responsável",
            "body":        """
            <p>O WebPronos publica previsões de futebol apenas para fins informativos e de entretenimento. <strong>As apostas envolvem risco.</strong> Nunca aposte mais do que você pode perder.</p>
            <h2 class="pr-h2">Sinais de um problema</h2>
            <ul>
              <li>Apostar mais do que pretendia ou tentar recuperar perdas</li>
              <li>Esconder a atividade de apostas de amigos e familiares</li>
              <li>Pedir dinheiro emprestado para apostar</li>
              <li>Sentir ansiedade ou depressão por causa das apostas</li>
              <li>As apostas interferirem no trabalho, estudos ou relacionamentos</li>
            </ul>
            <h2 class="pr-h2">Onde obter ajuda</h2>
            <ul>
              <li><strong>Jogadores Anônimos Brasil</strong> — <a href="https://jogadoresanonimos.com.br" rel="noopener noreferrer">jogadoresanonimos.com.br</a></li>
              <li><strong>CVV — Centro de Valorização da Vida</strong> (Brasil, 24h) — <a href="https://www.cvv.org.br" rel="noopener noreferrer">cvv.org.br</a> · 188</li>
              <li><strong>BeGambleAware</strong> (Internacional) — <a href="https://www.begambleaware.org" rel="noopener noreferrer">begambleaware.org</a></li>
              <li><strong>GamCare</strong> (Reino Unido) — <a href="https://www.gamcare.org.uk" rel="noopener noreferrer">gamcare.org.uk</a></li>
              <li><strong>SOS Jogador</strong> (Portugal) — <a href="https://www.sosjogador.org" rel="noopener noreferrer">sosjogador.org</a></li>
            </ul>
            <h2 class="pr-h2">Aposte com bom senso</h2>
            <p>Defina um orçamento diário/semanal e respeite-o. Faça pausas regulares. Trate as apostas como entretenimento — não como uma forma de ganhar dinheiro. Se estiver com dificuldades, entre em contato com um dos recursos acima.</p>
            <p><strong>Apenas 18+.</strong> Se você suspeita que tem um problema com apostas, pare imediatamente e procure ajuda.</p>
        """,
        },
    },
    "pt-pt": {
        "/about": {
            "title":       "Sobre a WebPronos — Como Funciona o Nosso Modelo de IA de Previsões de Futebol",
            "description": "A WebPronos é uma plataforma de palpites de futebol com inteligência artificial. O nosso modelo xG identifica apostas de valor positivo em tempo real em mais de 25 ligas e publica cada pick com auditoria completa.",
            "h1":          "Sobre a WebPronos",
            "breadcrumb":  "Sobre",
            "body":        """
            <p>A WebPronos é uma plataforma de previsões de futebol em direto movida por um modelo de IA proprietário construído em torno dos <strong>Golos Esperados (xG)</strong>. Monitorizamos jogos em direto em mais de 25 competições, ingerimos dados de cada remate e identificamos apostas de valor esperado positivo no momento em que as odds da casa de apostas divergem das probabilidades do modelo.</p>
            <h2 class="pr-h2">O que fazemos</h2>
            <p>Cada pick gerado pelo nosso modelo é registado no instante em que o valor aparece — nunca editado, nunca apagado. Publicamos a odd de entrada, o estado em direto do jogo no momento do pick, o resultado em curso e o resumo de lucro/prejuízo já liquidado. O histórico completo está em <a href="/pt/historico">webpronos.com/pt/historico</a>.</p>
            <h2 class="pr-h2">Como o modelo funciona</h2>
            <p>O motor central é um modelo xG treinado em dados ao nível do remate. Para cada remate num jogo, o modelo calcula a probabilidade de golo esperado com base na posição, parte do corpo, situação e pressão defensiva. Essas probabilidades por remate agregam-se em probabilidades em direto de vitória/empate/derrota, over/under e handicap, que são depois comparadas com as odds em direto da casa de apostas usando um cálculo de edge no estilo Benter. Qualquer mercado onde a probabilidade implícita do modelo excede a da casa (após ajuste de margem) é sinalizado como uma pick de valor.</p>
            <h2 class="pr-h2">Porque publicamos tudo</h2>
            <p>A maioria dos tipsters seleciona apenas as vitórias e esconde as derrotas. Ao publicar cada pick liquidado — incluindo as más — permitimos que qualquer pessoa audite o nosso edge. Se o ROI a longo prazo se mantiver positivo ao longo de milhares de picks, o modelo está realmente a bater o mercado.</p>
            <p>Vê os <a href="/pt/hoje">picks em direto de hoje</a>, os <a href="/pt/amanha">jogos de amanhã</a>, o <a href="/pt/historico">histórico completo de auditoria</a> ou lê o nosso <a href="/pt/blog">blog</a> para explicações mais profundas sobre xG.</p>
        """,
        },
        "/terms": {
            "title":       "Termos de Utilização | WebPronos",
            "description": "Termos de Utilização da WebPronos. Ao usar o nosso site aceitas estes termos. Fornecemos previsões estatísticas de futebol apenas para fins informativos — nenhum resultado é garantido.",
            "h1":          "Termos de Utilização",
            "breadcrumb":  "Termos",
            "body":        """
            <p>Ao aceder ou utilizar a WebPronos concordas com estes termos. A WebPronos publica previsões estatísticas de futebol e dados históricos de desempenho <strong>apenas para fins informativos</strong>. Não oferecemos apostas, jogos de azar ou aconselhamento financeiro.</p>
            <h2 class="pr-h2">Nenhum resultado garantido</h2>
            <p>Cada previsão na WebPronos é uma estimativa probabilística gerada por um modelo de IA com base em dados de jogos publicamente disponíveis. Nenhum resultado é garantido. O desempenho passado — incluindo qualquer ROI positivo mostrado no nosso <a href="/pt/historico">histórico</a> — não prevê resultados futuros.</p>
            <h2 class="pr-h2">Responsabilidade do utilizador</h2>
            <p>Tu és o único responsável por qualquer decisão de aposta que tomes. A WebPronos não é uma casa de apostas e não assume responsabilidade por qualquer perda financeira decorrente do uso das nossas previsões.</p>
            <h2 class="pr-h2">Restrição de idade</h2>
            <p>Tens de ter pelo menos 18 anos para usar este site. Consulta a nossa <a href="/pt/jogo-responsavel">página de jogo responsável</a> para recursos de ajuda e apoio.</p>
            <h2 class="pr-h2">Propriedade intelectual</h2>
            <p>As previsões, conjuntos de dados e comentários analíticos na WebPronos são propriedade intelectual da WebPronos. A reutilização para fins não comerciais é permitida com atribuição e backlink para a página de origem.</p>
            <h2 class="pr-h2">Alterações a estes termos</h2>
            <p>Podemos atualizar estes termos periodicamente. Alterações materiais serão indicadas no topo desta página com a data de entrada em vigor.</p>
        """,
        },
        "/privacy": {
            "title":       "Política de Privacidade | WebPronos",
            "description": "Política de privacidade da WebPronos. Recolhemos análises mínimas para melhorar o serviço. Não vendemos dados de utilizadores e cumprimos o RGPD.",
            "h1":          "Política de Privacidade",
            "breadcrumb":  "Privacidade",
            "body":        """
            <p>A WebPronos respeita a tua privacidade. Esta página explica que dados recolhemos, porquê e como os tratamos.</p>
            <h2 class="pr-h2">O que recolhemos</h2>
            <p>Recolhemos análises mínimas de primeira parte: visualizações de página, referenciador, tipo de browser e região geográfica aproximada. Não recolhemos nomes, endereços de e-mail ou dados de pagamento, exceto se os forneceres explicitamente (por exemplo, ao subscreveres o canal de Telegram).</p>
            <h2 class="pr-h2">Serviços de terceiros</h2>
            <p>Parte do conteúdo é servido via Cloudflare (CDN), Lovable (alojamento) e Fly.io (backend). Esses fornecedores podem definir cookies técnicos necessários para o funcionamento do site.</p>
            <h2 class="pr-h2">Os teus direitos (RGPD)</h2>
            <p>Se estiveres na UE/EEE (incluindo Portugal), tens o direito de aceder, corrigir ou eliminar quaisquer dados que tenhamos sobre ti. Contacta-nos pelo link no rodapé.</p>
            <h2 class="pr-h2">Retenção de dados</h2>
            <p>Os dados analíticos são retidos por no máximo 12 meses e depois agregados e anonimizados automaticamente.</p>
        """,
        },
        "/responsible-gambling": {
            "title":       "Jogo Responsável — Aposta em Segurança | WebPronos",
            "description": "Ajuda e recursos para apostas seguras. Se o jogo deixou de ser divertido, procura ajuda. Apenas 18+. Recursos incluem SOS Jogador, Jogadores Anónimos, BeGambleAware e GamCare.",
            "h1":          "Jogo Responsável",
            "breadcrumb":  "Jogo Responsável",
            "body":        """
            <p>A WebPronos publica previsões de futebol apenas para fins informativos e de entretenimento. <strong>As apostas envolvem risco.</strong> Nunca apostes mais do que podes perder.</p>
            <h2 class="pr-h2">Sinais de um problema</h2>
            <ul>
              <li>Apostar mais do que pretendias ou tentar recuperar perdas</li>
              <li>Esconder a atividade de apostas de amigos e familiares</li>
              <li>Pedir dinheiro emprestado para apostar</li>
              <li>Sentir ansiedade ou depressão por causa das apostas</li>
              <li>As apostas interferirem com o trabalho, estudos ou relacionamentos</li>
            </ul>
            <h2 class="pr-h2">Onde obter ajuda</h2>
            <ul>
              <li><strong>SOS Jogador</strong> (Portugal) — <a href="https://www.sosjogador.org" rel="noopener noreferrer">sosjogador.org</a> · 813 405 405</li>
              <li><strong>Jogadores Anónimos Portugal</strong> — <a href="https://jogadoresanonimos.pt" rel="noopener noreferrer">jogadoresanonimos.pt</a></li>
              <li><strong>SICAD — Serviço de Intervenção nos Comportamentos Aditivos</strong> — <a href="https://www.sicad.pt" rel="noopener noreferrer">sicad.pt</a></li>
              <li><strong>SNS24</strong> (24h) — 808 24 24 24</li>
              <li><strong>BeGambleAware</strong> (Internacional) — <a href="https://www.begambleaware.org" rel="noopener noreferrer">begambleaware.org</a></li>
              <li><strong>GamCare</strong> (Reino Unido) — <a href="https://www.gamcare.org.uk" rel="noopener noreferrer">gamcare.org.uk</a></li>
            </ul>
            <h2 class="pr-h2">Aposta com bom senso</h2>
            <p>Define um orçamento diário/semanal e respeita-o. Faz pausas regulares. Trata as apostas como entretenimento — não como uma forma de ganhar dinheiro. Se estiveres com dificuldades, contacta um dos recursos acima.</p>
            <p><strong>Apenas 18+.</strong> Se suspeitas que tens um problema com apostas, pára imediatamente e procura ajuda.</p>
        """,
        },
    },
    "es": {
        "/about": {
            "title":       "Sobre WebPronos — Cómo Funciona Nuestro Modelo de IA de Predicciones de Fútbol",
            "description": "WebPronos es una plataforma de pronósticos de fútbol con inteligencia artificial. Nuestro modelo xG identifica apuestas de valor positivo en tiempo real en más de 25 ligas y publica cada pick con auditoría completa.",
            "h1":          "Sobre WebPronos",
            "breadcrumb":  "Sobre",
            "body":        """
            <p>WebPronos es una plataforma de predicciones de fútbol en vivo impulsada por un modelo de IA propietario construido en torno a los <strong>Goles Esperados (xG)</strong>. Monitorizamos partidos en vivo en más de 25 competiciones, ingerimos datos de cada disparo e identificamos apuestas de valor esperado positivo en el momento en que las cuotas del operador divergen de las probabilidades del modelo.</p>
            <h2 class="pr-h2">Qué hacemos</h2>
            <p>Cada pick generado por nuestro modelo se registra en el instante en que aparece el valor — nunca se edita, nunca se borra. Publicamos la cuota de entrada, el estado en vivo del partido en el momento del pick, el resultado en curso y el resumen de beneficio/pérdida ya liquidado. El histórico completo está en <a href="/es/historial">webpronos.com/es/historial</a>.</p>
            <h2 class="pr-h2">Cómo funciona el modelo</h2>
            <p>El motor central es un modelo xG entrenado con datos a nivel de disparo. Para cada disparo en un partido, el modelo calcula la probabilidad de gol esperado en base a la posición, parte del cuerpo, situación y presión defensiva. Esas probabilidades por disparo se agregan en probabilidades en vivo de victoria/empate/derrota, over/under y hándicap, que después se comparan con las cuotas en vivo del operador usando un cálculo de edge al estilo Benter. Cualquier mercado donde la probabilidad implícita del modelo supera a la del operador (tras ajuste de margen) se señala como un pick de valor.</p>
            <h2 class="pr-h2">Por qué publicamos todo</h2>
            <p>La mayoría de los tipsters seleccionan solo las victorias y esconden las derrotas. Al publicar cada pick liquidado — incluyendo los malos — permitimos que cualquiera audite nuestro edge. Si el ROI a largo plazo se mantiene positivo a lo largo de miles de picks, el modelo realmente está batiendo al mercado.</p>
            <p>Mira los <a href="/es/hoy">picks en vivo de hoy</a>, los <a href="/es/manana">partidos de mañana</a>, el <a href="/es/historial">histórico completo de auditoría</a> o lee nuestro <a href="/es/blog">blog</a> para explicaciones más profundas sobre xG.</p>
        """,
        },
        "/terms": {
            "title":       "Términos de Uso | WebPronos",
            "description": "Términos de Uso de WebPronos. Al usar nuestro sitio aceptas estos términos. Proporcionamos predicciones estadísticas de fútbol únicamente con fines informativos — no se garantiza ningún resultado.",
            "h1":          "Términos de Uso",
            "breadcrumb":  "Términos",
            "body":        """
            <p>Al acceder o usar WebPronos aceptas estos términos. WebPronos publica predicciones estadísticas de fútbol y datos históricos de rendimiento <strong>únicamente con fines informativos</strong>. No ofrecemos apuestas, juegos de azar ni asesoramiento financiero.</p>
            <h2 class="pr-h2">Ningún resultado garantizado</h2>
            <p>Cada predicción en WebPronos es una estimación probabilística generada por un modelo de IA basada en datos de partidos públicamente disponibles. No se garantiza ningún resultado. El rendimiento pasado — incluido cualquier ROI positivo mostrado en nuestro <a href="/es/historial">histórico</a> — no predice resultados futuros.</p>
            <h2 class="pr-h2">Responsabilidad del usuario</h2>
            <p>Eres el único responsable de cualquier decisión de apuesta que tomes. WebPronos no es un operador de apuestas y no asume responsabilidad por ninguna pérdida financiera derivada del uso de nuestras predicciones.</p>
            <h2 class="pr-h2">Restricción de edad</h2>
            <p>Debes tener al menos 18 años para usar este sitio. Consulta nuestra <a href="/es/juego-responsable">página de juego responsable</a> para recursos de ayuda y apoyo.</p>
            <h2 class="pr-h2">Propiedad intelectual</h2>
            <p>Las predicciones, conjuntos de datos y comentarios analíticos en WebPronos son propiedad intelectual de WebPronos. La reutilización con fines no comerciales está permitida con atribución y enlace de retorno a la página de origen.</p>
            <h2 class="pr-h2">Cambios en estos términos</h2>
            <p>Podemos actualizar estos términos periódicamente. Los cambios materiales se indicarán al principio de esta página con la fecha de entrada en vigor.</p>
        """,
        },
        "/privacy": {
            "title":       "Política de Privacidad | WebPronos",
            "description": "Política de privacidad de WebPronos. Recopilamos analíticas mínimas para mejorar el servicio. No vendemos datos de usuarios y cumplimos el RGPD/LOPDGDD.",
            "h1":          "Política de Privacidad",
            "breadcrumb":  "Privacidad",
            "body":        """
            <p>WebPronos respeta tu privacidad. Esta página explica qué datos recopilamos, por qué y cómo los tratamos.</p>
            <h2 class="pr-h2">Qué recopilamos</h2>
            <p>Recopilamos analíticas mínimas de primera parte: visualizaciones de página, referente, tipo de navegador y región geográfica aproximada. No recopilamos nombres, direcciones de correo electrónico ni datos de pago, a menos que los proporciones explícitamente (por ejemplo, al suscribirte al canal de Telegram).</p>
            <h2 class="pr-h2">Servicios de terceros</h2>
            <p>Parte del contenido se sirve a través de Cloudflare (CDN), Lovable (alojamiento) y Fly.io (backend). Estos proveedores pueden establecer cookies técnicas necesarias para el funcionamiento del sitio.</p>
            <h2 class="pr-h2">Tus derechos (RGPD/LOPDGDD)</h2>
            <p>Si estás en la UE/EEE (incluida España), tienes derecho a acceder, corregir o eliminar cualquier dato que tengamos sobre ti. Contáctanos a través del enlace en el pie de página.</p>
            <h2 class="pr-h2">Retención de datos</h2>
            <p>Los datos analíticos se retienen por un máximo de 12 meses y después se agregan y anonimizan automáticamente.</p>
        """,
        },
        "/responsible-gambling": {
            "title":       "Juego Responsable — Apuesta con Seguridad | WebPronos",
            "description": "Ayuda y recursos para apostar de forma segura. Si el juego ha dejado de ser divertido, busca ayuda. Solo +18. Recursos incluyen FEJAR, Jugadores Anónimos, BeGambleAware y GamCare.",
            "h1":          "Juego Responsable",
            "breadcrumb":  "Juego Responsable",
            "body":        """
            <p>WebPronos publica predicciones de fútbol únicamente con fines informativos y de entretenimiento. <strong>Las apuestas implican riesgo.</strong> Nunca apuestes más de lo que puedas perder.</p>
            <h2 class="pr-h2">Señales de un problema</h2>
            <ul>
              <li>Apostar más de lo previsto o intentar recuperar pérdidas</li>
              <li>Ocultar la actividad de apuestas a amigos y familiares</li>
              <li>Pedir dinero prestado para apostar</li>
              <li>Sentir ansiedad o depresión a causa de las apuestas</li>
              <li>Que las apuestas interfieran con el trabajo, los estudios o las relaciones</li>
            </ul>
            <h2 class="pr-h2">Dónde obtener ayuda</h2>
            <ul>
              <li><strong>FEJAR — Federación Española de Jugadores de Azar Rehabilitados</strong> — <a href="https://fejar.org" rel="noopener noreferrer">fejar.org</a> · 900 200 225</li>
              <li><strong>Jugadores Anónimos España</strong> — <a href="https://www.jugadoresanonimos.org" rel="noopener noreferrer">jugadoresanonimos.org</a></li>
              <li><strong>Juego Responsable (DGOJ)</strong> — <a href="https://www.juegoresponsable.es" rel="noopener noreferrer">juegoresponsable.es</a></li>
              <li><strong>BeGambleAware</strong> (Internacional) — <a href="https://www.begambleaware.org" rel="noopener noreferrer">begambleaware.org</a></li>
              <li><strong>GamCare</strong> (Reino Unido) — <a href="https://www.gamcare.org.uk" rel="noopener noreferrer">gamcare.org.uk</a></li>
            </ul>
            <h2 class="pr-h2">Apuesta con sentido común</h2>
            <p>Define un presupuesto diario/semanal y respétalo. Toma descansos regulares. Trata las apuestas como entretenimiento — no como una forma de ganar dinero. Si tienes dificultades, contacta con uno de los recursos anteriores.</p>
            <p><strong>Solo +18.</strong> Si sospechas que tienes un problema con las apuestas, detente inmediatamente y busca ayuda.</p>
        """,
        },
    },
}


def _get_static_page(path: str, locale: str = "en") -> dict | None:
    """Lookup static page content for the requested locale with EN fallback.
    Returns the page dict (title/description/h1/breadcrumb/body) or None
    if `path` isn't a known static page in any locale.
    """
    bucket = _STATIC_PAGES_I18N.get(locale) or {}
    if path in bucket:
        # Merge so any keys missing in the translation fall back to EN —
        # safety net while translations are being rolled out incrementally.
        merged = dict(_STATIC_PAGES.get(path) or {})
        merged.update(bucket[path])
        return merged
    return _STATIC_PAGES.get(path)


def _render_static_page(path: str, locale: str = "en") -> str:
    """
    SSR for /about, /terms, /privacy, /responsible-gambling.

    Replaces the old `_render_passthrough` for these pages, which served
    the Lovable SPA shell with the WRONG canonical (pointing to homepage)
    and the WRONG title (homepage title). Google interpreted this as
    duplicate content and would deindex these URLs.

    Now each page gets its own title, description, self-canonical, body
    copy, and BreadcrumbList JSON-LD.
    """
    page = _get_static_page(path, locale)
    if not page:
        return _render_passthrough(path, locale)
    # Locale-aware canonical. The URL slug itself may also be localized:
    # /about → /sobre on pt-br, /historico, /jogo-responsavel, etc.
    lang_prefix = _LOCALE_TO_PREFIX.get(locale, "")
    localized_path = _localized_slug(path, locale)
    canonical = f"{SITE_URL}{lang_prefix}{localized_path}"
    root_url  = f"{SITE_URL}{lang_prefix}/"
    site_name = _t_site(locale, "breadcrumb_root")
    body = f"""
        <nav class="pr-nav">
          <a href="{root_url}">{site_name}</a> › {page['breadcrumb']}
        </nav>
        <h1 class="pr-h1">{page['h1']}</h1>
        {page['body']}
        {_render_pr_footer(locale)}
    """
    jsonld = json.dumps({
        "@context": "https://schema.org",
        "@type":    "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": site_name,         "item": root_url},
            {"@type": "ListItem", "position": 2, "name": page['breadcrumb'], "item": canonical},
        ],
    }, ensure_ascii=False)
    return _build_html_page(
        title       = page['title'],
        description = page['description'],
        canonical   = canonical,
        body_html   = body,
        jsonld      = jsonld,
        locale      = locale,
        stripped_path = path,
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

    # Result normalization: "win"/"green" → "win", "loss"/"red" → "loss",
    # "void"/"push" → "void" (settled with stake returned, not pending).
    # Anything else (including None) → None, which the frontend renders as
    # 'pending'. Without the explicit 'void' branch, Handicap pushes
    # (e.g. Team -1 ending in a 1-goal win) showed as PENDING forever.
    raw_result = (row["result"] or "").lower() if row["result"] else None
    if raw_result in ("win", "green"):
        result = "win"
    elif raw_result in ("loss", "red"):
        result = "loss"
    elif raw_result in ("void", "push"):
        result = "void"
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
        # League-wide on-pitch stats — same data the prerender uses to build
        # the SEO copy. Exposed here so Lovable can render the enriched view.
        performance = _league_performance(variants, recent_days=30)
        payload = {
            "league_name":  name,
            "slug":         slug,
            "country":      country_row["country"] if country_row else None,
            "logo_url":     _league_logo(name),
            "upcoming":     upcoming,
            "recent_picks": picks,
            "stats":        _calc_stats(picks),
            "performance":  performance,
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

    i18n: if the forwarded path starts with /pt|/br|/es the dispatcher
    detects the locale, strips the prefix, and routes to the matching
    EN renderer with `locale=...`. Renderers that have been refactored
    for i18n (static pages, _build_html_page) will honour it; others
    (dynamic SEO pages) fall back to EN body with translated chrome via
    _build_html_page's hreflang/<html lang> injection.
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

        # i18n: detect lang prefix and strip it before route matching.
        locale, path = _extract_lang_prefix(path)

        # Route patterns. `last_mod_ts` is set per path so each page gets
        # an accurate Last-Modified header — Googlebot can then short-circuit
        # subsequent crawls with If-Modified-Since on pages that haven't changed.
        last_mod_ts = _BUILD_TIME_TS
        if path == "/" or path == "":
            html = _render_homepage(locale)
            last_mod_ts = _newest_pick_ts()
        elif path == "/blog":
            html = _render_blog_listing(locale)
            # blog index lastmod = newest post's published_at (best-effort)
        elif _re.match(r'^/blog/[^/]+$', path):
            slug = path[len("/blog/"):]
            return prerender_blog(slug, locale)
        elif _re.match(r'^/match/\d+', path):
            mid = int(_re.match(r'^/match/(\d+)', path).group(1))
            return prerender_match(mid, locale)
        elif _re.match(r'^/team/[^/]+$', path):
            return _render_team(path[len("/team/"):], locale)
        elif _re.match(r'^/league/[^/]+$', path):
            return _render_league(path[len("/league/"):], locale)
        elif _re.match(r'^/tips/[^/]+$', path):
            return _render_tips_market(path[len("/tips/"):], locale)
        elif path == "/today":
            html = _render_today(locale)
            # Pages-of-fixtures recencey ≈ when the upcoming cache last
            # refreshed. _last_cycle_ts updates every BG cycle (~2 min).
            last_mod_ts = int(_last_cycle_ts) if _last_cycle_ts else _BUILD_TIME_TS
        elif path == "/history":
            html = _render_history(locale)
            last_mod_ts = _newest_pick_ts()  # newest pick anywhere
        elif path == "/tomorrow" or path == "/upcoming":
            html = _render_tomorrow(locale)
            last_mod_ts = int(_last_cycle_ts) if _last_cycle_ts else _BUILD_TIME_TS
        elif path in ("/about", "/terms", "/privacy", "/responsible-gambling"):
            html = _render_static_page(path, locale)
            # Static legal copy — only changes on deploy
        else:
            # Unknown path — pass through Lovable shell
            html = _render_passthrough(path, locale)

        return html, 200, {
            "Content-Type":  "text/html; charset=utf-8",
            "Cache-Control": "public, max-age=300",   # 5min cache
            "Last-Modified": _http_date(last_mod_ts),
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
    Initialize APScheduler for daily summary + admin stats messages.

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
    # Job 1 — Public daily summary at 23:55 Lisbon (subscribers)
    scheduler.add_job(
        _send_daily_summary_locked,
        trigger=CronTrigger(hour=23, minute=55, timezone='Europe/Lisbon'),
        id='daily_summary',
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
    # Job 2 — Admin-only daily stats at 09:00 Lisbon
    # Fires AFTER the public summary so admin sees yesterday's numbers in
    # the morning along with subscriber growth, attribution funnel, etc.
    scheduler.add_job(
        _send_admin_stats_locked,
        trigger=CronTrigger(hour=9, minute=0, timezone='Europe/Lisbon'),
        id='admin_stats',
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
    # Job 3 — Public daily preview at 12:00 Lisbon (teaser: top 3 matches of the day)
    # Designed to drive notification opt-ins and remind subscribers there's
    # action coming this afternoon/evening.
    scheduler.add_job(
        _send_daily_preview_locked,
        trigger=CronTrigger(hour=12, minute=0, timezone='Europe/Lisbon'),
        id='daily_preview',
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
    # Job 4 — X resolution worker every 5 minutes. Scans x_tweets for
    # rows whose underlying tip has settled and posts a reply-tweet
    # to the original (✅ won / ❌ lost). Cheap (1 query + 0-N replies),
    # safe to run on every worker (idempotent via resolution_tweet_id
    # check), no DB lock needed.
    scheduler.add_job(
        _x_resolve_settled_tips,
        trigger=CronTrigger(minute='*/5', timezone='Europe/Lisbon'),
        id='x_resolution',
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )
    # Job 5 — Hourly archive of local tips → Supabase tips_archive. Our
    # off-machine backup so a Fly volume loss never wipes the historical
    # track record again (see the 2026-05-20 incident comments above
    # _archive_tips_to_supabase). Runs at minute :07 to avoid clashing
    # with the on-the-hour barrage of cron jobs.
    scheduler.add_job(
        _archive_tips_to_supabase,
        trigger=CronTrigger(minute=7, timezone='Europe/Lisbon'),
        id='tips_archive_hourly',
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )
    # Job 6 — Proactive WC2026 fixtures cache refresh every 3h.
    # The knockout bracket "fills in" as the group stage progresses
    # (placeholders like W99 / L101 become real team names within
    # minutes of the deciding group-stage match ending). Without this
    # job partners would see stale brackets for up to 6h after a
    # bracket flip; with this job, worst case ~3h. Cheap (3-4 Sofascore
    # calls per refresh, no DB writes). Safe to run year-round —
    # outside the tournament it's just a no-op refresh of the same data.
    def _refresh_wc2026_cache():
        try:
            _fetch_wc2026_all_fixtures(force=True)
        except Exception as e:
            log.warning(f"WC2026 cache refresh failed: {e}")
    scheduler.add_job(
        _refresh_wc2026_cache,
        trigger=CronTrigger(hour='*/3', timezone='Europe/Lisbon'),
        id='wc2026_fixtures_refresh',
        replace_existing=True,
        misfire_grace_time=900,
        coalesce=True,
        max_instances=1,
    )
    # Job 7 — In-app Sofascore watchdog. Probes Sofascore from inside the
    # running machine every 5 min. After 3 consecutive failures (= ~15 min
    # confirmed block) AND 30-min cooldown elapsed, fires _failover_execute
    # which forks our volume to the next region in FAILOVER_REGION_POOL,
    # clones our machine there, waits for it to be healthy, and destroys
    # us. Replaces the external GitHub Actions watchdog (whose */30 cron
    # was being throttled to ~2h on the free tier — see the 2026-05-22
    # bom→lhr incident logged in failover_audit).
    #
    # Why only on the first worker? gunicorn runs 2 workers, each with its
    # own APScheduler. Without a DB lock both would fire the failover
    # simultaneously. We solve it cheaply: gate by FLY_MACHINE_ID +
    # `_failover_probe_job` itself uses `_FAILOVER_LOCK` so even if both
    # fire the lock prevents double-execution. Result: at most 1 probe
    # per machine per 5 min (2 with 2 workers, but both share state via
    # the lock so duplicates only cost a few HTTP calls — harmless).
    scheduler.add_job(
        _failover_probe_job,
        trigger=CronTrigger(minute='*/5', timezone='Europe/Lisbon'),
        id='sofascore_watchdog',
        replace_existing=True,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1,
    )
    scheduler.start()
    log.info("Scheduler started: daily summary 23:55 + admin stats 09:00 + daily preview 12:00 + X resolution */5min (Lisbon, 1h grace, DB-locked)")
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


def _build_daily_preview_message() -> str | None:
    """
    Build the "top 3 matches of the day" teaser message.

    Pulls today's monitored fixtures (UTC) from _upcoming_cache, sorts by
    league priority (Premier League before lower divisions) and then by
    kickoff time, and picks the top 3.

    Copy varies day-to-day via a deterministic rotation of intro/outro
    templates keyed on day-of-year — same day always renders the same
    template (idempotent), but consecutive days look different.

    Returns the HTML-formatted message string, or None if there are no
    eligible matches (in which case the caller should skip sending).
    """
    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    today_cache = _upcoming_cache.get(today_str)
    if not today_cache or not today_cache.get("matches"):
        log.info("_build_daily_preview_message: no upcoming cache for today, skipping")
        return None

    now_ts = int(now_utc.timestamp())

    candidates = []
    for m in today_cache["matches"]:
        ts = m.get("startTimestamp") or 0
        if ts <= now_ts:
            continue  # already started or in the past
        sk = m.get("_sport_key")
        if sk not in MONITORED_SPORT_KEYS:
            continue  # ignore unmonitored leagues
        candidates.append({
            "id":          m["id"],
            "home":        m.get("homeTeam", ""),
            "away":        m.get("awayTeam", ""),
            "tournament":  m.get("tournament", ""),
            "country":     m.get("country", ""),
            "kickoff_ts":  ts,
            "_priority":   _league_priority(sk),
        })

    if not candidates:
        log.info("_build_daily_preview_message: no upcoming monitored matches left today")
        return None

    candidates.sort(key=lambda x: (x["_priority"], x["kickoff_ts"]))
    top3 = candidates[:3]

    # Deterministic per-day template rotation (idempotent if re-sent same day)
    templates = [
        {
            "intro": "🔥 <b>Os 3 jogos mais aguardados de hoje:</b>",
            "outro": "💡 <i>Ativa as notificações do bot para receberes os picks no momento do kickoff.</i>",
        },
        {
            "intro": "⚡ <b>Em radar hoje:</b>",
            "outro": "🎯 <i>O modelo entra em modo live a cada apito inicial — não percas o ponto de entrada.</i>",
        },
        {
            "intro": "👀 <b>Os jogos que vamos seguir hoje:</b>",
            "outro": "🚀 <i>Quem está pronto? Ativa as notificações para receberes os picks ao vivo.</i>",
        },
        {
            "intro": "🎯 <b>Top 3 do dia:</b>",
            "outro": "📲 <i>Ativa os alertas e recebe cada value pick no segundo em que aparecer.</i>",
        },
        {
            "intro": "🏆 <b>Hoje em destaque no nosso radar:</b>",
            "outro": "💪 <i>Picks live geradas em tempo real — ativa as notificações para não falhares nenhuma.</i>",
        },
        {
            "intro": "📅 <b>Hoje na nossa lista:</b>",
            "outro": "🔔 <i>Notificações ativas = picks no momento exato. Não percas o edge.</i>",
        },
        {
            "intro": "⚽ <b>Os jogos de hoje que merecem atenção:</b>",
            "outro": "🚀 <i>O modelo entra ao vivo no apito inicial. Ativa as alertas para receberes o sinal.</i>",
        },
    ]
    day_of_year = now_utc.timetuple().tm_yday
    tpl = templates[day_of_year % len(templates)]

    lines = [tpl["intro"], ""]
    for i, m in enumerate(top3, 1):
        ko_str = datetime.fromtimestamp(m["kickoff_ts"], tz=timezone.utc).strftime("%H:%M UTC")
        flag = _country_flag(m["country"])
        # No CTA / link to webpronos here — this is a Telegram teaser, not
        # a traffic redirect. Keep the user inside the bot conversation so
        # the call-to-action of the outro (enabling notifications) lands.
        lines.append(f"{i}️⃣ {flag} <b>{m['home']} vs {m['away']}</b>")
        lines.append(f"   <i>{m['tournament']}</i> · {ko_str}")
        lines.append("")
    lines.append(tpl["outro"])
    return "\n".join(lines)


def _send_daily_preview_locked():
    """
    Daily noon teaser sender. Posts the top-3-matches preview to every
    Telegram subscriber. DB-locked via daily_preview_locks to keep behaviour
    idempotent across gunicorn workers (same pattern as the public summary).
    """
    from datetime import datetime
    lisbon_tz = pytz.timezone('Europe/Lisbon')
    today_str = datetime.now(lisbon_tz).strftime("%Y-%m-%d")
    try:
        msg = _build_daily_preview_message()
        if not msg:
            log.info(f"_send_daily_preview_locked: no message to send for {today_str}")
            return
        with _db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_preview_locks (
                    day TEXT PRIMARY KEY,
                    sent_at INTEGER NOT NULL
                )
            """)
            try:
                conn.execute(
                    "INSERT INTO daily_preview_locks (day, sent_at) VALUES (?, ?)",
                    (today_str, int(datetime.utcnow().timestamp()))
                )
            except sqlite3.IntegrityError:
                log.info(f"_send_daily_preview_locked: another worker already sent for {today_str}")
                return
        log.info(f"_send_daily_preview_locked: sending preview for {today_str}")
        _send_telegram(msg)
    except Exception as e:
        log.error(f"_send_daily_preview_locked error: {e}", exc_info=True)


def _send_admin_stats_locked():
    """
    Send the /admin_stats report to every chat_id in TELEGRAM_ADMIN_CHAT_IDS.

    Runs daily at 09:00 Lisbon (1h after the public summary at 23:55 the night
    before). Same DB-lock pattern as _send_daily_summary_locked to prevent
    duplicate sends across gunicorn workers.

    No-op if no admin chat IDs are configured (TELEGRAM_ADMIN_CHAT_IDS env
    var) — there's no public/general audience for this; the report contains
    subscriber breakdowns, attribution funnel, and other internal metrics.
    """
    from datetime import datetime
    if not TELEGRAM_ADMIN_CHAT_IDS:
        log.info("_send_admin_stats_locked: no TELEGRAM_ADMIN_CHAT_IDS configured, skipping")
        return
    lisbon_tz = pytz.timezone('Europe/Lisbon')
    today_str = datetime.now(lisbon_tz).strftime("%Y-%m-%d")
    try:
        with _db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_stats_locks (
                    day TEXT PRIMARY KEY,
                    sent_at INTEGER NOT NULL
                )
            """)
            try:
                conn.execute(
                    "INSERT INTO admin_stats_locks (day, sent_at) VALUES (?, ?)",
                    (today_str, int(datetime.utcnow().timestamp()))
                )
            except sqlite3.IntegrityError:
                log.info(f"_send_admin_stats_locked: another worker already sent for {today_str}")
                return
        # Lock acquired → build the same text /admin_stats produces, send it
        # to every configured admin chat_id. _send_telegram takes a single
        # chat_id at a time when the kwarg is provided.
        report = _tg_admin_stats()
        for cid in TELEGRAM_ADMIN_CHAT_IDS:
            try:
                _send_telegram(report, chat_id=cid)
            except Exception as e:
                log.error(f"_send_admin_stats_locked: send to {cid} failed: {e}")
        log.info(f"_send_admin_stats_locked: sent to {len(TELEGRAM_ADMIN_CHAT_IDS)} admin(s) for {today_str}")
    except Exception as e:
        log.error(f"_send_admin_stats_locked error: {e}", exc_info=True)


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
        # Same bootstrap restore as the gunicorn path — keeps dev/CLI
        # behaviour symmetric with prod.
        threading.Thread(target=_restore_tips_from_supabase, daemon=True,
                          name="tips-restore-bootstrap").start()
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
    # Auto-restore from Supabase IF the local tips table came up empty
    # (typically: fresh Fly volume after machine recycle / region change).
    # No-op if local has data already. Runs in a thread so it never
    # blocks startup — restore can take 5-10s for a full pull.
    threading.Thread(
        target=_restore_tips_from_supabase,
        daemon=True,
        name="tips-restore-bootstrap",
    ).start()
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
