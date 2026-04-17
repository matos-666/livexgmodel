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

from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sofascore")

# ── CORS — permite pedidos do dashboard no Netlify (ou qualquer origem) ──
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
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

ODDS_API_KEY = "85e4f12b9d76a7bb0464eeb802f6f388"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

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
        ("matchbook",      STALE_MAX),
        ("coolbet",        STALE_MAX),
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
ODDS_CACHE_TTL = 120   # 2 min server-side cache per sport key + api key
_api_requests_remaining = None
_api_quotas = {}   # api_key → remaining (tracks quota per key independently)


def _get_odds_api(url, params=None, api_key=None):
    global _api_requests_remaining
    import requests as req

    effective_key = api_key or ODDS_API_KEY
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


def get_odds_for_sport(sport_key, force=False, api_key=None):
    now = time.time()
    cache_key = f"{sport_key}:{api_key or 'default'}"

    with _odds_cache_lock:
        cached = _odds_cache.get(cache_key)
        if cached and not force and (now - cached["ts"]) < ODDS_CACHE_TTL:
            log.info(f"Odds cache HIT for {cache_key} ({now - cached['ts']:.0f}s old)")
            return cached["data"]

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    data = _get_odds_api(url, {
        "regions": "eu",   # all regions — needed for SA, MLS, Asian leagues
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
    """Strip Sofascore suffixes like ', Group A', ', Phase 1', ', Round 2', etc."""
    import re
    # Remove trailing group/round/phase/stage/pool qualifiers (with or without comma)
    cleaned = re.sub(
        r'[,\s]+(group|grp|round|phase|stage|pool|matchday|md|jornada|giornata|journée|spieltag)\b.*$',
        '', name, flags=re.IGNORECASE
    ).strip()
    # Also remove trailing parenthetical qualifiers: "Premier League (Women)"
    cleaned = re.sub(r'\s*\(.*\)\s*$', '', cleaned).strip()
    return cleaned.lower()


def _resolve_sport_key(tournament_name, country=None):
    if not tournament_name:
        return None

    # Try with original name first, then with suffixes stripped
    for candidate in [tournament_name, _normalize_tournament(tournament_name)]:
        t = candidate.lower().strip()

        if t in TOURNAMENT_TO_SPORT_KEY:
            return TOURNAMENT_TO_SPORT_KEY[t]

        for keyword, sport_key in TOURNAMENT_TO_SPORT_KEY.items():
            if keyword in t:
                return sport_key

        if country:
            c = country.lower()
            combined = f"{c} {t}"
            for keyword, sport_key in TOURNAMENT_TO_SPORT_KEY.items():
                if keyword in combined:
                    return sport_key

    return None


def _extract_bookmaker_odds(bookmakers, market_key):
    priority = BOOKMAKER_PRIORITY.get(market_key, [])
    now_iso = datetime.now(timezone.utc)

    for bookie_key, max_stale in priority:
        for bm in bookmakers:
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
    if minute is None or minute <= 0:
        minute = 1

    remaining = max(90 - minute, 1)
    elapsed = min(minute, 90)

    home_rate = home_xg / elapsed if elapsed > 0 else 0
    away_rate = away_xg / elapsed if elapsed > 0 else 0

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
            "isValue": value > 0,
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

            odds_result["benter"] = {
                "h2h": benter_1x2,
                "totals": benter_totals,
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
        elapsed = max(0, now - period_ts) // 60
        if code == 6:
            minute = 1 + elapsed   # 1ª parte começa em 1'
        else:
            minute = 46 + elapsed  # 2ª parte começa em 46'

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
            "x": s.get("playerCoordinates", {}).get("x"),
            "y": s.get("playerCoordinates", {}).get("y"),
        }
        if p["isHome"]:
            hs.append(p); hx += p["xg"]
        else:
            aws.append(p); ax += p["xg"]

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


@app.route("/api/odds/quota")
def r_odds_quota():
    """Check remaining Odds API quota — per key if ?apiKey= provided."""
    api_key = flask_request.args.get("apiKey", "").strip() or None
    effective_key = api_key or ODDS_API_KEY
    rem = _api_quotas.get(effective_key, _api_requests_remaining)
    return jsonify({"remaining": rem, "key": effective_key[:8] + "…" if effective_key else None})


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
        print(f"  Client: {_client_type}")
        print(f"  Odds API: enabled")
        print(f"  Team aliases: {len(_team_aliases)} loaded\n")
        app.run(host="0.0.0.0", port=5050, debug=True)
else:
    # Running under gunicorn — __main__ block is skipped, so initialize here
    _load_aliases()
    threading.Thread(target=_init_client, daemon=True).start()
