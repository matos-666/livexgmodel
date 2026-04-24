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

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "85e4f12b9d76a7bb0464eeb802f6f388")
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
    # China
    "chinese super league":             "soccer_china_superleague",
    "csl":                              "soccer_china_superleague",
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
    # Normalizar minuto — nunca usar None ou 0 como elapsed (causaria divisão por 0
    # ou projeções astronómicas ao tratar todo o xG como ganho em 1 minuto)
    if minute is None or minute <= 0:
        minute = 45   # fallback seguro: assume que estamos a meio do jogo

    # Duração efetiva: 90 min (tempo regulamentar) ou 120 (prolongamento)
    # Para minutos > 90 (prolongamento) usamos a duração total real
    full_duration = 120 if minute > 90 else 90
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
            "isValue": value > 0.10,   # >10% edge required
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
                benter_spreads["market"] = "HCP"
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
    "soccer_argentina_primera_division",
    "soccer_chile_campeonato", "soccer_mexico_ligamx",
    "soccer_conmebol_copa_libertadores", "soccer_conmebol_copa_sudamericana",
    "soccer_conmebol_copa_america",
    # Asia
    "soccer_japan_j_league", "soccer_china_superleague",
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
            tip_key      TEXT PRIMARY KEY,
            match_id     INTEGER NOT NULL,
            market       TEXT NOT NULL,
            label        TEXT NOT NULL,
            odd_entry    REAL,
            odd_now      REAL,
            edge_entry   REAL,
            minute_entry INTEGER,
            wall_ts      INTEGER NOT NULL,
            result       TEXT DEFAULT NULL,
            FOREIGN KEY (match_id) REFERENCES games(id)
        );
        CREATE INDEX IF NOT EXISTS idx_tips_match ON tips(match_id);
        CREATE INDEX IF NOT EXISTS idx_games_finished ON games(is_finished);
        """)
    # Migration: add edge_entry column to existing DBs
    with _db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tips)").fetchall()]
        if "edge_entry" not in cols:
            conn.execute("ALTER TABLE tips ADD COLUMN edge_entry REAL")
            log.info("DB migration: added edge_entry column to tips")
    log.info(f"DB ready: {DB_PATH}")

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

GOAL_COOLDOWN_MINUTES = 4  # block new tips for this many minutes after a goal
HCP_MIN_GAP_MINUTES   = 8  # minimum minutes between HCP tips for the same team
MAX_TIPS_PER_GAME     = 6  # hard cap on tips per game

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
                  last_goal_minute=None) -> list:
    """
    Sync server-computed picks into the DB.
    Returns the full tip list for this match (including historical).
    """
    import re as _re
    now_ts = int(time.time())

    # Goal cooldown: suppress NEW tip insertions within GOAL_COOLDOWN_MINUTES of a goal
    in_cooldown = (
        last_goal_minute is not None
        and minute is not None
        and 0 <= (minute - last_goal_minute) < GOAL_COOLDOWN_MINUTES
    )
    if in_cooldown:
        log.info(f"match {match_id}: goal cooldown active (goal@{last_goal_minute}', now@{minute}') — skipping new tips")

    with _db() as conn:
        # Pre-load all existing tips for this game
        existing_all = conn.execute(
            "SELECT tip_key, market, label, minute_entry FROM tips WHERE match_id = ?",
            (match_id,)
        ).fetchall()
        existing_keys      = {r["tip_key"] for r in existing_all}
        existing_hcp_rows  = [r for r in existing_all if r["market"] == "HCP"]
        existing_hcp_canonical = {_hcp_canonical(r["label"]) for r in existing_hcp_rows}

        # O/U conflict index: line → set of directions already stored ("over"/"under")
        existing_ou = {}
        for r in existing_all:
            if r["market"].startswith("O/U"):
                m_ou = _re.match(r'^(Over|Under)\s+([\d.]+)$', r["label"], _re.IGNORECASE)
                if m_ou:
                    line = m_ou.group(2)
                    existing_ou.setdefault(line, set()).add(m_ou.group(1).lower())

        total_tips = len(existing_all)

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
                log.info(f"match {match_id}: tip cap ({MAX_TIPS_PER_GAME}) reached, skipping '{p['label']}'")
                continue

            # Goal cooldown
            if in_cooldown:
                continue

            # O/U conflict: block opposite direction on same line
            if p["market"].startswith("O/U"):
                m_ou = _re.match(r'^(Over|Under)\s+([\d.]+)$', p["label"], _re.IGNORECASE)
                if m_ou:
                    direction = m_ou.group(1).lower()
                    line      = m_ou.group(2)
                    opposite  = "under" if direction == "over" else "over"
                    if opposite in existing_ou.get(line, set()):
                        log.info(f"match {match_id}: skipping {p['label']} — opposite direction already stored for line {line}")
                        continue

            # HCP dedup: same canonical value already stored
            if p["market"] == "HCP":
                canon = _hcp_canonical(p["label"])
                if canon in existing_hcp_canonical:
                    log.info(f"match {match_id}: skipping duplicate HCP '{p['label']}'")
                    continue

                # HCP gap: same team, less than HCP_MIN_GAP_MINUTES ago
                hm = _re.search(r'([+-][\d.]+)$', p["label"])
                if hm:
                    team_part = p["label"][:p["label"].rfind(hm.group(0))].strip().lower()
                    for r in existing_hcp_rows:
                        rt = r["label"][:r["label"].rfind(_re.search(r'([+-][\d.]+)$', r["label"]).group(0))].strip().lower() \
                             if _re.search(r'([+-][\d.]+)$', r["label"]) else ""
                        if rt == team_part and r["minute_entry"] is not None:
                            gap = (minute or 0) - r["minute_entry"]
                            if 0 <= gap < HCP_MIN_GAP_MINUTES:
                                log.info(f"match {match_id}: skipping HCP '{p['label']}' — same team tipped {gap}' ago")
                                continue

            conn.execute("""
                INSERT INTO tips (tip_key, match_id, market, label,
                                  odd_entry, odd_now, edge_entry, minute_entry, wall_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, match_id, p["market"], p["label"],
                  p["odds"], p["odds"], p.get("edge"), minute, now_ts))
            existing_keys.add(key)
            total_tips += 1
            if p["market"] == "HCP":
                existing_hcp_canonical.add(_hcp_canonical(p["label"]))
                existing_hcp_rows.append({"label": p["label"], "minute_entry": minute,
                                          "market": "HCP", "tip_key": key})
            if p["market"].startswith("O/U"):
                m_ou = _re.match(r'^(Over|Under)\s+([\d.]+)$', p["label"], _re.IGNORECASE)
                if m_ou:
                    existing_ou.setdefault(m_ou.group(2), set()).add(m_ou.group(1).lower())

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
                out_map = {"home": match.get("homeTeam",""), "draw": "Empate", "away": match.get("awayTeam","")}
                for side, name in out_map.items():
                    if lbl.lower() in name.lower() or (len(lbl) > 3 and name.lower().startswith(lbl[:4].lower())):
                        new_result = "green" if side == ft else "red"
                        break

            # HCP — only at FT
            if mkt == "HCP" and finished and new_result is None:
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

def _extract_picks_from_odds(odds: dict, match: dict) -> list:
    """Extract value picks from pre-computed odds dict (mirrors frontend logic)."""
    picks = []
    if not odds or not odds.get("available"):
        return picks

    def valid_odds(o):
        od = o.get("bookieOdds", 0) or 0
        return ODDS_MIN_PICK <= od <= ODDS_MAX_PICK

    benter = odds.get("benter") or {}

    # 1X2
    bh = benter.get("h2h")
    if bh and bh.get("outcomes"):
        out_lbls = {
            "home": match.get("homeTeam", "Casa"),
            "draw": "Empate",
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
        hpt_h = (("+" if (bs.get("homePoint") or 0) >= 0 else "") + str(bs.get("homePoint", ""))) if bs.get("homePoint") is not None else ""
        hpt_a = (("+" if (bs.get("awayPoint") or 0) >= 0 else "") + str(bs.get("awayPoint", ""))) if bs.get("awayPoint") is not None else ""
        sp_lbl = {
            "home": f"{match.get('homeTeam','Casa')} {hpt_h}".strip(),
            "away": f"{match.get('awayTeam','Fora')} {hpt_a}".strip(),
        }
        for k, o in bs["outcomes"].items():
            if o.get("isValue") and valid_odds(o):
                picks.append({
                    "market": "HCP", "label": sp_lbl.get(k, k),
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
            minute = m.get("minute") or 0
            picks  = _extract_picks_from_odds(odds, m) if odds else []
            last_goal_minute = incidents.get("lastGoalMinute") if incidents else None
            tips   = _sync_tips_db(mid, picks, minute, odds or {}, last_goal_minute)
            _auto_resolve_db(mid, m, incidents)

            # Re-read tips after resolution
            with _db() as conn:
                tips = [dict(t) for t in conn.execute(
                    "SELECT * FROM tips WHERE match_id = ? ORDER BY wall_ts", (mid,)
                ).fetchall()]

            new_state[mid] = {
                "match":     m,
                "shots":     shots,
                "incidents": incidents,
                "odds":      odds,
                "tips":      tips,
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
                    out_map = {"home": r["home_team"], "draw": "Empate", "away": r["away_team"]}
                    for side, name in out_map.items():
                        if lbl.lower() in name.lower() or (len(lbl) > 3 and name.lower().startswith(lbl[:4].lower())):
                            new_result = "green" if side == ft else "red"
                            break
                elif mkt == "HCP":
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
    "premier league": {"england", "english", "uk"},
    "championship": {"england", "english"},
    "efl": {"england", "english"},
    # Spain
    "la liga": None,
    "laliga": None,
    "primera division": {"spain", "spanish"},
    # Italy
    "serie a": {"italy", "italian"},
    # Germany
    "bundesliga": {"germany", "german"},
    # France
    "ligue 1": {"france", "french"},
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
    "liga profesional": None,
    "primera división": {"argentina", "chile"},
    "copa libertadores": None,
    "conmebol libertadores": None,
    "copa sudamericana": None,
    "conmebol sudamericana": None,
    "copa america": None,
    "liga mx": None,
    "liga bbva": None,
    "campeonato nacional": {"chile"},
    "brasileirão série b": None,
    # Asia
    "j1 league": None,
    "chinese super league": None,
}

_YOUTH_KEYWORDS = {"u23","u21","u20","u19","u18","u17","u15","youth","reserve","b team"}

# Tournament fragments that always mean NOT a monitored competition
_BLOCKED_TOURNAMENT_FRAGMENTS = {
    "série d", "serie d", "série c", "serie c",   # Brazil lower divisions
    "série a2", "série a3", "serie a2", "serie a3",
    "paulista", "carioca", "gaúcho", "mineiro", "baiano",  # Brazil state leagues
    "ligapro",                                    # Ecuador
    "usl", "nisa",                                # US non-MLS
    "frauen",                                     # Women's competitions
}

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
    # Exclude explicitly blocked fragments (checked on raw name)
    for frag in _BLOCKED_TOURNAMENT_FRAGMENTS:
        if frag in raw:
            return False
    # Check against strict keyword map
    for kw, allowed_countries in sorted(_MONITORED_LEAGUE_STRICT_KEYWORDS.items(), key=lambda x: -len(x[0])):
        if kw in t:
            if allowed_countries is None:
                return True
            for ac in allowed_countries:
                if ac in c or ac in t:
                    return True
            return False  # keyword found but country doesn't match
    return False


@app.route("/api/today/monitored")
def r_today_monitored():
    """Today\'s scheduled games for monitored leagues only (strict filter)."""
    try:
        all_today = get_scheduled()
        result = []
        for m in all_today:
            if m.get("isFinished") or m.get("isLive"):
                continue
            if _is_monitored_league_strict(m.get("tournament",""), m.get("country","")):
                sk = _resolve_sport_key(m.get("tournament",""), m.get("country",""))
                m["_sport_key"] = sk
                result.append(m)
        result.sort(key=lambda m: m.get("startTimestamp") or 0)
        return jsonify({"count": len(result), "matches": result})
    except Exception as e:
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
                "SELECT * FROM tips WHERE match_id = ? ORDER BY wall_ts", (g["id"],)
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



# ── Team Logos ──
_LOGOS_SHEET = (
    "https://docs.google.com/spreadsheets/d/"
    "1tDUlWmZZcJKXHd0Nlr5QIm1V15OMsvkOgfhXUuPI9_M/"
    "gviz/tq?tqx=out:csv&sheet=footballstats+team+logo"
)
_logos_cache: dict = {}   # name → url
_logos_ts: float  = 0.0
_LOGOS_TTL = 86400         # refresh every 24 h

def _load_logos():
    global _logos_cache, _logos_ts
    import csv, io
    try:
        resp = _session.get(_LOGOS_SHEET, timeout=30)
        resp.raise_for_status()
        reader = csv.reader(io.StringIO(resp.text))
        logos = {}
        for row in reader:
            # Two paired columns: (col0=name, col1=url) and (col3=name, col5=url)
            for name_i, url_i in [(0, 1), (3, 5)]:
                if len(row) > url_i:
                    name = row[name_i].strip()
                    url  = row[url_i].strip()
                    if name and url.startswith("http"):
                        logos[name] = url
        _logos_cache = logos
        _logos_ts    = time.time()
        log.info(f"Team logos loaded: {len(logos)} entries")
    except Exception as e:
        log.error(f"Failed to load team logos: {e}")

def _get_logos():
    if time.time() - _logos_ts > _LOGOS_TTL or not _logos_cache:
        _load_logos()
    return _logos_cache

@app.route("/api/team_logos")
def r_team_logos():
    return jsonify({"teams": _get_logos(), "count": len(_logos_cache)})


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
