"""
Backfill home_team_id / away_team_id for all historical games.
Uses the Sofascore event endpoint (same API the server already uses).
Then downloads all logos to ./logos/ ready to commit to Lovable public/logos/.

Run: python backfill_team_ids.py
"""
import requests, time, os, json, re
from pathlib import Path

API_BASE   = "https://livexgmodel-pt.fly.dev"
SOFASCORE  = "https://api.sofascore.com/api/v1"
LOGO_DIR   = Path("./logos")
LOGO_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.sofascore.com/",
}

# ── 1. Get all game IDs from the server ──────────────────────────────────────
print("Fetching game list...")
resp = requests.get(f"{API_BASE}/api/state/tips?limit=2000", timeout=30)
games = resp.json().get("games", [])
print(f"  {len(games)} games in DB")

# ── 2. Fetch team IDs from Sofascore for each game ───────────────────────────
team_map = {}   # team_name → sofascore_id
updates  = []   # (match_id, home_team_id, away_team_id)

for i, game in enumerate(games):
    match_id   = game["id"]
    home_team  = game["home_team"]
    away_team  = game["away_team"]

    # Skip if already resolved both teams
    if home_team in team_map and away_team in team_map:
        updates.append((match_id, team_map[home_team], team_map[away_team]))
        continue

    try:
        url  = f"{SOFASCORE}/event/{match_id}"
        data = requests.get(url, headers=HEADERS, timeout=8).json()
        event = data.get("event", {})
        ht_id = event.get("homeTeam", {}).get("id")
        at_id = event.get("awayTeam", {}).get("id")
        ht_name = event.get("homeTeam", {}).get("name", home_team)
        at_name = event.get("awayTeam", {}).get("name", away_team)

        if ht_id:
            team_map[home_team]  = ht_id
            team_map[ht_name]    = ht_id
        if at_id:
            team_map[away_team]  = at_id
            team_map[at_name]    = at_id

        if ht_id and at_id:
            updates.append((match_id, ht_id, at_id))

        status = f"✓ {home_team} ({ht_id}) vs {away_team} ({at_id})"
    except Exception as e:
        status = f"✗ {home_team} vs {away_team}: {e}"

    print(f"  [{i+1}/{len(games)}] {status}")
    time.sleep(0.25)   # be polite to Sofascore

# Save team map
with open("team_ids.json", "w") as f:
    json.dump(team_map, f, indent=2, ensure_ascii=False)
print(f"\nSaved {len(team_map)} team IDs to team_ids.json")

# ── 3. Send updates to the server via new bulk-update endpoint ───────────────
print(f"\nSending {len(updates)} team ID updates to server...")
resp = requests.post(
    f"{API_BASE}/api/admin/backfill-team-ids",
    json={"updates": updates},
    timeout=30
)
print(f"  Server response: {resp.status_code} — {resp.text[:200]}")

# ── 4. Download logos ────────────────────────────────────────────────────────
unique_ids = set(team_map.values())
print(f"\nDownloading {len(unique_ids)} logos to ./logos/...")

ok = 0
fail = 0
for team_id in sorted(unique_ids):
    out_path = LOGO_DIR / f"{team_id}.png"
    if out_path.exists():
        ok += 1
        continue
    try:
        img_url = f"https://api.sofascore.app/api/v1/team/{team_id}/image"
        r = requests.get(img_url, headers=HEADERS, timeout=8)
        if r.status_code == 200 and r.headers.get("content-type","").startswith("image"):
            out_path.write_bytes(r.content)
            ok += 1
            print(f"  ✓ {team_id}.png ({len(r.content)//1024}KB)")
        else:
            fail += 1
            print(f"  ✗ {team_id}: HTTP {r.status_code}")
    except Exception as e:
        fail += 1
        print(f"  ✗ {team_id}: {e}")
    time.sleep(0.1)

print(f"\nLogos: {ok} downloaded, {fail} failed")
print(f"Ready to commit ./logos/ to Lovable public/logos/")
