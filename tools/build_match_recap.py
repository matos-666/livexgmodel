"""
Build an animated GIF recap of a finished match's algorithm performance.

Visual design (light theme, "video-player" feel):
  ┌──────────────────────────────────────────────┐
  │ 🇺🇸 LEAGUE · COUNTRY            [FINISHED]    │
  │ [logo] Home  H–A  Away [logo]                │
  ├──────────────────────────────────────────────┤
  │ XG TIMELINE REPLAY                            │
  │ How the algorithm tracked momentum            │
  │                                               │
  │ Step-line xG (home / away) + filled area      │
  │ Vertical dashed lines at each pick minute     │
  │ Star markers at the bottom for each goal      │
  │ HT marker at 45'                              │
  ├──────────────────────────────────────────────┤
  │ 28'  [HANDICAP] Vancouver -1  @3.25  ↔ VOID │
  │ 33'  [TOTAIS]  Under 4.5      @2.62  ✗ LOST │
  └──────────────────────────────────────────────┘

No WebPronos branding anywhere — the BetRadarAI bot must stay neutral.

Run standalone for mock test:
  python3 tools/build_match_recap.py <match_id> [/tmp/output.gif]
"""
import os, sys, io, sqlite3
from pathlib import Path
import urllib.request as _urllib

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib.patches import FancyBboxPatch
from PIL import Image

# ── Light theme palette ────────────────────────────────────────────────────
BG          = "#ffffff"
CARD        = "#ffffff"
SOFT        = "#f5f7fb"
INK         = "#1a2540"
MUTED       = "#6b7280"
SUBTLE      = "#d1d5db"
GRID        = "#e5e7eb"
HOME_CLR    = "#16a34a"   # green
AWAY_CLR    = "#f97316"   # orange
GOAL_CLR    = "#fbbf24"   # gold star
HT_CLR      = "#94a3b8"

# Result badges
WIN_BG, WIN_FG     = "#dcfce7", "#15803d"
LOSS_BG, LOSS_FG   = "#fee2e2", "#b91c1c"
VOID_BG, VOID_FG   = "#e5e7eb", "#6b7280"

# Market pill colours
MARKET_PILLS = {
    "Handicap":  ("#fed7aa", "#9a3412"),  # orange-tinted
    "1X2":       ("#dbeafe", "#1e40af"),  # blue
    "O/U 0.5":   ("#e9d5ff", "#6b21a8"),
    "O/U 1.5":   ("#e9d5ff", "#6b21a8"),
    "O/U 2.5":   ("#e9d5ff", "#6b21a8"),
    "O/U 3.5":   ("#e9d5ff", "#6b21a8"),
    "O/U 4.5":   ("#e9d5ff", "#6b21a8"),
    "BTTS":      ("#fbcfe8", "#9d174d"),
    "Draw No Bet":("#cffafe", "#155e75"),
}
MARKET_DISPLAY = {
    "Handicap": "HANDICAP",
    "1X2":      "1X2",
    "O/U 0.5":  "TOTAIS",
    "O/U 1.5":  "TOTAIS",
    "O/U 2.5":  "TOTAIS",
    "O/U 3.5":  "TOTAIS",
    "O/U 4.5":  "TOTAIS",
    "BTTS":     "AMBAS MARCAM",
    "Draw No Bet": "DRAW NO BET",
}

DB_DEFAULT = "/tmp/tips_prod.db"

# Logo cache so we don't re-download every animation render.
_LOGO_CACHE: dict[int, Image.Image] = {}


# ── Data loaders ───────────────────────────────────────────────────────────
def load_match(match_id: int, db_path: str = DB_DEFAULT) -> dict:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    g = con.execute(
        "SELECT id, home_team, away_team, home_goals, away_goals, "
        "       home_team_id, away_team_id, tournament, country, start_ts "
        "FROM games WHERE id = ?",
        (match_id,)
    ).fetchone()
    if g is None:
        raise ValueError(f"Match {match_id} not found")

    tips = con.execute(
        "SELECT market, label, odd_entry, edge_entry, minute_entry, result "
        "FROM tips WHERE match_id = ? AND result IN ('green','red','win','loss','void') "
        "ORDER BY minute_entry",
        (match_id,)
    ).fetchall()

    shots = con.execute(
        "SELECT minute, added_time, is_home, xg, is_goal, is_penalty "
        "FROM match_shots WHERE match_id = ? ORDER BY minute, added_time",
        (match_id,)
    ).fetchall()
    con.close()

    return {
        "match": dict(g),
        "tips":  [dict(r) for r in tips],
        "shots": [dict(r) for r in shots],
    }


def build_xg_step_arrays(shots: list, max_minute: int = 95):
    """Build step-line cumulative xG. Returns (xs, h, a) where each step is
    a discrete jump at the minute of a non-penalty shot."""
    events = []
    for s in shots:
        if s.get("is_penalty"):
            continue
        m = int(s["minute"]) + (int(s.get("added_time") or 0) // 60)
        m = max(0, min(max_minute, m))
        events.append((m, bool(s["is_home"]), float(s.get("xg") or 0)))
    events.sort()
    h_sum = a_sum = 0.0
    xs, h, a = [0], [0.0], [0.0]
    for m, is_home, xg in events:
        if is_home:
            h_sum += xg
        else:
            a_sum += xg
        xs.append(m); h.append(h_sum); a.append(a_sum)
    xs.append(max_minute); h.append(h_sum); a.append(a_sum)
    return xs, h, a


def goal_minutes(shots: list, max_minute: int = 95) -> tuple[list, list]:
    """Returns (home_goal_minutes, away_goal_minutes) excluding penalties so
    visual matches the headline scoreline derived from the same shots."""
    h, a = [], []
    for s in shots:
        if not s.get("is_goal"):
            continue
        m = int(s["minute"]) + (int(s.get("added_time") or 0) // 60)
        m = max(0, min(max_minute, m))
        if s["is_home"]:
            h.append(m)
        else:
            a.append(m)
    return h, a


def fetch_team_logo(team_id: int | None) -> Image.Image | None:
    """Download team crest from Sofascore CDN and cache it. Returns None on
    any failure — caller draws a fallback circle."""
    if not team_id:
        return None
    if team_id in _LOGO_CACHE:
        return _LOGO_CACHE[team_id]
    url = f"https://api.sofascore.app/api/v1/team/{team_id}/image"
    try:
        req = _urllib.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _urllib.urlopen(req, timeout=4) as resp:
            data = resp.read()
        img = Image.open(io.BytesIO(data)).convert("RGBA")
        # Resize to a reasonable working size (matplotlib downscales again)
        img.thumbnail((128, 128), Image.LANCZOS)
        _LOGO_CACHE[team_id] = img
        return img
    except Exception:
        return None


# ── Result helpers ─────────────────────────────────────────────────────────
def tip_profit(t: dict) -> float:
    r = (t["result"] or "").lower()
    if r in ("green", "win"):  return float(t["odd_entry"] or 0) - 1.0
    if r in ("red", "loss"):   return -1.0
    return 0.0


def result_chip(r: str) -> tuple[str, str, str]:
    r = (r or "").lower()
    if r in ("green", "win"):  return ("✓ WON",  WIN_BG,  WIN_FG)
    if r in ("red", "loss"):   return ("✗ LOST", LOSS_BG, LOSS_FG)
    return ("↔ VOID", VOID_BG, VOID_FG)


# ── Animation ──────────────────────────────────────────────────────────────
def build_recap(match_id: int, out_path: str = "/tmp/match_recap.gif",
                db_path: str = DB_DEFAULT, fps: int = 10) -> str:
    data   = load_match(match_id, db_path)
    g      = data["match"]
    tips   = data["tips"]
    shots  = data["shots"]

    max_min = 95
    xs, h_cum, a_cum = build_xg_step_arrays(shots, max_min)
    max_xg = max(0.5, max(max(h_cum), max(a_cum))) * 1.18
    goals_h, goals_a = goal_minutes(shots, max_min)

    home_name, away_name = g["home_team"], g["away_team"]
    score = f"{g['home_goals']} — {g['away_goals']}"

    # Fetch logos (best-effort, may be None)
    logo_home = fetch_team_logo(g.get("home_team_id"))
    logo_away = fetch_team_logo(g.get("away_team_id"))

    # ── Figure layout: header strip / chart / picks list ──────────────────
    fig = plt.figure(figsize=(6.8, 7.2), dpi=100, facecolor=BG)
    gs  = fig.add_gridspec(
        nrows=3, ncols=1,
        height_ratios=[0.22, 0.50, 0.28],
        hspace=0.18, left=0.06, right=0.96, top=0.96, bottom=0.04,
    )
    ax_head  = fig.add_subplot(gs[0]); ax_head.axis("off"); ax_head.set_facecolor(BG)
    ax_chart = fig.add_subplot(gs[1]); ax_chart.set_facecolor(SOFT)
    ax_list  = fig.add_subplot(gs[2]); ax_list.axis("off"); ax_list.set_facecolor(BG)

    # ── HEADER ────────────────────────────────────────────────────────────
    tournament_line = f"{g['tournament']}"
    if g.get("country"):
        tournament_line += f" · {g['country']}"
    ax_head.text(0.02, 0.86, tournament_line, transform=ax_head.transAxes,
                 color=MUTED, fontsize=9, fontweight="bold", va="top")
    # FINISHED chip top-right
    chip = FancyBboxPatch((0.86, 0.78), 0.13, 0.13,
                          boxstyle="round,pad=0.01,rounding_size=0.04",
                          transform=ax_head.transAxes,
                          facecolor=SOFT, edgecolor=SUBTLE, linewidth=1)
    ax_head.add_patch(chip)
    ax_head.text(0.925, 0.845, "FINISHED", transform=ax_head.transAxes,
                 color=MUTED, fontsize=7, fontweight="bold",
                 ha="center", va="center")

    # Team names + score row. Position logos with OffsetImage when available.
    # Layout columns (in axes coords): home logo 0.16 · home name 0.32 ·
    #                                  score 0.50 · away name 0.68 · away logo 0.84
    def _draw_logo(img: Image.Image | None, x: float, y: float, fallback_color: str):
        if img is not None:
            oi = OffsetImage(img, zoom=0.32)
            ab = AnnotationBbox(oi, (x, y), xycoords=ax_head.transAxes,
                                frameon=False, box_alignment=(0.5, 0.5))
            ax_head.add_artist(ab)
        else:
            ax_head.scatter([x], [y], s=900, transform=ax_head.transAxes,
                            facecolor=fallback_color, edgecolor=SUBTLE, linewidths=1.5)

    _draw_logo(logo_home, 0.18, 0.36, HOME_CLR)
    _draw_logo(logo_away, 0.82, 0.36, AWAY_CLR)

    # Home / away dot + name
    ax_head.text(0.32, 0.36, home_name, transform=ax_head.transAxes,
                 color=INK, fontsize=13, fontweight="bold", ha="left", va="center")
    ax_head.text(0.30, 0.36, "●", transform=ax_head.transAxes,
                 color=HOME_CLR, fontsize=10, ha="right", va="center")
    ax_head.text(0.68, 0.36, away_name, transform=ax_head.transAxes,
                 color=INK, fontsize=13, fontweight="bold", ha="right", va="center")
    ax_head.text(0.70, 0.36, "●", transform=ax_head.transAxes,
                 color=AWAY_CLR, fontsize=10, ha="left", va="center")

    # Score big
    ax_head.text(0.50, 0.36, score, transform=ax_head.transAxes,
                 color=INK, fontsize=22, fontweight="800",
                 ha="center", va="center")

    # ── CHART AXES ────────────────────────────────────────────────────────
    ax_chart.set_xlim(-2, max_min + 2)
    ax_chart.set_ylim(-max_xg * 0.06, max_xg)
    ax_chart.set_xticks([0, 15, 30, 45, 60, 75, 90])
    ax_chart.set_xticklabels(["0'", "15'", "30'", "45'", "60'", "75'", "90'"],
                              color=MUTED, fontsize=8)
    ax_chart.tick_params(axis="y", colors=MUTED, labelsize=7, length=0)
    # Custom y-ticks: only every 0.3
    yticks = []
    v = 0.0
    while v <= max_xg:
        yticks.append(round(v, 2)); v += 0.3
    ax_chart.set_yticks(yticks)
    for spine in ax_chart.spines.values():
        spine.set_color(GRID); spine.set_linewidth(0.8)
    ax_chart.spines["top"].set_visible(False)
    ax_chart.spines["right"].set_visible(False)
    ax_chart.grid(color=GRID, alpha=0.7, linestyle="--", linewidth=0.6, axis="y")
    ax_chart.set_axisbelow(True)
    ax_chart.set_title("XG TIMELINE REPLAY", loc="left", color=INK, fontsize=10,
                        fontweight="bold", pad=14, x=0.0, y=1.04)
    ax_chart.text(0.0, 1.02, "How the algorithm tracked momentum",
                  transform=ax_chart.transAxes, color=MUTED, fontsize=8,
                  ha="left", va="bottom", style="italic")

    # HT line (45')
    ax_chart.axvline(45, color=HT_CLR, linewidth=1, linestyle=":", alpha=0.6, zorder=1)
    ax_chart.text(45, max_xg * 0.97, "HT", color=MUTED, fontsize=7,
                  ha="center", va="top", alpha=0.7, fontweight="bold")

    # Static placeholders (filled per frame)
    home_line, = ax_chart.step([], [], where="post", color=HOME_CLR,
                                linewidth=2.2, zorder=4)
    away_line, = ax_chart.step([], [], where="post", color=AWAY_CLR,
                                linewidth=2.2, zorder=4)

    # Pre-render the pick vertical-dashed-lines + minute pills (alpha set per frame)
    pick_artists = []  # list of (vline, pill_patch, pill_text, minute)
    for t in tips:
        m = int(t["minute_entry"] or 0)
        clr = HOME_CLR if (t["result"] or "").lower() in ("green","win") else \
              LOSS_FG if (t["result"] or "").lower() in ("red","loss") else MUTED
        vl  = ax_chart.axvline(m, color=clr, linewidth=1.2, linestyle="--",
                                alpha=0, zorder=3)
        # Pill at top of chart
        pill = FancyBboxPatch((m - 4, max_xg * 0.86), 8, max_xg * 0.10,
                              boxstyle="round,pad=0.0,rounding_size=0.05",
                              facecolor="white", edgecolor=clr, linewidth=1.2,
                              alpha=0, zorder=5)
        ax_chart.add_patch(pill)
        ptxt = ax_chart.text(m, max_xg * 0.91, f"{m}'",
                             color=INK, fontsize=8, fontweight="bold",
                             ha="center", va="center", alpha=0, zorder=6)
        pick_artists.append((vl, pill, ptxt, m, clr))

    # Goal markers under the x-axis
    for m in goals_h:
        ax_chart.scatter([m], [-max_xg * 0.04], marker="*", s=120,
                          color=HOME_CLR, edgecolor=INK, linewidths=0.5, zorder=3)
    for m in goals_a:
        ax_chart.scatter([m], [-max_xg * 0.04], marker="*", s=120,
                          color=AWAY_CLR, edgecolor=INK, linewidths=0.5, zorder=3)

    # Play cursor (moving vertical line as time advances)
    play_cursor = ax_chart.axvline(0, color=INK, linewidth=1, alpha=0.25, zorder=2)

    # Legend manually drawn at top-right of chart
    ax_chart.text(0.99, 1.04, f"●", transform=ax_chart.transAxes,
                  color=HOME_CLR, fontsize=11, ha="right", va="bottom")
    ax_chart.text(0.99, 1.04, f"           {home_name}   ",
                  transform=ax_chart.transAxes, color=INK, fontsize=8,
                  ha="right", va="bottom")

    # ── PICKS LIST ────────────────────────────────────────────────────────
    # Render row layout once, set alpha 0; reveal per frame as match clock
    # passes each pick's minute_entry.
    list_artists = []  # list of (minute, list of artists to fade)
    n = max(1, len(tips))
    row_h   = 1.0 / (n + 0.4)   # uniform spacing
    for i, t in enumerate(tips):
        y = 1.0 - (i + 0.7) * row_h
        m = int(t["minute_entry"] or 0)
        market_disp = MARKET_DISPLAY.get(t["market"], (t["market"] or "?").upper())
        bg_clr, fg_clr = MARKET_PILLS.get(t["market"], ("#e5e7eb", "#374151"))
        chip_txt, chip_bg, chip_fg = result_chip(t["result"])
        odd = float(t["odd_entry"] or 0)

        # Minute column
        a_min = ax_list.text(0.015, y, f"{m}'", transform=ax_list.transAxes,
                              color=INK, fontsize=10, fontweight="bold",
                              ha="left", va="center", alpha=0)
        # Market pill
        pill_w, pill_h = 0.16, row_h * 0.62
        pill_x = 0.07
        pill = FancyBboxPatch((pill_x, y - pill_h / 2), pill_w, pill_h,
                              boxstyle="round,pad=0.005,rounding_size=0.025",
                              transform=ax_list.transAxes,
                              facecolor=bg_clr, edgecolor="none", alpha=0,
                              zorder=2)
        ax_list.add_patch(pill)
        a_market = ax_list.text(pill_x + pill_w / 2, y, market_disp,
                                transform=ax_list.transAxes,
                                color=fg_clr, fontsize=7, fontweight="bold",
                                ha="center", va="center", alpha=0)
        # Label
        a_label = ax_list.text(pill_x + pill_w + 0.02, y, str(t["label"] or ""),
                                transform=ax_list.transAxes,
                                color=INK, fontsize=10, fontweight="600",
                                ha="left", va="center", alpha=0)
        # Odd
        a_odd = ax_list.text(0.78, y, f"@{odd:.2f}",
                              transform=ax_list.transAxes,
                              color=MUTED, fontsize=9, fontweight="600",
                              ha="right", va="center", alpha=0,
                              family="monospace")
        # Result chip
        chip_w, chip_h = 0.18, row_h * 0.62
        chip_x = 0.80
        rchip = FancyBboxPatch((chip_x, y - chip_h / 2), chip_w, chip_h,
                                boxstyle="round,pad=0.005,rounding_size=0.025",
                                transform=ax_list.transAxes,
                                facecolor=chip_bg, edgecolor="none", alpha=0,
                                zorder=2)
        ax_list.add_patch(rchip)
        a_chip = ax_list.text(chip_x + chip_w / 2, y, chip_txt,
                               transform=ax_list.transAxes,
                               color=chip_fg, fontsize=8, fontweight="bold",
                               ha="center", va="center", alpha=0)
        list_artists.append((m, [a_min, pill, a_market, a_label, a_odd, rchip, a_chip]))

    # ── Frame plan ────────────────────────────────────────────────────────
    fps = int(fps)
    intro_frames = int(0.5 * fps)
    play_frames  = int(4.5 * fps)
    outro_frames = int(1.0 * fps)
    total_frames = intro_frames + play_frames + outro_frames

    def update(frame_i):
        # Determine current "match clock"
        if frame_i < intro_frames:
            t_minute = 0.0
        elif frame_i < intro_frames + play_frames:
            prog = (frame_i - intro_frames) / max(1, play_frames - 1)
            t_minute = prog * max_min
        else:
            t_minute = max_min

        play_cursor.set_xdata([t_minute, t_minute])

        # Filter the step arrays up to current minute (preserves last step)
        cutoff_idx = next((i for i, x in enumerate(xs) if x > t_minute), len(xs))
        # Plot from start to cutoff and add a final point at the cursor minute
        sub_xs = list(xs[:cutoff_idx])
        sub_h  = list(h_cum[:cutoff_idx])
        sub_a  = list(a_cum[:cutoff_idx])
        if sub_xs and sub_xs[-1] < t_minute:
            sub_xs.append(t_minute)
            sub_h.append(sub_h[-1])
            sub_a.append(sub_a[-1])
        home_line.set_data(sub_xs, sub_h)
        away_line.set_data(sub_xs, sub_a)

        # Reveal pick verticals + pills
        for (vl, pill, ptxt, m, clr) in pick_artists:
            alpha = 1.0 if m <= t_minute else 0.0
            vl.set_alpha(alpha * 0.85)
            pill.set_alpha(alpha)
            ptxt.set_alpha(alpha)

        # Reveal list rows
        for (m, artists) in list_artists:
            alpha = 1.0 if m <= t_minute else 0.0
            for a in artists:
                a.set_alpha(alpha)

        return []

    anim = FuncAnimation(fig, update, frames=total_frames, interval=1000 / fps, blit=False)
    writer = PillowWriter(fps=fps)
    anim.save(out_path, writer=writer, dpi=90, savefig_kwargs={"facecolor": BG})
    plt.close(fig)

    size_kb = Path(out_path).stat().st_size // 1024
    return f"{out_path} ({size_kb}KB · {total_frames} frames · {total_frames/fps:.1f}s)"


# ── CLI ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    match_id = int(sys.argv[1]) if len(sys.argv) > 1 else 15260790
    out_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/match_recap.gif"
    db_path  = os.environ.get("RECAP_DB", DB_DEFAULT)
    print("Building recap for match", match_id, "...")
    print(build_recap(match_id, out_path, db_path))
