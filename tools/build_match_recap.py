"""
Build an animated GIF recap of a finished match's algorithm performance.

Pulls match + tips + shots from the production SQLite, builds a matplotlib
FuncAnimation showing:
  - Title bar (teams · final score · tournament)
  - Cumulative xG curves (home / away) over match minutes
  - Pick markers fading in at their entry minute
  - Running P&L counter that updates per pick result

Run standalone for mock test:
  python3 tools/build_match_recap.py <match_id> [/tmp/output.gif]

Output: GIF, ~600px wide, ~10 seconds, lt 3MB.
"""
import sys, sqlite3, os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.animation import FuncAnimation, PillowWriter

# ── Palette (matches the InBet dark theme used in the widgets) ─────────────
BG       = "#0c1126"
CARD     = "#181f38"
INK      = "#ffffff"
MUTED    = "#8b95a9"
ACCENT   = "#ff8a1e"   # InBet orange
GREEN    = "#22c55e"
RED      = "#ef4444"
HOME_CLR = "#22c55e"
AWAY_CLR = "#ef4444"

DB_DEFAULT = "/tmp/tips_prod.db"


# ── Data loaders ───────────────────────────────────────────────────────────
def load_match(match_id: int, db_path: str = DB_DEFAULT) -> dict:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    g = con.execute(
        "SELECT id, home_team, away_team, home_goals, away_goals, tournament, country "
        "FROM games WHERE id = ?",
        (match_id,)
    ).fetchone()
    if g is None:
        raise ValueError(f"Match {match_id} not found")

    tips = con.execute(
        "SELECT market, label, odd_entry, edge_entry, minute_entry, result "
        "FROM tips WHERE match_id = ? AND result IN ('green','red','win','loss') "
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


def build_xg_curves(shots: list, max_minute: int = 95) -> tuple[list, list, list]:
    """Return (xs, home_cum, away_cum) — three parallel arrays, minute by minute."""
    home_xg = [0.0] * (max_minute + 1)
    away_xg = [0.0] * (max_minute + 1)
    for s in shots:
        if s.get("is_penalty"):
            continue  # exclude penalties from cumulative xG
        m = int(s["minute"]) + (int(s.get("added_time") or 0) // 60)
        m = max(0, min(max_minute, m))
        if s["is_home"]:
            home_xg[m] += float(s["xg"] or 0)
        else:
            away_xg[m] += float(s["xg"] or 0)
    # cumulative
    h_cum, a_cum, h_sum, a_sum = [], [], 0.0, 0.0
    for i in range(max_minute + 1):
        h_sum += home_xg[i]; a_sum += away_xg[i]
        h_cum.append(round(h_sum, 3)); a_cum.append(round(a_sum, 3))
    xs = list(range(max_minute + 1))
    return xs, h_cum, a_cum


# ── Result helpers ─────────────────────────────────────────────────────────
def tip_profit(t: dict) -> float:
    if t["result"] in ("green", "win"):
        return float(t["odd_entry"] or 0) - 1.0
    if t["result"] in ("red", "loss"):
        return -1.0
    return 0.0


def fmt_market(m: str, label: str) -> str:
    short = {
        "Handicap": "AH",
        "1X2": "1X2",
        "O/U 0.5": "O0.5",
        "O/U 1.5": "O1.5",
        "O/U 2.5": "O2.5",
        "O/U 3.5": "O3.5",
        "O/U 4.5": "O4.5",
    }
    return f"{short.get(m, m)} · {label}"


# ── Animation ──────────────────────────────────────────────────────────────
def build_recap(match_id: int, out_path: str = "/tmp/match_recap.gif",
                db_path: str = DB_DEFAULT, fps: int = 10) -> str:
    data   = load_match(match_id, db_path)
    g      = data["match"]
    tips   = data["tips"]
    shots  = data["shots"]

    max_min = 95
    xs, h_cum, a_cum = build_xg_curves(shots, max_min)
    max_xg = max(0.5, max(max(h_cum), max(a_cum)) * 1.15)

    total_profit = sum(tip_profit(t) for t in tips)
    home, away = g["home_team"], g["away_team"]
    score = f"{g['home_goals']}–{g['away_goals']}"

    # ── Figure layout ──────────────────────────────────────────────────────
    fig = plt.figure(figsize=(6.4, 5.6), dpi=90, facecolor=BG)
    gs = fig.add_gridspec(
        nrows=3, ncols=1,
        height_ratios=[0.18, 0.55, 0.27],
        hspace=0.20,
    )
    ax_header = fig.add_subplot(gs[0]); ax_header.set_facecolor(BG); ax_header.axis("off")
    ax_chart  = fig.add_subplot(gs[1]); ax_chart.set_facecolor(CARD)
    ax_list   = fig.add_subplot(gs[2]); ax_list.set_facecolor(BG); ax_list.axis("off")

    # Header
    ax_header.text(0.5, 0.78, f"{home}   {score}   {away}",
                   ha="center", va="center", color=INK, fontsize=18, fontweight="bold",
                   transform=ax_header.transAxes)
    ax_header.text(0.5, 0.32, g["tournament"], ha="center", va="center",
                   color=MUTED, fontsize=10, transform=ax_header.transAxes, style="italic")

    # Chart axes config
    ax_chart.set_xlim(0, max_min)
    ax_chart.set_ylim(0, max_xg)
    ax_chart.set_xticks([0, 15, 30, 45, 60, 75, 90])
    ax_chart.set_xticklabels(["0'", "15'", "30'", "45'", "60'", "75'", "90'"], color=MUTED, fontsize=8)
    ax_chart.tick_params(axis="y", colors=MUTED, labelsize=8)
    for spine in ax_chart.spines.values():
        spine.set_color("#252d4a"); spine.set_linewidth(0.8)
    ax_chart.grid(color="#252d4a", alpha=0.4, linestyle="-", linewidth=0.6)
    ax_chart.set_axisbelow(True)
    ax_chart.set_title("Momentum xG", loc="left", color=MUTED, fontsize=9,
                       fontweight="bold", pad=6, x=0.01)

    # Placeholders (filled per frame)
    home_line, = ax_chart.plot([], [], color=HOME_CLR, linewidth=2.5, label=home)
    away_line, = ax_chart.plot([], [], color=AWAY_CLR, linewidth=2.5, label=away)
    home_fill = ax_chart.fill_between([], [], color=HOME_CLR, alpha=0.14)
    away_fill = ax_chart.fill_between([], [], color=AWAY_CLR, alpha=0.14)
    pick_dots = ax_chart.scatter([], [], s=180, zorder=5,
                                 edgecolors=INK, linewidths=1.5)
    minute_marker = ax_chart.axvline(0, color=ACCENT, linewidth=1.2, alpha=0.7)

    # Legend (top-right inside chart)
    legend = ax_chart.legend(loc="upper left", frameon=False,
                              labelcolor=INK, fontsize=8)

    # Bottom list — render statically at the end; show progressively
    # Position lines manually so we can fade them in
    list_n = len(tips)
    line_ys = [0.78 - i * 0.18 for i in range(list_n)] if list_n else []
    line_texts = []
    for i, t in enumerate(tips):
        prof = tip_profit(t)
        sign = "+" if prof > 0 else ""
        result_emoji = "✓" if t["result"] in ("green", "win") else "✗"
        result_clr   = GREEN if t["result"] in ("green", "win") else RED
        # Build the row text in 3 segments so the result fragment has its own colour
        left_txt = f"  {t['minute_entry']}'   {fmt_market(t['market'], t['label'])}   @{t['odd_entry']:.2f}"
        right_txt = f"{sign}{prof:.2f}u  {result_emoji}"
        l = ax_list.text(0.02, line_ys[i], left_txt, transform=ax_list.transAxes,
                         color=INK, fontsize=10, alpha=0, family="monospace",
                         va="center")
        r = ax_list.text(0.98, line_ys[i], right_txt, transform=ax_list.transAxes,
                         color=result_clr, fontsize=10, alpha=0, family="monospace",
                         va="center", ha="right", fontweight="bold")
        line_texts.append((l, r))

    # Running P&L counter — bottom of list, centered
    pnl_text = ax_list.text(0.5, 0.02, "", transform=ax_list.transAxes,
                            ha="center", va="bottom", color=ACCENT,
                            fontsize=15, fontweight="bold")

    # ── Frame plan ─────────────────────────────────────────────────────────
    # Shorter than v1 to keep peak memory under the Fly 1GB cap during
    # GIF encoding (PillowWriter holds every frame in RAM until save).
    fps = int(fps)
    intro_frames = int(0.4 * fps)
    play_frames  = int(4.0 * fps)
    outro_frames = int(1.0 * fps)
    total_frames = intro_frames + play_frames + outro_frames

    def update(frame_i):
        nonlocal home_fill, away_fill
        # Phase detection
        if frame_i < intro_frames:
            t_minute = 0
        elif frame_i < intro_frames + play_frames:
            t_minute = ((frame_i - intro_frames) / max(1, play_frames - 1)) * max_min
        else:
            t_minute = max_min

        # Discrete index into the cumulative arrays
        idx = int(min(max_min, max(0, t_minute)))

        # Update xG curves up to current minute
        home_line.set_data(xs[: idx + 1], h_cum[: idx + 1])
        away_line.set_data(xs[: idx + 1], a_cum[: idx + 1])
        # Re-create fills
        for c in list(ax_chart.collections):
            if c is pick_dots: continue
            c.remove()
        ax_chart.fill_between(xs[: idx + 1], 0, h_cum[: idx + 1],
                              color=HOME_CLR, alpha=0.14)
        ax_chart.fill_between(xs[: idx + 1], 0, a_cum[: idx + 1],
                              color=AWAY_CLR, alpha=0.14)
        minute_marker.set_xdata([t_minute, t_minute])

        # Reveal picks whose minute_entry <= t_minute
        revealed = []
        running_pnl = 0.0
        for i, t in enumerate(tips):
            target_alpha = 1.0 if (t["minute_entry"] or 0) <= t_minute else 0.0
            line_texts[i][0].set_alpha(target_alpha)
            line_texts[i][1].set_alpha(target_alpha)
            if (t["minute_entry"] or 0) <= t_minute:
                revealed.append(t)
                running_pnl += tip_profit(t)

        # Pick markers on the chart at their (minute, mid-xg) coords
        if revealed:
            xys = []
            colors = []
            for t in revealed:
                m = int(t["minute_entry"] or 0)
                # plot dot slightly above the higher of two xG curves at that minute
                top = max(h_cum[m], a_cum[m])
                xys.append((m, min(top + 0.08, max_xg * 0.95)))
                colors.append(GREEN if t["result"] in ("green", "win") else RED)
            pick_dots.set_offsets(xys)
            pick_dots.set_color(colors)
        else:
            pick_dots.set_offsets([(0, -1)])

        # Running P&L
        sign = "+" if running_pnl > 0 else ""
        eur  = running_pnl * 100  # assume 1u = €100
        pnl_text.set_text(f"P&L: {sign}{running_pnl:.2f}u   ({sign}€{eur:,.0f})")

        # In outro phase, also briefly pulse the colour
        if frame_i >= intro_frames + play_frames:
            pulse = 1.0 + 0.04 * ((frame_i - intro_frames - play_frames) % 4)
            pnl_text.set_fontsize(15 * pulse)

        return [home_line, away_line, pick_dots, minute_marker, pnl_text]

    anim = FuncAnimation(fig, update, frames=total_frames, interval=1000 / fps, blit=False)

    writer = PillowWriter(fps=fps)
    anim.save(out_path, writer=writer, dpi=90, savefig_kwargs={"facecolor": BG})
    plt.close(fig)

    size_kb = Path(out_path).stat().st_size // 1024
    return f"{out_path} ({size_kb}KB · {total_frames} frames · {total_frames/fps:.1f}s)"


# ── CLI ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    match_id = int(sys.argv[1]) if len(sys.argv) > 1 else 15260790  # HamKam-Vålerenga mock
    out_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/match_recap.gif"
    db_path  = os.environ.get("RECAP_DB", DB_DEFAULT)
    print("Building recap for match", match_id, "...")
    print(build_recap(match_id, out_path, db_path))
