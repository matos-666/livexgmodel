"""
Build an animated MP4/GIF of the day's cumulative P&L over the picks.

Visual: light theme, single green step-line that grows pick-by-pick.
  ┌──────────────────────────────────────────────┐
  │  RESUMO DIÁRIO              [11/05/2026]      │
  │  +€349.00                                     │
  ├──────────────────────────────────────────────┤
  │                       _.--                    │
  │                  __--                         │
  │              __--                             │
  │       __----                                  │
  │   __--                                        │
  │ 0 €                                           │
  │   1   2   3   4   5   6   7   8   9   10     │
  ├──────────────────────────────────────────────┤
  │   Wins: N · Losses: M · ROI: X%               │
  └──────────────────────────────────────────────┘
"""
import os, sys, sqlite3
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, FFMpegWriter, PillowWriter
from matplotlib.patches import FancyBboxPatch
from matplotlib.collections import PolyCollection

# ── Palette (matches the BetRadar match-recap to stay consistent) ──────────
BG          = "#ffffff"
CARD_BG     = "#ffffff"
SOFT        = "#f5f7fb"
INK         = "#1a2540"
MUTED       = "#6b7280"
SUBTLE      = "#d1d5db"
GRID        = "#e5e7eb"
GREEN       = "#16a34a"     # cumulative line when profitable
GREEN_FILL  = "#bbf7d0"     # soft fill above zero
RED         = "#dc2626"     # cumulative line when in red
RED_FILL    = "#fecaca"     # soft fill below zero

DB_DEFAULT  = "/tmp/tips_prod.db"


# ── Data loader ───────────────────────────────────────────────────────────
def load_day_tips(target_start_ts: int, target_end_ts: int,
                   db_path: str = DB_DEFAULT) -> list:
    """Return settled tips for the day, ordered by wall_ts (chronological).
    Each tip carries enough info to compute the running P&L per pick."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT t.market, t.label, t.odd_entry, t.minute_entry, t.result, "
        "       t.wall_ts, g.home_team, g.away_team, g.tournament, g.country "
        "FROM tips t LEFT JOIN games g ON g.id = t.match_id "
        "WHERE t.wall_ts >= ? AND t.wall_ts < ? "
        "  AND t.result IN ('green','red','win','loss') "
        "ORDER BY t.wall_ts ASC",
        (target_start_ts, target_end_ts)
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def tip_profit_eur(t: dict, stake_eur: float = 100.0) -> float:
    r = (t["result"] or "").lower()
    if r in ("green", "win"):  return (float(t["odd_entry"] or 0) - 1.0) * stake_eur
    if r in ("red", "loss"):   return -stake_eur
    return 0.0


# ── Builder ────────────────────────────────────────────────────────────────
def build_daily_recap(target_start_ts: int, target_end_ts: int,
                       date_label: str = "",
                       out_path: str = "/tmp/daily_recap.mp4",
                       db_path: str = DB_DEFAULT,
                       fps: int = 12,
                       stake_eur: float = 100.0,
                       header_label: str = "RESUMO DIÁRIO") -> str:
    """Build the cumulative-P&L animation for the window.

    target_start_ts / target_end_ts : window in unix seconds (Lisbon).
    date_label                      : text shown in the top-right chip.
    header_label                    : title rendered top-left. Override
                                       with 'RESUMO MENSAL' (or other) for
                                       monthly variants — same animation,
                                       different framing.
    """
    tips = load_day_tips(target_start_ts, target_end_ts, db_path)
    if not tips:
        raise ValueError("no settled tips in the requested day window")

    n = len(tips)
    wins   = sum(1 for t in tips if (t["result"] or "").lower() in ("green","win"))
    losses = n - wins

    # Cumulative profit and per-pick profit deltas (in €)
    deltas = [tip_profit_eur(t, stake_eur) for t in tips]
    cum    = [0.0]
    for d in deltas:
        cum.append(cum[-1] + d)
    total_profit = cum[-1]
    odd_sum = sum(float(t["odd_entry"] or 0) for t in tips)
    avg_odds = odd_sum / n if n else 0.0
    roi = (total_profit / (n * stake_eur) * 100) if n else 0.0

    # ── Figure layout ─────────────────────────────────────────────────────
    fig = plt.figure(figsize=(7.0, 6.4), dpi=160, facecolor=BG)
    gs  = fig.add_gridspec(
        nrows=3, ncols=1,
        height_ratios=[0.22, 0.62, 0.16],
        hspace=0.18, left=0.10, right=0.96, top=0.96, bottom=0.06,
    )
    ax_head  = fig.add_subplot(gs[0]); ax_head.axis("off"); ax_head.set_facecolor(BG)
    ax_chart = fig.add_subplot(gs[1]); ax_chart.set_facecolor(SOFT)
    ax_foot  = fig.add_subplot(gs[2]); ax_foot.axis("off"); ax_foot.set_facecolor(BG)

    # ── HEADER ────────────────────────────────────────────────────────────
    ax_head.text(0.02, 0.92, header_label, transform=ax_head.transAxes,
                  color=MUTED, fontsize=9, fontweight="bold", va="top")
    if date_label:
        chip = FancyBboxPatch((0.79, 0.82), 0.20, 0.16,
                              boxstyle="round,pad=0.01,rounding_size=0.04",
                              transform=ax_head.transAxes,
                              facecolor=SOFT, edgecolor=SUBTLE, linewidth=1)
        ax_head.add_patch(chip)
        ax_head.text(0.89, 0.90, date_label, transform=ax_head.transAxes,
                      color=MUTED, fontsize=8, fontweight="bold",
                      ha="center", va="center")

    # Big profit number — placeholder, will be set in update()
    sign = "+" if total_profit >= 0 else "−"
    profit_color = GREEN if total_profit >= 0 else RED
    profit_text = ax_head.text(0.02, 0.20, f"{sign}€{abs(total_profit):,.2f}",
                                transform=ax_head.transAxes,
                                color=profit_color, fontsize=34, fontweight="800",
                                ha="left", va="center")

    ax_head.text(0.98, 0.20, f"{n} picks · {wins}V – {losses}P",
                  transform=ax_head.transAxes,
                  color=MUTED, fontsize=10, fontweight="600",
                  ha="right", va="center")

    # ── CHART ─────────────────────────────────────────────────────────────
    ax_chart.set_xlim(0, max(2, n))
    y_min = min(cum) * 1.15 if min(cum) < 0 else -max(abs(total_profit) * 0.05, 20)
    y_max = max(cum) * 1.15 if max(cum) > 0 else 50
    ax_chart.set_ylim(y_min, y_max)
    ax_chart.set_xticks(range(0, n + 1))
    ax_chart.set_xticklabels([str(i) for i in range(0, n + 1)],
                              color=MUTED, fontsize=8)
    ax_chart.tick_params(axis="y", colors=MUTED, labelsize=8, length=0)
    # y formatter as €
    from matplotlib.ticker import FuncFormatter
    ax_chart.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"€{int(v)}"))
    for spine in ax_chart.spines.values():
        spine.set_color(GRID); spine.set_linewidth(0.8)
    ax_chart.spines["top"].set_visible(False)
    ax_chart.spines["right"].set_visible(False)
    ax_chart.grid(color=GRID, alpha=0.7, linestyle="--", linewidth=0.6, axis="y")
    ax_chart.set_axisbelow(True)
    ax_chart.set_title("Lucro acumulado", loc="left", color=INK, fontsize=10,
                        fontweight="bold", pad=12, x=0.0, y=1.03)
    ax_chart.text(0.0, 1.01, "Pick a pick, da primeira à última do dia",
                   transform=ax_chart.transAxes, color=MUTED, fontsize=8,
                   ha="left", va="bottom", style="italic")

    # Zero line
    ax_chart.axhline(0, color=SUBTLE, linewidth=1, linestyle="-", zorder=1)

    # Placeholders animated in update()
    line, = ax_chart.step(range(len(cum)), cum, where="post",
                            color=GREEN, linewidth=2.6, zorder=4)
    line.set_data([], [])
    # Markers for each settled pick — colour by result
    marker_artists = []
    for i, t in enumerate(tips, start=1):
        won = (t["result"] or "").lower() in ("green", "win")
        clr = GREEN if won else RED
        m = ax_chart.scatter([i], [cum[i]], s=70, color=clr,
                              edgecolor=INK, linewidths=0.6, zorder=5, alpha=0)
        marker_artists.append(m)

    # ── FOOTER (stats line) ───────────────────────────────────────────────
    ax_foot.text(0.5, 0.6,
                  f"ROI {roi:+.1f}%    ·    Odds médias {avg_odds:.2f}",
                  transform=ax_foot.transAxes,
                  color=INK, fontsize=10, fontweight="600",
                  ha="center", va="center")

    # ── Frame plan ────────────────────────────────────────────────────────
    fps = int(fps)
    intro_frames = int(0.5 * fps)
    play_frames  = int(4.0 * fps)
    outro_frames = int(1.0 * fps)
    total_frames = intro_frames + play_frames + outro_frames

    def update(frame_i):
        # Determine how many picks are "revealed"
        if frame_i < intro_frames:
            revealed = 0
            fract = 0.0
        elif frame_i < intro_frames + play_frames:
            prog = (frame_i - intro_frames) / max(1, play_frames - 1)
            scaled = prog * n
            revealed = int(scaled)
            fract = scaled - revealed
        else:
            revealed = n
            fract = 0.0

        # Build the partial curve up to the revealed index, with an
        # interpolated trailing point for smooth growth between picks.
        if revealed == 0 and fract == 0:
            xs, ys = [0], [0.0]
        else:
            xs = list(range(revealed + 1))     # 0..revealed inclusive
            ys = list(cum[: revealed + 1])
            if revealed < n and fract > 0:
                next_y = cum[revealed] + (cum[revealed + 1] - cum[revealed]) * fract
                xs.append(revealed + fract)
                ys.append(next_y)

        # Bi-color filled area: green above 0, red below 0. Re-drawn each
        # frame on the partial xs/ys. Previous PolyCollection fills are
        # removed before the new ones are added; scatter markers
        # (PathCollection) and the line are untouched.
        for c in list(ax_chart.collections):
            if isinstance(c, PolyCollection):
                c.remove()
        if len(xs) >= 2:
            ax_chart.fill_between(xs, 0, ys, step="post",
                                   where=[y >= 0 for y in ys],
                                   interpolate=False,
                                   color=GREEN_FILL, alpha=0.55, zorder=2)
            ax_chart.fill_between(xs, 0, ys, step="post",
                                   where=[y < 0 for y in ys],
                                   interpolate=False,
                                   color=RED_FILL, alpha=0.55, zorder=2)

        # Line: change colour according to CURRENT cumulative sign.
        current_p = ys[-1] if ys else 0.0
        line_color = GREEN if current_p >= 0 else RED
        line.set_data(xs, ys)
        line.set_color(line_color)

        # Show markers for revealed picks (kept above the fill)
        for i, m in enumerate(marker_artists, start=1):
            m.set_alpha(1.0 if i <= revealed else 0.0)

        # Header big number — colour-coded too
        s = "+" if current_p >= 0 else "−"
        profit_text.set_text(f"{s}€{abs(current_p):,.2f}")
        profit_text.set_color(line_color)
        return []

    anim = FuncAnimation(fig, update, frames=total_frames, interval=1000 / fps, blit=False)

    # Save MP4 if ffmpeg is around, otherwise GIF
    want_mp4 = out_path.endswith(".mp4") and FFMpegWriter.isAvailable()
    if want_mp4:
        writer = FFMpegWriter(
            fps=fps, codec="libx264", bitrate=2400,
            extra_args=["-pix_fmt", "yuv420p", "-preset", "medium",
                        "-movflags", "+faststart"],
        )
    else:
        if out_path.endswith(".mp4"):
            out_path = out_path[:-4] + ".gif"
        writer = PillowWriter(fps=fps)
    anim.save(out_path, writer=writer, dpi=160, savefig_kwargs={"facecolor": BG})
    plt.close(fig)

    size_kb = Path(out_path).stat().st_size // 1024
    return f"{out_path} ({size_kb}KB · {total_frames} frames · {total_frames/fps:.1f}s)"


# ── Static (PNG) recap — for monthly / long windows where animating 1000+
# picks blows past gunicorn's 30s worker timeout AND the 1 GB Fly memory
# cap. Same visual language as the animated daily, just frozen on the
# final state.
def build_static_recap(target_start_ts: int, target_end_ts: int,
                        date_label: str = "",
                        out_path: str = "/tmp/static_recap.png",
                        db_path: str = DB_DEFAULT,
                        stake_eur: float = 100.0,
                        header_label: str = "RESUMO MENSAL") -> str:
    """Render the final-state P&L chart as a single PNG. Used by the
    monthly recap path where animating 1000+ picks OOMs the Fly box."""
    tips = load_day_tips(target_start_ts, target_end_ts, db_path)
    if not tips:
        raise ValueError("no settled tips in the requested window")

    n = len(tips)
    wins   = sum(1 for t in tips if (t["result"] or "").lower() in ("green","win"))
    losses = n - wins
    deltas = [tip_profit_eur(t, stake_eur) for t in tips]
    cum    = [0.0]
    for d in deltas:
        cum.append(cum[-1] + d)
    total_profit = cum[-1]
    odd_sum = sum(float(t["odd_entry"] or 0) for t in tips)
    avg_odds = odd_sum / n if n else 0.0
    roi = (total_profit / (n * stake_eur) * 100) if n else 0.0

    fig = plt.figure(figsize=(7.0, 6.4), dpi=140, facecolor=BG)
    gs  = fig.add_gridspec(
        nrows=3, ncols=1,
        height_ratios=[0.22, 0.62, 0.16],
        hspace=0.18, left=0.10, right=0.96, top=0.96, bottom=0.06,
    )
    ax_head  = fig.add_subplot(gs[0]); ax_head.axis("off"); ax_head.set_facecolor(BG)
    ax_chart = fig.add_subplot(gs[1]); ax_chart.set_facecolor(SOFT)
    ax_foot  = fig.add_subplot(gs[2]); ax_foot.axis("off"); ax_foot.set_facecolor(BG)

    # Header
    ax_head.text(0.02, 0.92, header_label, transform=ax_head.transAxes,
                  color=MUTED, fontsize=9, fontweight="bold", va="top")
    if date_label:
        chip = FancyBboxPatch((0.74, 0.82), 0.25, 0.16,
                              boxstyle="round,pad=0.01,rounding_size=0.04",
                              transform=ax_head.transAxes,
                              facecolor=SOFT, edgecolor=SUBTLE, linewidth=1)
        ax_head.add_patch(chip)
        ax_head.text(0.865, 0.90, date_label, transform=ax_head.transAxes,
                      color=MUTED, fontsize=8, fontweight="bold",
                      ha="center", va="center")

    sign = "+" if total_profit >= 0 else "−"
    line_color = GREEN if total_profit >= 0 else RED
    ax_head.text(0.02, 0.20, f"{sign}€{abs(total_profit):,.2f}",
                  transform=ax_head.transAxes,
                  color=line_color, fontsize=34, fontweight="800",
                  ha="left", va="center")
    ax_head.text(0.98, 0.20, f"{n} picks · {wins}V – {losses}P",
                  transform=ax_head.transAxes,
                  color=MUTED, fontsize=10, fontweight="600",
                  ha="right", va="center")

    # Chart — full final curve at once.
    ax_chart.set_xlim(0, max(2, n))
    y_min = min(cum) * 1.15 if min(cum) < 0 else -max(abs(total_profit) * 0.05, 20)
    y_max = max(cum) * 1.15 if max(cum) > 0 else 50
    ax_chart.set_ylim(y_min, y_max)
    # X-axis: don't enumerate every pick for a month — just show 5 evenly-
    # spaced ticks so the axis stays readable.
    n_ticks = 5
    tick_idxs = [int(round(i * n / (n_ticks - 1))) for i in range(n_ticks)]
    ax_chart.set_xticks(tick_idxs)
    ax_chart.set_xticklabels([str(i) for i in tick_idxs], color=MUTED, fontsize=8)
    ax_chart.tick_params(axis="y", colors=MUTED, labelsize=8, length=0)
    from matplotlib.ticker import FuncFormatter
    ax_chart.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"€{int(v)}"))
    for spine in ax_chart.spines.values():
        spine.set_color(GRID); spine.set_linewidth(0.8)
    ax_chart.spines["top"].set_visible(False)
    ax_chart.spines["right"].set_visible(False)
    ax_chart.grid(color=GRID, alpha=0.7, linestyle="--", linewidth=0.6, axis="y")
    ax_chart.set_axisbelow(True)
    ax_chart.set_title("Lucro acumulado", loc="left", color=INK, fontsize=10,
                        fontweight="bold", pad=12, x=0.0, y=1.03)
    ax_chart.text(0.0, 1.01, "Pick a pick, da primeira à última",
                   transform=ax_chart.transAxes, color=MUTED, fontsize=8,
                   ha="left", va="bottom", style="italic")
    ax_chart.axhline(0, color=SUBTLE, linewidth=1, linestyle="-", zorder=1)

    xs = list(range(len(cum)))
    ax_chart.fill_between(xs, 0, cum, step="post",
                           where=[y >= 0 for y in cum],
                           interpolate=False,
                           color=GREEN_FILL, alpha=0.55, zorder=2)
    ax_chart.fill_between(xs, 0, cum, step="post",
                           where=[y < 0 for y in cum],
                           interpolate=False,
                           color=RED_FILL, alpha=0.55, zorder=2)
    ax_chart.step(xs, cum, where="post", color=line_color, linewidth=2.2, zorder=4)

    # Footer
    ax_foot.text(0.5, 0.6,
                  f"ROI {roi:+.1f}%    ·    Odds médias {avg_odds:.2f}",
                  transform=ax_foot.transAxes,
                  color=INK, fontsize=10, fontweight="600",
                  ha="center", va="center")

    if out_path.endswith(".mp4"):
        out_path = out_path[:-4] + ".png"
    fig.savefig(out_path, format="png", facecolor=BG, dpi=140,
                 bbox_inches=None)
    plt.close(fig)
    size_kb = Path(out_path).stat().st_size // 1024
    return f"{out_path} ({size_kb}KB · static)"


# ── CLI ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # CLI: build_daily_recap.py YYYY-MM-DD [out_path]
    from datetime import datetime, timedelta
    import pytz
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.utcnow().strftime("%Y-%m-%d")
    out_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/daily_recap.mp4"
    db_path  = os.environ.get("RECAP_DB", DB_DEFAULT)

    lisbon = pytz.timezone("Europe/Lisbon")
    y, m, d = map(int, date_str.split("-"))
    start = lisbon.localize(datetime(y, m, d))
    end   = start + timedelta(days=1)
    print(f"Building daily recap for {date_str}…")
    print(build_daily_recap(
        target_start_ts=int(start.timestamp()),
        target_end_ts=int(end.timestamp()),
        date_label=start.strftime("%d/%m/%Y"),
        out_path=out_path, db_path=db_path,
    ))
