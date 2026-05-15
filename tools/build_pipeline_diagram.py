"""
Renders the Live xG Model pipeline funnel as a clean, vector-quality PNG.
Output: /tmp/pipeline_funnel.png  (also returned for embedding in the .docx)
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

# ── Colour palette (matches the .docx) ───────────────────────────────────────
NAVY    = '#0F3460'
GOLD    = '#F5C518'
DARK    = '#1A1A2E'
WHITE   = '#FFFFFF'
INK     = '#2D3748'
MUTED   = '#718096'
BG      = '#FFFFFF'

INGEST_BG  = '#EBF8FF';  INGEST_BORDER  = '#3182CE'
MODEL_BG   = '#FAF5FF';  MODEL_BORDER   = '#805AD5'
DECIDE_BG  = '#FFFBEB';  DECIDE_BORDER  = '#D69E2E'
OUTPUT_BG  = '#F0FFF4';  OUTPUT_BORDER  = '#38A169'

# ── Stages ───────────────────────────────────────────────────────────────────
stages = [
    # (num, title, subtitle, category, bg, border)
    ('1', 'SCRAPER',           'curl_cffi → Sofascore · every 30 s',     INGEST_BG, INGEST_BORDER),
    ('2', 'INGESTION',         'Parse to normalized live-state object',   INGEST_BG, INGEST_BORDER),
    ('3', 'xG MODEL',          'Recalculates expected goals · shot map',  MODEL_BG,  MODEL_BORDER),
    ('4', 'PROBABILITY ENGINE','Converts xG → 1X2 · O/U · AH · BTTS',     MODEL_BG,  MODEL_BORDER),
    ('5', 'EDGE DETECTOR',     'Model prob. vs. bookmaker odds  ·  +4–6%',DECIDE_BG, DECIDE_BORDER),
    ('6', 'PERSISTENCE',       'SQLite — pick + full match snapshot',     OUTPUT_BG, OUTPUT_BORDER),
    ('7', 'DISTRIBUTION',      'Fan-out → REST · SSE · Telegram',         OUTPUT_BG, OUTPUT_BORDER),
    ('8', 'SETTLEMENT',        'Match finished → win / loss / push',      OUTPUT_BG, OUTPUT_BORDER),
]

# ── Figure ───────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 9), dpi=200)
ax.set_xlim(0, 10)
ax.set_ylim(0, 18)
ax.axis('off')
fig.patch.set_facecolor(BG)

# Title
ax.text(5, 17.4, 'LIVE xG MODEL — PIPELINE FUNNEL',
        ha='center', va='center', fontsize=14, fontweight='bold', color=NAVY)
ax.text(5, 16.9, 'From Sofascore data ingestion to multi-channel pick delivery',
        ha='center', va='center', fontsize=9, color=MUTED, style='italic')

# Vertical line backbone (for visual continuity)
backbone_x = 2.4
ax.add_line(Line2D([backbone_x, backbone_x], [1.0, 15.8],
                   color='#E2E8F0', linewidth=2, zorder=0))

# Stage boxes  (vertical stack, top to bottom)
box_w = 6.2
box_h = 1.35
gap   = 0.45
top_y = 15.6  # top edge of first box

for i, (num, title, sub, bg_color, border_color) in enumerate(stages):
    y_top    = top_y - i * (box_h + gap)
    y_center = y_top - box_h / 2
    x_left   = 3.2

    # The numbered circle on the backbone
    circle = patches.Circle((backbone_x, y_center), 0.34,
                            facecolor=NAVY, edgecolor=GOLD, linewidth=2.5, zorder=3)
    ax.add_patch(circle)
    ax.text(backbone_x, y_center, num,
            ha='center', va='center', fontsize=11, fontweight='bold',
            color=GOLD, zorder=4)

    # Stage card
    card = FancyBboxPatch(
        (x_left, y_top - box_h), box_w, box_h,
        boxstyle="round,pad=0.02,rounding_size=0.12",
        linewidth=1.5, edgecolor=border_color, facecolor=bg_color, zorder=2)
    ax.add_patch(card)

    # Title
    ax.text(x_left + 0.3, y_center + 0.22, title,
            ha='left', va='center', fontsize=11, fontweight='bold', color=INK)
    # Subtitle
    ax.text(x_left + 0.3, y_center - 0.22, sub,
            ha='left', va='center', fontsize=9, color=MUTED)

    # Connector arrow (down) between boxes — except after last
    if i < len(stages) - 1:
        arrow_top    = y_top - box_h - 0.05
        arrow_bottom = y_top - box_h - gap + 0.05
        ax.annotate('',
                    xy=(backbone_x, arrow_bottom),
                    xytext=(backbone_x, arrow_top),
                    arrowprops=dict(arrowstyle='-|>', lw=1.6, color=NAVY, zorder=1))

# ── Distribution fan-out (3 channels from stage 7) ───────────────────────────
stage7_idx = 6   # zero-indexed
y_top_s7   = top_y - stage7_idx * (box_h + gap)
y_center_s7 = y_top_s7 - box_h / 2

# Three small pill cards to the right of stage 7
pill_x   = 9.55
channels = [
    ('REST',      '/api JSON consumers',     '#3182CE'),
    ('SSE',       'Live iframe widgets',     '#805AD5'),
    ('Telegram',  'Eligible bot members',    '#38A169'),
]
pill_w   = 1.7
pill_h   = 0.55
pill_gap = 0.10
total_h  = len(channels) * pill_h + (len(channels) - 1) * pill_gap
first_y  = y_center_s7 + total_h / 2 - pill_h

for j, (label, desc, col) in enumerate(channels):
    y = first_y - j * (pill_h + pill_gap)
    pill = FancyBboxPatch(
        (pill_x, y), pill_w, pill_h,
        boxstyle="round,pad=0.02,rounding_size=0.10",
        linewidth=1.3, edgecolor=col, facecolor=WHITE, zorder=2)
    ax.add_patch(pill)
    ax.text(pill_x + pill_w / 2, y + pill_h * 0.66, label,
            ha='center', va='center', fontsize=8.5, fontweight='bold', color=col)
    ax.text(pill_x + pill_w / 2, y + pill_h * 0.27, desc,
            ha='center', va='center', fontsize=7, color=MUTED)

    # Connector from stage-7 card edge to pill
    ax.annotate('',
                xy=(pill_x - 0.02, y + pill_h / 2),
                xytext=(3.2 + box_w + 0.02, y_center_s7),
                arrowprops=dict(arrowstyle='-|>', lw=1.1, color=OUTPUT_BORDER,
                                connectionstyle='arc3,rad=0.0', zorder=1))

# ── Legend (category colour key) ─────────────────────────────────────────────
legend_y = 0.35
categories = [
    ('Data Intake', INGEST_BG, INGEST_BORDER),
    ('Modelling',   MODEL_BG,  MODEL_BORDER),
    ('Decision',    DECIDE_BG, DECIDE_BORDER),
    ('Output',      OUTPUT_BG, OUTPUT_BORDER),
]
total_w = 6.0
cat_w   = total_w / len(categories)
start_x = (10 - total_w) / 2

for k, (name, fc, ec) in enumerate(categories):
    cx = start_x + k * cat_w
    sw = FancyBboxPatch(
        (cx, legend_y), 0.32, 0.22,
        boxstyle="round,pad=0.0,rounding_size=0.05",
        linewidth=1.0, edgecolor=ec, facecolor=fc, zorder=2)
    ax.add_patch(sw)
    ax.text(cx + 0.42, legend_y + 0.11, name,
            ha='left', va='center', fontsize=9, color=INK, fontweight='bold')

# Footer note
ax.text(5, 0.05, 'A resilience layer (GitHub Actions watchdog) sits above the pipeline — auto-migrates the Fly.io machine to a working region if Sofascore blocks the current one.',
        ha='center', va='center', fontsize=7.5, color=MUTED, style='italic')

plt.tight_layout()
out_path = '/tmp/pipeline_funnel.png'
plt.savefig(out_path, dpi=200, bbox_inches='tight', facecolor=BG)
plt.close()
print(f'✅  Saved diagram: {out_path}')
