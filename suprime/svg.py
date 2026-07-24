"""A tiny, dependency-free SVG line-chart generator.

Just enough to turn benchmark results into readable charts without pulling in a
plotting library. Produces a self-contained ``<svg>`` string with axes, gridlines,
labelled ticks, multiple colour-coded series and a legend.
"""

from __future__ import annotations

from typing import List, Sequence, Tuple

Series = Tuple[str, Sequence[float], Sequence[float]]  # (label, xs, ys)

_PALETTE = ["#2563eb", "#dc2626", "#16a34a", "#d97706", "#7c3aed", "#0891b2"]


def line_chart(
    series: List[Series],
    title: str = "",
    x_label: str = "",
    y_label: str = "",
    width: int = 640,
    height: int = 380,
) -> str:
    """Render ``series`` as an SVG line chart string."""
    pad_l, pad_r, pad_t, pad_b = 60, 130, 40, 50
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    all_x = [x for _, xs, _ in series for x in xs] or [0, 1]
    all_y = [y for _, _, ys in series for y in ys] or [0, 1]
    xmin, xmax = min(all_x), max(all_x)
    ymin, ymax = min(all_y), max(all_y)
    if xmax == xmin:
        xmax += 1
    if ymax == ymin:
        ymax += 1
    ymin = min(ymin, 0)

    def sx(x: float) -> float:
        return pad_l + (x - xmin) / (xmax - xmin) * plot_w

    def sy(y: float) -> float:
        return pad_t + plot_h - (y - ymin) / (ymax - ymin) * plot_h

    parts: List[str] = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="system-ui,sans-serif">'
    )
    parts.append(f'<rect width="{width}" height="{height}" fill="white"/>')
    if title:
        parts.append(
            f'<text x="{width/2}" y="24" text-anchor="middle" '
            f'font-size="16" font-weight="600">{_esc(title)}</text>'
        )

    # gridlines + y ticks
    for i in range(5):
        gy = pad_t + plot_h * i / 4
        val = ymax - (ymax - ymin) * i / 4
        parts.append(
            f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{pad_l+plot_w}" y2="{gy:.1f}" '
            f'stroke="#e5e7eb"/>'
        )
        parts.append(
            f'<text x="{pad_l-8}" y="{gy+4:.1f}" text-anchor="end" '
            f'font-size="11" fill="#6b7280">{_fmt(val)}</text>'
        )
    # x ticks
    for i in range(5):
        gx = pad_l + plot_w * i / 4
        val = xmin + (xmax - xmin) * i / 4
        parts.append(
            f'<text x="{gx:.1f}" y="{pad_t+plot_h+18:.1f}" text-anchor="middle" '
            f'font-size="11" fill="#6b7280">{_fmt(val)}</text>'
        )

    # axes
    parts.append(
        f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" '
        f'y2="{pad_t+plot_h}" stroke="#374151"/>'
    )
    parts.append(
        f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t+plot_h}" '
        f'stroke="#374151"/>'
    )
    if x_label:
        parts.append(
            f'<text x="{pad_l+plot_w/2}" y="{height-10}" text-anchor="middle" '
            f'font-size="12" fill="#374151">{_esc(x_label)}</text>'
        )
    if y_label:
        parts.append(
            f'<text x="16" y="{pad_t+plot_h/2}" text-anchor="middle" font-size="12" '
            f'fill="#374151" transform="rotate(-90 16 {pad_t+plot_h/2})">{_esc(y_label)}</text>'
        )

    # series
    for idx, (label, xs, ys) in enumerate(series):
        color = _PALETTE[idx % len(_PALETTE)]
        pts = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in zip(xs, ys))
        parts.append(
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/>'
        )
        for x, y in zip(xs, ys):
            parts.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="2.5" fill="{color}"/>')
        ly = pad_t + 8 + idx * 20
        parts.append(
            f'<rect x="{pad_l+plot_w+16}" y="{ly-9}" width="12" height="12" fill="{color}"/>'
        )
        parts.append(
            f'<text x="{pad_l+plot_w+32}" y="{ly+1}" font-size="12" '
            f'fill="#374151">{_esc(label)}</text>'
        )

    parts.append("</svg>")
    return "".join(parts)


def _fmt(v: float) -> str:
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.2f}"


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def html_report(title: str, charts: List[str], notes: str = "") -> str:
    """Wrap SVG charts in a minimal self-contained HTML page."""
    body = "\n".join(f'<div class="card">{c}</div>' for c in charts)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{_esc(title)}</title>
<style>
 body{{font-family:system-ui,sans-serif;margin:0;background:#f9fafb;color:#111827}}
 header{{padding:24px 32px;background:#111827;color:#fff}}
 h1{{margin:0;font-size:20px}}
 .wrap{{padding:24px 32px;display:grid;gap:24px;grid-template-columns:repeat(auto-fit,minmax(420px,1fr))}}
 .card{{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:12px;box-shadow:0 1px 2px rgba(0,0,0,.04)}}
 .notes{{padding:0 32px 32px;color:#6b7280;max-width:900px;line-height:1.5}}
</style></head>
<body><header><h1>{_esc(title)}</h1></header>
<div class="wrap">{body}</div>
<div class="notes">{notes}</div>
</body></html>"""
