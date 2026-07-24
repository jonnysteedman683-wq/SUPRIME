"""Light tests for the benchmark harness and SVG chart generator."""

from __future__ import annotations

import pytest

from suprime.bench import bench_broadcast, bench_convergence, bench_pushsum
from suprime.svg import html_report, line_chart


def test_line_chart_produces_svg():
    svg = line_chart([("a", [1, 2, 3], [1, 4, 9])], title="t", x_label="x", y_label="y")
    assert svg.startswith("<svg")
    assert "polyline" in svg
    assert svg.strip().endswith("</svg>")


def test_html_report_embeds_charts():
    chart = line_chart([("a", [0, 1], [0, 1])])
    html = html_report("Report", [chart])
    assert "<!doctype html>" in html
    assert "Report" in html
    assert "<svg" in html


@pytest.mark.asyncio
async def test_convergence_benchmark_runs():
    xs, ys = await bench_convergence(sizes=(3, 6), fanout=3, max_rounds=30)
    assert xs == [3, 6]
    assert all(r >= 1 for r in ys)  # everyone converged within the round budget


@pytest.mark.asyncio
async def test_pushsum_benchmark_error_decreases():
    xs, ys = await bench_pushsum(n=6, rounds=25)
    assert ys[-1] < ys[0]  # error shrinks as it converges


@pytest.mark.asyncio
async def test_broadcast_benchmark_plumtree_beats_flooding():
    (px, py), (fx, fy) = await bench_broadcast(sizes=(6, 12))
    # Plumtree sends far fewer payload copies than naive flooding
    for p, f in zip(py, fy):
        assert p < f
