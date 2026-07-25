from suprime.svg import line_chart, html_report, _fmt, _esc


def test_line_chart_basic():
    series = [
        ("Test Series 1", [1.0, 2.0, 3.0], [10.0, 20.0, 15.0]),
        ("Test Series 2", [1.0, 2.0, 3.0], [5.0, 25.0, 30.0]),
    ]
    svg = line_chart(
        series,
        title="Test Chart",
        x_label="X Axis",
        y_label="Y Axis",
        width=800,
        height=600,
    )

    # Check overall SVG structure
    assert '<svg xmlns="http://www.w3.org/2000/svg"' in svg
    assert 'width="800"' in svg
    assert 'height="600"' in svg
    assert 'viewBox="0 0 800 600"' in svg

    # Check labels
    assert 'Test Chart</text>' in svg
    assert 'X Axis</text>' in svg
    assert 'Y Axis</text>' in svg
    assert 'Test Series 1</text>' in svg
    assert 'Test Series 2</text>' in svg

    # Check series rendering elements (polylines and circles)
    assert '<polyline points=' in svg
    assert '<circle cx=' in svg


def test_line_chart_edge_cases():
    # Empty series
    svg_empty = line_chart([])
    assert '<svg' in svg_empty
    assert '<polyline' not in svg_empty

    # Single point
    series_single = [("Single", [1.0], [10.0])]
    svg_single = line_chart(series_single)
    assert '<svg' in svg_single
    assert '<circle' in svg_single

    # Negative values
    series_neg = [("Neg", [-5.0, 0.0, 5.0], [-10.0, -5.0, 0.0])]
    svg_neg = line_chart(series_neg)
    assert '<svg' in svg_neg
    assert '<polyline' in svg_neg
    # Min negative Y value should be reflected in ticks
    assert '-10' in svg_neg


def test_html_report_basic():
    charts = ["<svg>chart1</svg>", "<svg>chart2</svg>"]
    report = html_report("Test & Report", charts, "Some notes here.")

    assert '<!doctype html>' in report
    assert '<title>Test &amp; Report</title>' in report
    assert '<h1>Test &amp; Report</h1>' in report
    assert '<div class="card"><svg>chart1</svg></div>' in report
    assert '<div class="card"><svg>chart2</svg></div>' in report
    assert '<div class="notes">Some notes here.</div>' in report


def test_svg_esc():
    assert _esc("test") == "test"
    assert _esc("a < b") == "a &lt; b"
    assert _esc("a > b") == "a &gt; b"
    assert _esc("a & b") == "a &amp; b"
    assert _esc("<script>alert(1)&</script>") == "&lt;script&gt;alert(1)&amp;&lt;/script&gt;"


def test_svg_fmt():
    assert _fmt(1.0) == "1"
    assert _fmt(0.0) == "0"
    assert _fmt(-5.0) == "-5"
    assert _fmt(3.14159) == "3.14"
    assert _fmt(3.1) == "3.10"
    assert _fmt(0.0000000001) == "0"
