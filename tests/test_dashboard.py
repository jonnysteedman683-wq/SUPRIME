"""Smoke tests for the live dashboard scenario runner."""

from __future__ import annotations

import pytest

from suprime.dashboard import SwarmDashboard


@pytest.mark.asyncio
async def test_dashboard_builds_and_renders():
    dash = SwarmDashboard(n_nodes=4)
    await dash.build()
    try:
        # a render must succeed and mention the core sections
        frame = dash.render()
        assert "SUPRIME swarm" in frame
        assert "push-sum" in frame
        assert "chaos" in frame
    finally:
        for node in dash._nodes:
            await node.stop()


@pytest.mark.asyncio
async def test_dashboard_short_run_completes():
    dash = SwarmDashboard(n_nodes=4)
    # a very short scripted run should exercise partition + heal without error
    await dash.run(duration=2.0)
    # after run, all nodes are stopped
    assert all(not n._running for n in dash._nodes)
