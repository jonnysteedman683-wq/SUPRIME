import sys
import asyncio
from unittest.mock import patch, MagicMock
from suprime.cli import _demo_handler, main

def test_demo_handler():
    task = MagicMock()

    # Test 'sum' op
    task.args = {"op": "sum", "values": [1, 2, 3]}
    assert _demo_handler(task) == 6

    # Test 'upper' op
    task.args = {"op": "upper", "text": "hello"}
    assert _demo_handler(task) == "HELLO"

    # Test fallback op
    task.args = {"op": "unknown", "other": "data"}
    assert _demo_handler(task) == {"op": "unknown", "other": "data"}

def ignore_coroutine(coro, *args, **kwargs):
    """Helper to silently consume coroutines so they don't warn about not being awaited."""
    if asyncio.iscoroutine(coro):
        coro.close()
    elif callable(coro) and asyncio.iscoroutinefunction(coro):
        pass # It's a function we don't call
    elif hasattr(coro, "__name__") and "mock" in coro.__name__.lower():
         pass # AsyncMock
    return None

@patch("asyncio.run")
def test_main_run(mock_run):
    mock_run.side_effect = ignore_coroutine
    main(["run"])
    mock_run.assert_called_once()

    # Also verify it runs with a keyboard interrupt
    def raise_interrupt(*args, **kwargs):
        if asyncio.iscoroutine(args[0]):
            args[0].close()
        raise KeyboardInterrupt()

    mock_run.side_effect = raise_interrupt
    main(["run"]) # Should not raise
    assert mock_run.call_count == 2

@patch("asyncio.run")
@patch("suprime.dashboard.run_dashboard")
def test_main_dashboard(mock_run_dashboard, mock_run):
    # run_dashboard is an async function, we mock it, which by default is AsyncMock in 3.8+
    # When asyncio.run is called with it, we need to close it if it's a coroutine.
    def close_and_return(*args, **kwargs):
        if asyncio.iscoroutine(args[0]):
             args[0].close()
    mock_run.side_effect = close_and_return

    main(["dashboard"])
    mock_run.assert_called_once()

    def raise_interrupt(*args, **kwargs):
        if asyncio.iscoroutine(args[0]):
            args[0].close()
        raise KeyboardInterrupt()

    mock_run.side_effect = raise_interrupt
    main(["dashboard"]) # Should not raise
    assert mock_run.call_count == 2

@patch("suprime.bench.main")
def test_main_bench(mock_bench):
    main(["bench"])
    mock_bench.assert_called_once()

@patch("asyncio.run")
def test_main_serve(mock_run):
    def close_and_return(*args, **kwargs):
        if asyncio.iscoroutine(args[0]):
             args[0].close()
    mock_run.side_effect = close_and_return

    main(["serve"])
    mock_run.assert_called_once()

    def raise_interrupt(*args, **kwargs):
        if asyncio.iscoroutine(args[0]):
            args[0].close()
        raise KeyboardInterrupt()

    mock_run.side_effect = raise_interrupt
    main(["serve"]) # Should not raise
    assert mock_run.call_count == 2

@patch("asyncio.run")
def test_main_no_args(mock_run):
    def close_and_return(*args, **kwargs):
        if asyncio.iscoroutine(args[0]):
             args[0].close()
    mock_run.side_effect = close_and_return
    with patch.object(sys, "argv", ["suprime", "run"]):
        main()
        mock_run.assert_called_once()
