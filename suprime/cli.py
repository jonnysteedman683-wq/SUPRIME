"""Command-line entry point for running a real SUPRIME node over TCP.

Examples::

    # Start the first node on a fixed port
    python -m suprime run --host 127.0.0.1 --port 7001 --id alpha

    # Start more nodes that bootstrap from it
    python -m suprime run --port 7002 --id beta  --seed 127.0.0.1:7001
    python -m suprime run --port 7003 --id gamma --seed 127.0.0.1:7001

Each node periodically prints its view of the swarm: live peers, the elected
leader and any replicated key/value state. Use ``--set k=v`` to seed data and
``--worker KIND`` to make the node execute a demo task kind.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from typing import List

from .node import SwarmNode
from .tasks import Task
from .transport import TcpTransport


def _demo_handler(task: Task):
    """A trivial handler used by ``--worker`` to demonstrate task execution."""
    op = task.args.get("op", "echo")
    if op == "sum":
        return sum(task.args.get("values", []))
    if op == "upper":
        return str(task.args.get("text", "")).upper()
    return task.args


async def _run(args: argparse.Namespace) -> None:
    transport = TcpTransport(host=args.host, port=args.port)
    node = SwarmNode(
        transport=transport,
        node_id=args.id,
        seeds=args.seed,
        gossip_interval=args.interval,
        fanout=args.fanout,
    )

    if args.worker:
        for kind in args.worker:
            node.tasks.register_handler(kind, _demo_handler)

    await node.start()
    print(f"[{node.id}] listening on {node.address}")

    for pair in args.set or []:
        if "=" in pair:
            key, value = pair.split("=", 1)
            node.store.set(key.strip(), value.strip())

    stop_event = asyncio.Event()

    def _request_stop(*_) -> None:
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:  # pragma: no cover - Windows
            pass

    async def _reporter() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(args.report)
            alive = [p.node_id for p in node.peers.alive()]
            print(
                f"[{node.id}] leader={node.leader} "
                f"peers={sorted(alive)} store={dict(node.store.items())}"
            )

    reporter = asyncio.ensure_future(_reporter())
    await stop_event.wait()
    reporter.cancel()
    await node.stop()
    print(f"[{node.id}] stopped")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="suprime", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run a swarm node over TCP")
    run.add_argument("--host", default="127.0.0.1", help="bind host")
    run.add_argument("--port", type=int, default=0, help="bind port (0 = auto)")
    run.add_argument("--id", default=None, help="explicit node id")
    run.add_argument(
        "--seed",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="seed peer address (repeatable)",
    )
    run.add_argument("--interval", type=float, default=0.5, help="gossip interval (s)")
    run.add_argument("--fanout", type=int, default=3, help="gossip fanout")
    run.add_argument("--report", type=float, default=2.0, help="status print interval (s)")
    run.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="seed a store entry (repeatable)",
    )
    run.add_argument(
        "--worker",
        action="append",
        default=[],
        metavar="KIND",
        help="register a demo handler for this task kind (repeatable)",
    )

    dash = sub.add_parser(
        "dashboard",
        help="run a live in-process chaos swarm with a terminal dashboard",
    )
    dash.add_argument("--nodes", type=int, default=6, help="number of nodes")
    dash.add_argument("--duration", type=float, default=30.0, help="run time (s)")

    bench = sub.add_parser(
        "bench",
        help="run scale/performance benchmarks and write an HTML chart report",
    )
    bench.add_argument("--out", default="bench_report.html", help="report output path")
    return parser


def main(argv: List[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        try:
            asyncio.run(_run(args))
        except KeyboardInterrupt:  # pragma: no cover
            pass
    elif args.command == "dashboard":
        from .dashboard import run_dashboard

        try:
            asyncio.run(run_dashboard(n_nodes=args.nodes, duration=args.duration))
        except KeyboardInterrupt:  # pragma: no cover
            pass
    elif args.command == "bench":
        from .bench import main as bench_main

        bench_main(args.out)


if __name__ == "__main__":  # pragma: no cover
    main()
