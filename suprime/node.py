"""The swarm node — where transport, membership, gossip, storage, tasks and
consensus come together into a single autonomous participant.

A :class:`SwarmNode` is self-contained: give it a transport and (optionally) a
few seed addresses and it will discover the rest of the swarm, keep its view of
membership current, replicate shared state, help execute distributed tasks and
agree on a leader — all through the one gossip channel.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Awaitable, Callable, Dict, List, Optional

from .consensus import LeaderView
from .gossip import GossipService
from .identity import NodeID
from .metrics import MetricsRegistry, StructuredLogger
from .message import Message, MessageType
from .peers import PeerTable
from .store import DistributedStore
from .tasks import TaskBoard
from .transport import InMemoryTransport, Transport

AppHandler = Callable[[Message], Awaitable[None]]


class SwarmNode:
    """An autonomous member of the SUPRIME swarm.

    Args:
        transport: The transport to communicate over. Defaults to an
            :class:`~suprime.transport.InMemoryTransport` bound to the node id.
        node_id: Explicit identity; a fresh one is generated if omitted.
        seeds: Addresses of existing swarm members used to bootstrap.
        gossip_interval: Seconds between automatic gossip rounds.
        fanout: Number of peers contacted per gossip round.
        suspect_after / dead_after: Failure-detection timeouts (seconds).
        rng: Injectable RNG for deterministic behaviour in tests.
        clock: Injectable wall-clock (for store versions and task claims).
        monotonic: Injectable monotonic clock (for failure detection).
    """

    def __init__(
        self,
        transport: Optional[Transport] = None,
        node_id: Optional[str] = None,
        *,
        seeds: Optional[List[str]] = None,
        gossip_interval: float = 0.5,
        fanout: int = 3,
        suspect_after: float = 3.0,
        dead_after: float = 6.0,
        claim_grace_rounds: int = 3,
        gossip_store: bool = True,
        persist_dir: Optional[str] = None,
        rng: Optional[random.Random] = None,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.id = node_id or NodeID.generate().value
        self._transport = transport or InMemoryTransport(self.id)
        self._seeds = list(seeds or [])
        self._gossip_interval = gossip_interval
        self._clock = clock

        self.peers = PeerTable(
            self.id,
            suspect_after=suspect_after,
            dead_after=dead_after,
            clock=monotonic,
        )
        self.store = DistributedStore(self.id, clock=clock)
        self.tasks = TaskBoard(
            self.id, self.store, clock=clock, claim_grace_rounds=claim_grace_rounds
        )
        self.gossip = GossipService(
            self.id,
            lambda: self._transport.address,
            self.peers,
            self.store,
            fanout=fanout,
            rng=rng or random.Random(),
            include_store=gossip_store,
        )
        self._leader_view = LeaderView(self.id)

        self.metrics = MetricsRegistry()
        self.metrics.gauge_from("peers_alive", lambda: float(len(self.peers.alive())))
        self.metrics.gauge_from("store_keys", lambda: float(len(self.store.keys())))
        self.log = StructuredLogger(self.id)

        self._app_handlers: Dict[str, List[AppHandler]] = {}
        self._leader_callbacks: List[Callable[[str], None]] = []
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        self._seen_messages: "set[str]" = set()
        self._tick_hooks: List[Callable[[], Awaitable[None]]] = []
        self._persist_dir = persist_dir
        self.persistence = None  # set on start() when persist_dir is given

    # -- properties ---------------------------------------------------------

    @property
    def address(self) -> str:
        return self._transport.address

    @property
    def leader(self) -> Optional[str]:
        return self._leader_view.leader

    def is_leader(self) -> bool:
        return self._leader_view.is_leader()

    # -- lifecycle ----------------------------------------------------------

    async def start(self, auto: bool = True) -> "SwarmNode":
        """Start the transport, bootstrap from seeds and (optionally) begin the
        automatic gossip loop.

        Args:
            auto: When ``True`` a background task drives periodic gossip rounds.
                Tests typically pass ``False`` and call :meth:`tick` manually.
        """
        if self._persist_dir is not None and self.persistence is None:
            from .persistence import PersistenceManager

            self.persistence = PersistenceManager(self.store, self._persist_dir)
            self.persistence.recover()   # restore any prior state from disk
            self.persistence.attach()    # durably log all future commits
        await self._transport.start(self._on_message)
        self._running = True
        self._leader_view.update(self.peers.known_ids())
        if self._seeds:
            await self._bootstrap()
        if auto:
            self._loop_task = asyncio.ensure_future(self._run_loop())
        return self

    async def stop(self) -> None:
        self._running = False
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None
        await self._transport.stop()
        if self.persistence is not None:
            self.persistence.snapshot()  # flush a final durable snapshot
            self.persistence.close()

    async def __aenter__(self) -> "SwarmNode":
        return await self.start()

    async def __aexit__(self, *exc) -> None:
        await self.stop()

    async def _bootstrap(self) -> None:
        join = Message(
            type=MessageType.JOIN,
            src=self.id,
            payload={"address": self.address, "heartbeat": self.gossip.heartbeat},
        )
        for seed in self._seeds:
            try:
                await self._transport.send(seed, join)
            except Exception:  # noqa: BLE001 - a dead seed must not abort join
                continue

    # -- the swarm tick -----------------------------------------------------

    async def _run_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._gossip_interval)
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 - keep the swarm alive on errors
                pass

    async def tick(self) -> None:
        """Perform one full swarm round.

        Bumps the heartbeat, ages membership, re-elects the leader, pushes
        gossip and advances distributed task coordination. Exposed publicly so
        callers (and tests) can drive the swarm deterministically.
        """
        self.metrics.inc("ticks")
        self.gossip.bump_heartbeat()
        self.peers.tick()
        # Re-attempt bootstrap while we know no peers: a node whose initial JOIN
        # was lost (or that got fully isolated) keeps knocking on its seeds until
        # it rejoins the swarm, instead of being orphaned forever.
        if self._seeds and len(self.peers) == 0:
            await self._bootstrap()
        self._update_leader()
        # Advance task coordination *before* gossiping so any claim or result
        # written this round is disseminated in the same round's digest, which
        # keeps claim state converging quickly (important for exactly-once).
        await self.tasks.evaluate()
        await self.gossip_once()
        for hook in self._tick_hooks:
            await hook()

    def on_tick(self, hook: Callable[[], Awaitable[None]]) -> None:
        """Register an async callback run at the end of every swarm tick.

        Extension services (aggregation, epidemic broadcast, …) use this to be
        driven by the same clock as the core swarm without subclassing.
        """
        self._tick_hooks.append(hook)

    async def gossip_once(self) -> None:
        """Push a gossip digest to a random fanout of peers."""
        targets = self.gossip.select_targets()
        if not targets:
            return
        message = self.gossip.make_message()
        for address in targets:
            try:
                await self._transport.send(address, message)
                self.metrics.inc("gossip_sent")
            except Exception:  # noqa: BLE001 - unreachable peers age out via FD
                continue

    def _update_leader(self) -> None:
        alive = [p.node_id for p in self.peers.alive()]
        changed = self._leader_view.update(alive)
        if changed is not None:
            for callback in self._leader_callbacks:
                callback(changed)

    # -- messaging ----------------------------------------------------------

    async def _on_message(self, message: Message) -> None:
        if message.id in self._seen_messages:
            return
        self._seen_messages.add(message.id)
        if len(self._seen_messages) > 10000:
            self._seen_messages.clear()

        self.metrics.inc("messages_received")
        mtype = message.type
        if mtype == MessageType.GOSSIP:
            self.gossip.apply(message)
        elif mtype == MessageType.JOIN:
            await self._handle_join(message)
        elif mtype == MessageType.WELCOME:
            self.gossip.apply(message)
        else:
            await self._dispatch_app(message)

    async def _handle_join(self, message: Message) -> None:
        address = message.payload.get("address")
        heartbeat = int(message.payload.get("heartbeat", 0))
        if address:
            self.peers.merge(message.src, address, heartbeat)
            welcome = self.gossip.make_message(MessageType.WELCOME)
            welcome.dst = message.src
            try:
                await self._transport.send(address, welcome)
            except Exception:  # noqa: BLE001
                pass

    async def _dispatch_app(self, message: Message) -> None:
        for handler in self._app_handlers.get(message.type, []):
            await handler(message)

    def on(self, msg_type: str, handler: AppHandler) -> None:
        """Register an async handler for an application message type."""
        self._app_handlers.setdefault(msg_type, []).append(handler)

    def on_leader_change(self, callback: Callable[[str], None]) -> None:
        """Register a callback invoked with the new leader id on each change."""
        self._leader_callbacks.append(callback)

    async def send_to(
        self, address: str, msg_type: str, payload: Optional[Dict] = None, dst: Optional[str] = None
    ) -> bool:
        """Send an application message to a raw transport address.

        Unlike :meth:`send`, this does not require the target to be in the
        membership table — overlays that keep their own address book (e.g.
        HyParView) route through here. Returns ``False`` on delivery failure.
        """
        message = Message(type=msg_type, src=self.id, dst=dst, payload=payload or {})
        try:
            await self._transport.send(address, message)
            return True
        except Exception:  # noqa: BLE001
            return False

    async def send(self, node_id: str, msg_type: str, payload: Optional[Dict] = None) -> bool:
        """Send an application message directly to a known node.

        Returns ``False`` if the target is not in the membership table.
        """
        peer = self.peers.get(node_id)
        if peer is None:
            return False
        message = Message(type=msg_type, src=self.id, dst=node_id, payload=payload or {})
        try:
            await self._transport.send(peer.address, message)
            return True
        except Exception:  # noqa: BLE001
            return False

    async def broadcast(self, msg_type: str, payload: Optional[Dict] = None) -> int:
        """Send an application message to every known peer.

        Returns the number of peers the message was dispatched to.
        """
        message = Message(type=msg_type, src=self.id, payload=payload or {})
        sent = 0
        for peer in self.peers.all():
            try:
                await self._transport.send(peer.address, message)
                sent += 1
            except Exception:  # noqa: BLE001
                continue
        return sent
