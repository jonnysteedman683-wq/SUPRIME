"""Pluggable transports for moving :class:`~suprime.message.Message` objects.

Two implementations are provided:

* :class:`InMemoryTransport` — routes messages between nodes in the same
  process through a shared registry. Zero sockets, fully deterministic, ideal
  for tests and simulation of large swarms on one machine.
* :class:`TcpTransport` — a length-prefixed JSON protocol over asyncio TCP for
  real, cross-host deployments.

Both satisfy the same :class:`Transport` interface so a node is agnostic to how
its bytes travel.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Awaitable, Callable, Dict, Optional

from .message import Message

MessageHandler = Callable[[Message], Awaitable[None]]


class TransportError(RuntimeError):
    """Raised when a message cannot be delivered."""


class Transport:
    """Abstract transport interface.

    A transport is responsible only for delivering opaque messages to an
    address. Addresses are plain strings whose meaning is transport specific
    (a registry key in memory, a ``host:port`` pair over TCP).
    """

    address: str

    async def start(self, on_message: MessageHandler) -> None:
        raise NotImplementedError

    async def send(self, address: str, message: Message) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError


class InMemoryTransport(Transport):
    """In-process transport backed by a shared registry.

    All transports sharing the same ``registry`` dict can reach each other by
    address, making it possible to run an entire swarm inside a single event
    loop. A tiny, configurable ``latency`` models network delay so ordering and
    convergence behaviour resemble a real deployment.
    """

    #: Default registry shared by transports created without an explicit one.
    _default_registry: Dict[str, "InMemoryTransport"] = {}

    def __init__(
        self,
        address: str,
        registry: Optional[Dict[str, "InMemoryTransport"]] = None,
        latency: float = 0.0,
    ) -> None:
        self.address = address
        self._registry = registry if registry is not None else self._default_registry
        self._latency = latency
        self._on_message: Optional[MessageHandler] = None
        self._running = False

    async def start(self, on_message: MessageHandler) -> None:
        self._on_message = on_message
        self._registry[self.address] = self
        self._running = True

    async def send(self, address: str, message: Message) -> None:
        peer = self._registry.get(address)
        if peer is None or not peer._running:
            raise TransportError(f"no route to {address}")
        if self._latency:
            await asyncio.sleep(self._latency)
        # Deliver on the event loop so send() never re-enters a handler inline.
        asyncio.get_event_loop().call_soon(peer._deliver, message)

    def _deliver(self, message: Message) -> None:
        if self._running and self._on_message is not None:
            asyncio.ensure_future(self._on_message(message))

    async def stop(self) -> None:
        self._running = False
        self._registry.pop(self.address, None)


class TcpTransport(Transport):
    """Length-prefixed JSON transport over asyncio TCP.

    Each frame on the wire is a 4-byte big-endian unsigned length followed by
    that many bytes of UTF-8 JSON. Outbound connections are opened lazily and
    cached per peer address.
    """

    _HEADER = struct.Struct(">I")

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self._host = host
        self._port = port
        self.address = f"{host}:{port}"
        self._server: Optional[asyncio.AbstractServer] = None
        self._on_message: Optional[MessageHandler] = None
        self._conns: Dict[str, asyncio.StreamWriter] = {}
        self._lock = asyncio.Lock()

    async def start(self, on_message: MessageHandler) -> None:
        self._on_message = on_message
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        # Resolve the OS-assigned port when port 0 was requested.
        sock = self._server.sockets[0]
        self._host, self._port = sock.getsockname()[:2]
        self.address = f"{self._host}:{self._port}"

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                header = await reader.readexactly(self._HEADER.size)
                (length,) = self._HEADER.unpack(header)
                body = await reader.readexactly(length)
                message = Message.from_bytes(body)
                if self._on_message is not None:
                    await self._on_message(message)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    async def _connect(self, address: str) -> asyncio.StreamWriter:
        async with self._lock:
            writer = self._conns.get(address)
            if writer is not None and not writer.is_closing():
                return writer
            host, port = address.rsplit(":", 1)
            try:
                _, writer = await asyncio.open_connection(host, int(port))
            except OSError as exc:  # pragma: no cover - network dependent
                raise TransportError(f"cannot connect to {address}: {exc}") from exc
            self._conns[address] = writer
            return writer

    async def send(self, address: str, message: Message) -> None:
        writer = await self._connect(address)
        body = message.to_bytes()
        frame = self._HEADER.pack(len(body)) + body
        try:
            writer.write(frame)
            await writer.drain()
        except ConnectionError as exc:  # pragma: no cover - network dependent
            self._conns.pop(address, None)
            raise TransportError(f"send to {address} failed: {exc}") from exc

    async def stop(self) -> None:
        for writer in list(self._conns.values()):
            writer.close()
        self._conns.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
