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
import gzip
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
    _FLAG_GZIP = 0x01

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        max_frame_size: int = 16 * 1024 * 1024,
        compress_over: int = 1024,
        connect_timeout: float = 3.0,
    ) -> None:
        self._host = host
        self._port = port
        self.address = f"{host}:{port}"
        self._server: Optional[asyncio.AbstractServer] = None
        self._on_message: Optional[MessageHandler] = None
        self._conns: Dict[str, asyncio.StreamWriter] = {}
        self._inbound: "set[asyncio.StreamWriter]" = set()
        self._closed = False
        self._lock = asyncio.Lock()
        #: Reject inbound frames larger than this (guards against OOM / bad peers).
        self._max_frame = max_frame_size
        #: gzip-compress bodies larger than this many bytes (0 disables).
        self._compress_over = compress_over
        #: Bound how long a connect may block, so a dead/slow peer can't stall
        #: the caller (e.g. the gossip/SWIM tick loop) indefinitely.
        self._connect_timeout = connect_timeout

    async def start(self, on_message: MessageHandler) -> None:
        self._on_message = on_message
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        # Resolve the OS-assigned port when port 0 was requested.
        sock = self._server.sockets[0]
        self._host, self._port = sock.getsockname()[:2]
        self.address = f"{self._host}:{self._port}"

    def _encode_frame(self, message: Message) -> bytes:
        body = message.to_bytes()
        flag = 0
        if self._compress_over and len(body) > self._compress_over:
            compressed = gzip.compress(body)
            if len(compressed) < len(body):
                body, flag = compressed, self._FLAG_GZIP
        payload = bytes([flag]) + body
        return self._HEADER.pack(len(payload)) + payload

    def _decode_body(self, payload: bytes) -> Message:
        flag, body = payload[0], payload[1:]
        if flag & self._FLAG_GZIP:
            body = gzip.decompress(body)
        return Message.from_bytes(body)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._inbound.add(writer)
        try:
            while not self._closed:
                header = await reader.readexactly(self._HEADER.size)
                (length,) = self._HEADER.unpack(header)
                if length > self._max_frame or length < 1:
                    # Oversized/garbage frame: drop the connection defensively.
                    break
                payload = await reader.readexactly(length)
                message = self._decode_body(payload)
                # A stopped node must stay silent, even on a still-open socket.
                if self._on_message is not None and not self._closed:
                    await self._on_message(message)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self._inbound.discard(writer)
            writer.close()

    async def _connect(self, address: str) -> asyncio.StreamWriter:
        async with self._lock:
            cached = self._conns.get(address)
            if cached is not None:
                reader, writer = cached
                # Reuse only if the link is still healthy: a peer that closed its
                # end surfaces as EOF on our reader even without a write error.
                if not writer.is_closing() and not reader.at_eof():
                    return writer
                writer.close()
                self._conns.pop(address, None)
            host, port = address.rsplit(":", 1)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, int(port)), self._connect_timeout
                )
            except (OSError, asyncio.TimeoutError) as exc:  # pragma: no cover - network dependent
                raise TransportError(f"cannot connect to {address}: {exc}") from exc
            self._conns[address] = (reader, writer)
            return writer

    async def send(self, address: str, message: Message) -> None:
        frame = self._encode_frame(message)
        # Try once, then transparently reconnect and retry once — a cached
        # connection may have been closed by the peer since we last used it.
        for attempt in (1, 2):
            writer = await self._connect(address)
            try:
                writer.write(frame)
                await writer.drain()
                return
            except (ConnectionError, OSError) as exc:
                self._conns.pop(address, None)
                try:
                    writer.close()
                except Exception:
                    pass
                if attempt == 2:
                    raise TransportError(f"send to {address} failed: {exc}") from exc

    async def stop(self) -> None:
        self._closed = True
        # Close outbound connections *and* inbound (server-accepted) ones, so a
        # stopped node cannot keep answering peers over a still-open socket.
        for _reader, writer in list(self._conns.values()):
            writer.close()
        self._conns.clear()
        for writer in list(self._inbound):
            writer.close()
        self._inbound.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
