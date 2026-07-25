"""Tests for TCP transport hardening: compression, size guard, reconnect."""

from __future__ import annotations

import asyncio

import pytest

from suprime.message import Message
from suprime.transport import TcpTransport


async def _wait_for(pred, timeout=5.0, interval=0.05):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.mark.asyncio
async def test_large_message_compressed_roundtrip():
    got = []
    async def handler(m):
        got.append(m)
    server = TcpTransport("127.0.0.1", 0, compress_over=256)
    await server.start(handler)
    client = TcpTransport("127.0.0.1", 0, compress_over=256)
    await client.start(lambda m: None)

    big = "x" * 5000  # highly compressible; exercises the gzip path
    await client.send(server.address, Message(type="t", src="c", payload={"blob": big}))
    assert await _wait_for(lambda: len(got) >= 1)
    assert got[-1].payload["blob"] == big  # compressed on the wire, intact on arrival
    await client.stop()
    await server.stop()


@pytest.mark.asyncio
async def test_small_message_roundtrip_uncompressed():
    got = []
    async def handler(m):
        got.append(m)
    server = TcpTransport("127.0.0.1", 0, compress_over=1024)
    await server.start(handler)
    client = TcpTransport("127.0.0.1", 0)
    await client.start(lambda m: None)
    await client.send(server.address, Message(type="t", src="c", payload={"n": 1}))
    assert await _wait_for(lambda: got)
    assert got[-1].payload["n"] == 1
    await client.stop()
    await server.stop()


@pytest.mark.asyncio
async def test_send_reconnects_after_peer_drop():
    got = []
    async def handler(m):
        got.append(m)
    server = TcpTransport("127.0.0.1", 0)
    await server.start(handler)
    client = TcpTransport("127.0.0.1", 0)
    await client.start(lambda m: None)

    await client.send(server.address, Message(type="t", src="c", payload={"i": 1}))
    assert await _wait_for(lambda: len(got) == 1)

    # Force the server to cycle its listener, invalidating the cached connection.
    addr = server.address
    await server.stop()
    server = TcpTransport(addr.split(":")[0], int(addr.split(":")[1]))
    await server.start(handler)

    # The client's cached connection is now dead; send must reconnect and retry.
    await client.send(server.address, Message(type="t", src="c", payload={"i": 2}))
    assert await _wait_for(lambda: len(got) == 2)
    assert got[-1].payload["i"] == 2
    await client.stop()
    await server.stop()


@pytest.mark.asyncio
async def test_oversized_frame_is_rejected():
    got = []
    async def handler(m):
        got.append(m)
    server = TcpTransport("127.0.0.1", 0, max_frame_size=128)
    await server.start(handler)
    client = TcpTransport("127.0.0.1", 0, compress_over=0)  # no compression
    await client.start(lambda m: None)

    # A payload well over the 128-byte cap must be dropped by the server.
    await client.send(server.address, Message(type="t", src="c", payload={"b": "y" * 500}))
    await asyncio.sleep(0.3)
    assert got == []
    await client.stop()
    await server.stop()
