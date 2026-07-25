"""Tests for HMAC-authenticated transport and proof-of-work identities."""

from __future__ import annotations


import pytest

from conftest import flush
from suprime.node import SwarmNode
from suprime.security import (
    Identity,
    SecureTransport,
    mint_identity,
    verify_identity,
)
from suprime.transport import InMemoryTransport


@pytest.mark.asyncio
async def test_secure_transport_allows_authentic_messages():
    reg: dict = {}
    key = b"cluster-secret"
    a = SwarmNode(transport=SecureTransport(InMemoryTransport("a", registry=reg), key), node_id="a")
    b = SwarmNode(transport=SecureTransport(InMemoryTransport("b", registry=reg), key), node_id="b", seeds=[])
    await a.start(auto=False)
    await b.start(auto=False)
    b._seeds = [a.address]
    await b._bootstrap()
    await flush()
    for _ in range(6):
        await a.tick(); await b.tick(); await flush()
    assert "b" in a.peers and "a" in b.peers
    await a.stop(); await b.stop()


@pytest.mark.asyncio
async def test_secure_transport_rejects_wrong_key():
    reg: dict = {}
    a_sec = SecureTransport(InMemoryTransport("a", registry=reg), b"key-A")
    b_sec = SecureTransport(InMemoryTransport("b", registry=reg), b"key-B")  # wrong key
    a = SwarmNode(transport=a_sec, node_id="a")
    b = SwarmNode(transport=b_sec, node_id="b")
    await a.start(auto=False)
    await b.start(auto=False)
    b._seeds = [a.address]
    await b._bootstrap()
    await flush()
    for _ in range(6):
        await a.tick(); await b.tick(); await flush()
    # mismatched keys → all cross-messages dropped, no membership formed
    assert "b" not in a.peers
    assert "a" not in b.peers
    assert a_sec.rejected > 0 or b_sec.rejected > 0
    await a.stop(); await b.stop()


@pytest.mark.asyncio
async def test_secure_transport_rejects_tampering():
    reg: dict = {}
    key = b"k"
    inner_a = InMemoryTransport("a", registry=reg)
    sec_a = SecureTransport(inner_a, key)
    got = []

    b = SwarmNode(transport=SecureTransport(InMemoryTransport("b", registry=reg), key), node_id="b")
    async def handler(msg):
        got.append(msg)
    b.on("chat", handler)
    await b.start(auto=False)
    await sec_a.start(lambda m: None)

    from suprime.message import Message
    msg = Message(type="chat", src="a", payload={"text": "hi"})
    # sign correctly, then tamper the payload after signing
    await sec_a.send(b.address, msg)
    await flush()
    assert len(got) == 1  # authentic message delivered

    tampered = Message(type="chat", src="a", payload={"text": "hi", "_sig": "deadbeef"})
    await inner_a.send(b.address, tampered)  # bypass signing with a bogus sig
    await flush()
    assert len(got) == 1  # tampered/forged message rejected
    await b.stop(); await sec_a.stop()


# -- proof of work ----------------------------------------------------------

def test_proof_of_work_mint_and_verify():
    ident = mint_identity("node-x", difficulty=10)
    assert verify_identity(ident, min_difficulty=10)
    # a forged identity claiming difficulty it didn't earn is rejected
    forged = Identity(node_id="node-x", nonce=0, difficulty=10)
    assert not verify_identity(forged, min_difficulty=10)


def test_proof_of_work_rejects_insufficient_difficulty():
    ident = mint_identity("n", difficulty=8)
    assert not verify_identity(ident, min_difficulty=20)
