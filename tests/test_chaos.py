import asyncio
import random
from unittest.mock import AsyncMock, patch

import pytest

from suprime.chaos import ChaosController, ChaosTransport
from suprime.message import Message
from suprime.transport import Transport, TransportError


def test_controller_partition_logic():
    controller = ChaosController()

    # Healthy state: everyone can reach everyone
    assert controller.can_reach("nodeA", "nodeB") is True

    # Partition into [A, B] and [C]
    controller.partition(["nodeA", "nodeB"], ["nodeC"])

    assert controller.can_reach("nodeA", "nodeB") is True
    assert controller.can_reach("nodeA", "nodeC") is False
    assert controller.can_reach("nodeB", "nodeC") is False

    # Unlisted node is fully isolated
    assert controller.can_reach("nodeA", "nodeD") is False
    assert controller.can_reach("nodeD", "nodeA") is False
    assert controller.can_reach("nodeD", "nodeE") is True # Both are unlisted, so neither is placed, so they can talk.

    # Heal the network
    controller.heal()
    assert controller.can_reach("nodeA", "nodeC") is True


def test_controller_probabilistic_decisions():
    controller = ChaosController(
        drop_rate=0.5,
        duplicate_rate=0.5,
        latency=0.1,
        jitter=0.05,
        rng=random.Random(42) # Seeded for determinism
    )

    # Test should_drop
    drops = [controller.should_drop() for _ in range(100)]
    assert 40 < sum(drops) < 60  # Should be around 50%

    # Test should_duplicate
    dupes = [controller.should_duplicate() for _ in range(100)]
    assert 40 < sum(dupes) < 60  # Should be around 50%

    # Test delay
    delays = [controller.delay() for _ in range(10)]
    for delay in delays:
        assert 0.1 <= delay <= 0.15


def test_controller_stats():
    controller = ChaosController()
    stats = controller.stats()
    assert stats == {
        "delivered": 0,
        "dropped": 0,
        "duplicated": 0,
        "partitions": 0,
    }

    controller.delivered = 10
    controller.dropped = 2
    controller.duplicated = 3
    controller.partition(["A", "B"])

    stats = controller.stats()
    assert stats == {
        "delivered": 10,
        "dropped": 2,
        "duplicated": 3,
        "partitions": 1,
    }


class MockTransport(Transport):
    def __init__(self, address):
        self._address = address
        self.sends = []
        self.started = False
        self.stopped = False
        self.on_message = None

    @property
    def address(self) -> str:
        return self._address

    async def start(self, on_message) -> None:
        self.started = True
        self.on_message = on_message

    async def send(self, address: str, message: Message) -> None:
        self.sends.append((address, message))

    async def stop(self) -> None:
        self.stopped = True


@pytest.mark.asyncio
async def test_transport_partition_rejection():
    controller = ChaosController()
    inner = MockTransport("nodeA")
    transport = ChaosTransport(inner, controller)

    msg = Message("test_kind", "payload")

    # Initially healthy
    await transport.send("nodeB", msg)
    assert len(inner.sends) == 1

    # Partitioned
    controller.partition(["nodeA"], ["nodeB"])
    with pytest.raises(TransportError):
        await transport.send("nodeB", msg)

    # Sent count hasn't increased
    assert len(inner.sends) == 1


@pytest.mark.asyncio
async def test_transport_drops():
    controller = ChaosController(drop_rate=1.0)
    inner = MockTransport("nodeA")
    transport = ChaosTransport(inner, controller)

    msg = Message("test_kind", "payload")

    await transport.send("nodeB", msg)

    # Dropped: not sent by inner
    assert len(inner.sends) == 0
    assert controller.dropped == 1
    assert controller.delivered == 0


@pytest.mark.asyncio
async def test_transport_latency():
    controller = ChaosController(latency=0.1)
    inner = MockTransport("nodeA")
    transport = ChaosTransport(inner, controller)

    msg = Message("test_kind", "payload")

    with patch("suprime.chaos.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await transport.send("nodeB", msg)
        mock_sleep.assert_called_once_with(0.1)

    assert len(inner.sends) == 1
    assert controller.delivered == 1


@pytest.mark.asyncio
async def test_transport_duplicates():
    controller = ChaosController(duplicate_rate=1.0)
    inner = MockTransport("nodeA")
    transport = ChaosTransport(inner, controller)

    msg = Message("test_kind", "payload")

    await transport.send("nodeB", msg)

    # Sent twice
    assert len(inner.sends) == 2
    assert inner.sends[0] == ("nodeB", msg)
    assert inner.sends[1] == ("nodeB", msg)

    assert controller.delivered == 1
    assert controller.duplicated == 1

@pytest.mark.asyncio
async def test_transport_receive_and_lifecycle():
    controller = ChaosController()
    inner = MockTransport("nodeA")
    transport = ChaosTransport(inner, controller)

    received = []
    async def on_message(msg):
        received.append(msg)

    await transport.start(on_message)
    assert inner.started

    msg = Message("test_kind", "payload")
    await inner.on_message(msg)

    assert len(received) == 1
    assert received[0] == msg

    await transport.stop()
    assert inner.stopped
