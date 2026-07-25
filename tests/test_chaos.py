import random
import time

import pytest

from suprime.chaos import ChaosController, ChaosTransport
from suprime.message import Message
from suprime.transport import Transport, TransportError


def test_chaos_controller_partition():
    ctrl = ChaosController()

    # By default, everyone can reach everyone
    assert ctrl.can_reach("node_a", "node_b") is True

    # Partition into groups
    ctrl.partition(["node_a", "node_b"], ["node_c", "node_d"])

    # Intra-group reachability
    assert ctrl.can_reach("node_a", "node_b") is True
    assert ctrl.can_reach("node_c", "node_d") is True

    # Inter-group reachability (partitioned)
    assert ctrl.can_reach("node_a", "node_c") is False
    assert ctrl.can_reach("node_b", "node_d") is False

    # Unplaced nodes cannot reach placed nodes
    assert ctrl.can_reach("node_e", "node_a") is False
    assert ctrl.can_reach("node_b", "node_e") is False

    # Unplaced nodes CAN reach other unplaced nodes
    assert ctrl.can_reach("node_e", "node_f") is True

    # Heal the partition
    ctrl.heal()
    assert ctrl.can_reach("node_a", "node_c") is True
    assert ctrl.can_reach("node_e", "node_a") is True


def test_chaos_controller_probabilities():
    # Use a fixed seed for predictable random values
    # random.Random(42).random() produces ~0.639
    rng = random.Random(42)

    # Test drop logic
    ctrl = ChaosController(drop_rate=0.5, rng=rng)
    # First random value is ~0.639 > 0.5, so should_drop -> False
    assert ctrl.should_drop() is False

    ctrl.drop_rate = 0.8
    # Next random value is ~0.025 < 0.8, so should_drop -> True
    assert ctrl.should_drop() is True

    # Test duplicate logic
    ctrl = ChaosController(duplicate_rate=0.5, rng=random.Random(42))
    # First random value is ~0.639 > 0.5, so should_duplicate -> False
    assert ctrl.should_duplicate() is False

    ctrl.duplicate_rate = 0.8
    # Next random value is ~0.025 < 0.8, so should_duplicate -> True
    assert ctrl.should_duplicate() is True


def test_chaos_controller_delay():
    # No latency or jitter
    ctrl = ChaosController()
    assert ctrl.delay() == 0.0

    # Fixed latency
    ctrl = ChaosController(latency=0.1)
    assert ctrl.delay() == 0.1

    # Jitter included
    # random.Random(42).random() produces ~0.639
    ctrl = ChaosController(latency=0.1, jitter=0.2, rng=random.Random(42))
    expected_delay = 0.1 + (0.6394267984578837 * 0.2)
    assert abs(ctrl.delay() - expected_delay) < 1e-6

def test_chaos_controller_stats():
    ctrl = ChaosController()
    ctrl.delivered = 5
    ctrl.dropped = 2
    ctrl.duplicated = 1
    ctrl.partition(["a", "b"])

    stats = ctrl.stats()
    assert stats["delivered"] == 5
    assert stats["dropped"] == 2
    assert stats["duplicated"] == 1
    assert stats["partitions"] == 1


class DummyTransport(Transport):
    def __init__(self, address="dummy"):
        self._address = address
        self.sends = []
        self.started = False
        self.stopped = False

    @property
    def address(self):
        return self._address

    async def start(self, on_message):
        self.started = True

    async def send(self, address, message):
        self.sends.append((address, message))

    async def stop(self):
        self.stopped = True

@pytest.mark.asyncio
async def test_chaos_transport_drop_duplicate():
    inner = DummyTransport()
    ctrl = ChaosController()
    transport = ChaosTransport(inner, ctrl)
    msg = Message(b"data", "src_node")

    # Test drop
    ctrl.drop_rate = 1.0
    await transport.send("peer", msg)
    assert len(inner.sends) == 0
    assert ctrl.dropped == 1
    assert ctrl.delivered == 0

    # Test duplicate
    ctrl.drop_rate = 0.0
    ctrl.duplicate_rate = 1.0
    await transport.send("peer", msg)
    assert len(inner.sends) == 2
    assert ctrl.delivered == 1
    assert ctrl.duplicated == 1


@pytest.mark.asyncio
async def test_chaos_transport_delay_partition():
    inner = DummyTransport("me")
    ctrl = ChaosController()
    transport = ChaosTransport(inner, ctrl)
    msg = Message(b"data", "src_node")

    # Test delay
    ctrl.latency = 0.05
    start_time = time.monotonic()
    await transport.send("peer", msg)
    elapsed = time.monotonic() - start_time
    assert elapsed >= 0.05
    assert len(inner.sends) == 1

    # Test partition blocking
    ctrl.partition(["me"], ["peer"])
    with pytest.raises(TransportError, match="partitioned from peer"):
        await transport.send("peer", msg)
    # Sends count should not increase
    assert len(inner.sends) == 1


@pytest.mark.asyncio
async def test_chaos_transport_lifecycle():
    inner = DummyTransport()
    ctrl = ChaosController()
    transport = ChaosTransport(inner, ctrl)

    received = []
    async def handler(msg):
        received.append(msg)

    # start
    await transport.start(handler)
    assert inner.started is True

    # recv (delegation)
    msg = Message(b"hello", "src_node")
    # Simulate inner transport delivering a message
    await transport._recv(msg)
    assert len(received) == 1
    assert received[0] == msg

    # stop
    await transport.stop()
    assert inner.stopped is True
