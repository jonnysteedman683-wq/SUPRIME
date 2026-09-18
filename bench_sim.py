import timeit

setup = """
from suprime.simulation import SimNetwork, SimClock
import random
import asyncio

class DummyTransport:
    async def _deliver(self, msg): pass

clock = SimClock()
network = SimNetwork(random.Random(42), clock)
network._transports = {"dst": DummyTransport()}

for i in range(10000):
    network._queue.append((i % 100, i, "dst", "msg"))

clock.t = 50
"""

stmt = "network.flush_due()"
print("SimNetwork flush_due:", timeit.timeit(stmt, setup=setup, number=100))
