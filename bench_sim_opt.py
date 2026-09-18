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

def flush_due_opt(net):
    due = []
    future = []
    t = net._clock.t
    for item in net._queue:
        if item[0] <= t:
            due.append(item)
        else:
            future.append(item)
    net._queue = future
    net._rng.shuffle(due)
    due.sort(key=lambda it: it[0])
    for _at, _seq, dst, message in due:
        transport = net._transports.get(dst)
        if transport is None:
            continue
        net.delivered += 1
        asyncio.ensure_future(transport._deliver(message))
"""

stmt = "flush_due_opt(network)"
print("SimNetwork flush_due_opt:", timeit.timeit(stmt, setup=setup, number=100))
