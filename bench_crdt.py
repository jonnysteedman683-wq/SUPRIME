import timeit

setup = """
from suprime.crdt import VectorClock

v1 = VectorClock({str(i): i for i in range(10000)})
v2 = VectorClock({str(i): i for i in range(5000, 15000)})
"""

stmt = "v1.compare(v2)"
print("CRDT compare:", timeit.timeit(stmt, setup=setup, number=100))
