import timeit

setup = """
from suprime.aggregate import PushSumAggregator
class DummyNode:
    def on(self, *args): pass
    def on_tick(self, *args): pass
agg = PushSumAggregator(DummyNode())
agg._inbox = {str(i): [] for i in range(10000)}
agg._estimate = {str(i): 0.0 for i in range(5000, 15000)}
"""

stmt = "agg.keys()"
print("Aggregate keys:", timeit.timeit(stmt, setup=setup, number=100))
