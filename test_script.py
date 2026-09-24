from suprime.crdt import VectorClock

vc1 = VectorClock({"A": 1, "B": 2})
vc2 = VectorClock({"A": 2, "B": 1})

print(vc1.compare(vc2))
