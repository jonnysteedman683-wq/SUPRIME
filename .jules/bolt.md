## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.

## 2024-07-25 - [Concurrent I/O in plumtree tick]
**Learning:** `PlumtreeBroadcast._tick` was sequentially awaiting `self._node.send(peer, PT_IHAVE, {"ids": ids})` inside a `for` loop, causing network I/O to delay the next iteration. For 50 peers, a simulated 0.01s latency accumulates to 0.53 seconds.
**Action:** Using `asyncio.gather` for asynchronous I/O operations (like network broadcasts) allows Python's event loop to dispatch all messages concurrently, bringing total time down to the max of individual latencies rather than the sum, drastically reducing latency in high-fanout scenarios.
