## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.

## 2024-07-25 - [O(1) Broadcast Concurrent Scaling]
**Learning:** `Plumtree._eager_push` iterated over peers and sent messages sequentially using `await self._node.send()`. This made broadcasts O(N) with respect to time per broadcast depending on peer amount, slowing network I/O heavily.
**Action:** Changed loop to gather coroutines of `self._node.send()` for each peer and executed them concurrently via `await asyncio.gather(*tasks)`. This reduces the broadcast penalty back to O(1) in blocking latency.
