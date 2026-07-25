## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.

## 2024-07-25 - [Concurrency in Anti-Entropy]
**Learning:** `AntiEntropy._on_digest` previously awaited `AE_PUSH` and `AE_PULL` sequentially, adding unnecessary network round-trip time per peer interaction.
**Action:** Always prefer `asyncio.gather` for independent network operations, especially to the same peer, to execute concurrently and minimize total wait time.
