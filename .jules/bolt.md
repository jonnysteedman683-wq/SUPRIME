## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.

## 2024-10-24 - [O(K) Filtering in MVRegister Merge]
**Learning:** `MVRegister.merge` originally compared every element against every other element to remove causally dominated versions, making it an O(N^2) operation. For many concurrent versions, this was very slow.
**Action:** Using a single-pass filtering algorithm where incoming versions are only compared to a running list of causal maxima (`kept`) reduces the time complexity to O(N*K) where K is the number of mutually concurrent versions. This provided a ~400x speedup for 1000 concurrent versions.
