## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.
## 2024-07-25 - [Optimize Plumtree set diff operations]
**Learning:**  constructed a union of two sets and iterated over a difference to update one of them. By leveraging the CPython  and  methods, we skip a temporary python set allocation (for the union) and the explicit loop entirely.
**Action:** Replace  with .
## 2024-07-25 - [Optimize Plumtree set diff operations]
**Learning:** `PlumtreeBroadcast._sync_neighbors` constructed a union of two sets and iterated over a difference to update one of them. By leveraging the CPython `.difference()` and `.update()` methods, we skip a temporary python set allocation (for the union) and the explicit loop entirely.
**Action:** Replace `for n in current - (self.eager | self.lazy): self.eager.add(n)` with `self.eager.update(current.difference(self.eager, self.lazy))`.
