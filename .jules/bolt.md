## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.
## 2024-07-25 - [Suboptimal Generator Conversion to Set]
**Learning:** Constructing intermediate sets (e.g., `set(local) | set(remote)`) for dictionary key iteration creates unnecessary overhead in memory and time. Python 3 provides dictionary views (`local.keys() | remote.keys()`) that are zero-copy, memory-efficient, and faster for these set operations.
**Action:** Use dictionary view set operations (`.keys() | .keys()`, `.keys() & .keys()`, etc.) instead of explicitly casting dictionaries to sets when working with dictionary keys in union, intersection, or difference operations.
