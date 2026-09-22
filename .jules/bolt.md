## 2024-07-24 - [O(1) Local Version Generation]
**Learning:** `DistributedStore._next_version` iterated over all values linearly `max(e.version.ts for e in self._data.values() if e.version.origin == self._node_id)` to ensure monotonicity, making `set` / `delete` an O(N) operation where N is the number of keys. Over time, local writes degrade rapidly.
**Action:** Caching `_highest_ts: float` and maintaining it during `_next_version` and `merge_entry` makes version timestamp calculation O(1), a massive speedup when working with large data sets or performing many initial seed insertions.
## 2024-07-25 - [Suboptimal Generator Conversion to Set]
**Learning:** Constructing intermediate sets (e.g., `set(local) | set(remote)`) for dictionary key iteration creates unnecessary overhead in memory and time. Python 3 provides dictionary views (`local.keys() | remote.keys()`) that are zero-copy, memory-efficient, and faster for these set operations.
**Action:** Use dictionary view set operations (`.keys() | .keys()`, `.keys() & .keys()`, etc.) instead of explicitly casting dictionaries to sets when working with dictionary keys in union, intersection, or difference operations.
## 2024-07-26 - [Set Operation and Loop Short-Circuiting Improvements]
**Learning:** Constructing intermediate sets (e.g., `a - (b | c)`) for difference operations creates unnecessary overhead in memory and time. Direct iteration with membership checks is often faster. Also, in algorithms that determine a boolean combination of flags (like vector clock `compare` checking both `less` and `greater`), checking the condition inside the loop allows for an early return, avoiding unnecessary iteration over remaining elements.
**Action:** Use direct membership checks (`not in`) and in-place updates (`.intersection_update()`) instead of explicitly creating intermediate union sets when filtering. Short-circuit loops early as soon as the target state is identified.
## 2024-07-27 - [Set Difference for Node Filtering]
**Learning:** List comprehensions to filter out specific nodes (e.g., `[p for p in pool if p != src and p != nid]`) when `pool` is a set are slower than `list(pool - {src, nid})` due to Python iterating and evaluating the condition per element instead of using fast, C-level set difference algorithms.
**Action:** When filtering a small number of known elements out of a set to create a list, construct a temporary set of the items to exclude and use the set difference operator `-`, then cast back to a list (e.g. `list(pool - {src, nid})`).
## 2024-07-28 - [Set Operations with intersection_update]
**Learning:** Iterating over a list conversion of a set to conditionally remove items using `discard` is significantly slower than using the built-in C-level set method `intersection_update` (or `difference_update`).
**Action:** Always prefer `a.intersection_update(b)` over `for item in list(a): if item not in b: a.discard(item)` for optimizing Python set intersections/removals in performance-critical code.
## 2024-07-29 - [Optimize Set Comparison Memory Allocation]
**Learning:** Checking equality of two generated sets of object IDs (e.g. `{id(v) for v in a} != {id(v) for v in b}`) is memory intensive as it builds two temporary sets before comparison.
**Action:** When asserting identical contents between a generated subset and its source list, evaluate list lengths first, then fallback to an `any(...)` generator expression against a single set of the source IDs to avoid dual temporary set allocations.
