"""Durability for the store: write-ahead log + snapshots.

Nodes are ephemeral by default — restart one and its state is gone. This module
adds crash-durable persistence:

* every committed entry (local write *or* merged remote update) is appended to a
  **write-ahead log** (append-only JSONL), so it survives an abrupt stop;
* periodically the whole store is written to a **snapshot** and the log is
  truncated, bounding recovery time.

On startup, :meth:`PersistenceManager.recover` loads the snapshot and replays the
log on top. Because the store is a CRDT, replay is just a sequence of idempotent
merges — a half-written tail record is simply skipped, so recovery is safe even
after a crash mid-write.
"""

from __future__ import annotations

import json
import os

from .store import DistributedStore, Entry


class PersistenceManager:
    """Persists a :class:`DistributedStore` to a directory and restores it.

    Args:
        store: The store to persist.
        directory: Where to keep ``snapshot.json`` and ``wal.log``.
        snapshot_every: Compact to a snapshot after this many log appends.
    """

    def __init__(self, store: DistributedStore, directory: str, snapshot_every: int = 200) -> None:
        self._store = store
        self._dir = directory
        self._snapshot_every = snapshot_every
        os.makedirs(directory, exist_ok=True)
        self._wal_path = os.path.join(directory, "wal.log")
        self._snap_path = os.path.join(directory, "snapshot.json")
        self._since_snapshot = 0
        self._wal = None  # lazily opened append handle

    # -- recovery (call before attaching) ----------------------------------

    def recover(self) -> int:
        """Restore state from snapshot + WAL. Returns entries recovered."""
        count = 0
        if os.path.exists(self._snap_path):
            with open(self._snap_path, "r", encoding="utf-8") as fh:
                try:
                    digest = json.load(fh)
                    self._store.apply_digest(digest)
                    count += len(digest)
                except json.JSONDecodeError:
                    pass
        if os.path.exists(self._wal_path):
            with open(self._wal_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        # Torn tail record from a crash mid-append: stop here.
                        break
                    self._store.merge_entry(rec["key"], Entry.from_dict(rec["entry"]))
                    count += 1
        return count

    # -- live persistence --------------------------------------------------

    def attach(self) -> "PersistenceManager":
        """Begin recording every future commit to the WAL."""
        self._wal = open(self._wal_path, "a", encoding="utf-8")
        self._store.on_commit(self._on_commit)
        return self

    def _on_commit(self, key: str, entry: Entry) -> None:
        record = json.dumps({"key": key, "entry": entry.to_dict()}, separators=(",", ":"))
        self._wal.write(record + "\n")
        self._wal.flush()
        os.fsync(self._wal.fileno())
        self._since_snapshot += 1
        if self._since_snapshot >= self._snapshot_every:
            self.snapshot()

    def snapshot(self) -> None:
        """Write a full snapshot atomically and truncate the WAL."""
        tmp = self._snap_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._store.digest(), fh, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._snap_path)  # atomic on POSIX
        # Truncate the WAL now that its contents are folded into the snapshot.
        if self._wal is not None:
            self._wal.close()
        open(self._wal_path, "w", encoding="utf-8").close()
        self._wal = open(self._wal_path, "a", encoding="utf-8")
        self._since_snapshot = 0

    def close(self) -> None:
        if self._wal is not None:
            self._wal.close()
            self._wal = None

    def wal_size(self) -> int:
        """Number of records currently in the WAL (for tests/observability)."""
        if not os.path.exists(self._wal_path):
            return 0
        with open(self._wal_path, "r", encoding="utf-8") as fh:
            return sum(1 for line in fh if line.strip())
