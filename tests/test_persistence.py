"""Tests for WAL + snapshot persistence and crash recovery."""

from __future__ import annotations

import os


from suprime.persistence import PersistenceManager
from suprime.store import DistributedStore


def test_state_survives_restart(tmp_path):
    d = str(tmp_path / "data")
    store = DistributedStore("n1")
    pm = PersistenceManager(store, d).attach()
    store.set("a", 1)
    store.set("b", 2)
    store.set("a", 3)  # overwrite
    store.delete("b")
    pm.close()

    # "restart": a fresh store recovers from disk
    store2 = DistributedStore("n1")
    pm2 = PersistenceManager(store2, d)
    pm2.recover()
    assert store2.get("a") == 3
    assert store2.get("b") is None  # tombstone recovered
    assert "b" not in store2


def test_snapshot_compacts_wal(tmp_path):
    d = str(tmp_path / "data")
    store = DistributedStore("n1")
    pm = PersistenceManager(store, d, snapshot_every=10).attach()
    for i in range(25):
        store.set(f"k{i}", i)
    # after 25 writes with snapshot_every=10, the WAL was compacted at least once
    assert pm.wal_size() < 25
    assert os.path.exists(os.path.join(d, "snapshot.json"))
    pm.close()

    store2 = DistributedStore("n1")
    PersistenceManager(store2, d).recover()
    for i in range(25):
        assert store2.get(f"k{i}") == i


def test_recovery_tolerates_torn_tail(tmp_path):
    d = str(tmp_path / "data")
    store = DistributedStore("n1")
    pm = PersistenceManager(store, d).attach()
    store.set("ok", "value")
    pm.close()

    # simulate a crash that left a half-written record at the end of the WAL
    with open(os.path.join(d, "wal.log"), "a", encoding="utf-8") as fh:
        fh.write('{"key": "broken", "entry": {"val')  # truncated JSON

    store2 = DistributedStore("n1")
    recovered = PersistenceManager(store2, d).recover()
    assert store2.get("ok") == "value"  # good record recovered
    assert "broken" not in store2       # torn record safely skipped
    assert recovered >= 1


def test_persistence_records_merged_remote_entries(tmp_path):
    # entries arriving via replication (not just local writes) are persisted too
    d = str(tmp_path / "data")
    local = DistributedStore("local")
    pm = PersistenceManager(local, d).attach()

    remote = DistributedStore("remote")
    e = remote.set("shared", "from-remote")
    local.merge_entry("shared", e)  # simulate a gossiped update
    pm.close()

    store2 = DistributedStore("local")
    PersistenceManager(store2, d).recover()
    assert store2.get("shared") == "from-remote"
