"""Tests for environment-driven node configuration."""

from __future__ import annotations

import pytest

from suprime.config import NodeConfig, build_node


def test_config_defaults():
    cfg = NodeConfig.from_env(env={})
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 7000
    assert cfg.seeds == []
    assert cfg.cluster_key() is None


def test_config_from_env():
    cfg = NodeConfig.from_env(env={
        "SUPRIME_HOST": "127.0.0.1",
        "SUPRIME_PORT": "7100",
        "SUPRIME_ID": "alpha",
        "SUPRIME_SEEDS": "a:7000, b:7001 ,",
        "SUPRIME_GOSSIP_INTERVAL": "0.25",
        "SUPRIME_FANOUT": "5",
        "SUPRIME_PERSIST_DIR": "/data",
    })
    assert cfg.port == 7100
    assert cfg.node_id == "alpha"
    assert cfg.seeds == ["a:7000", "b:7001"]  # trimmed, empties dropped
    assert cfg.gossip_interval == 0.25
    assert cfg.fanout == 5
    assert cfg.persist_dir == "/data"


def test_config_cluster_key_validation():
    good = "ab" * 32  # 64 hex chars = 32 bytes
    assert NodeConfig(cluster_key_hex=good).cluster_key() == bytes.fromhex(good)
    with pytest.raises(ValueError):
        NodeConfig(cluster_key_hex="dead").cluster_key()  # wrong length


@pytest.mark.asyncio
async def test_build_node_from_config_starts():
    cfg = NodeConfig(host="127.0.0.1", port=0, node_id="n", cluster_key_hex="cd" * 32)
    node = build_node(cfg)
    await node.start(auto=False)
    assert node.id == "n"
    assert node.address.startswith("127.0.0.1:")
    await node.stop()
