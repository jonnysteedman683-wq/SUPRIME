# SUPRIME

**A powerful experimental swarm network** — a decentralised, gossip-based mesh
of autonomous nodes that self-organise with no central coordinator.

Every node runs the same code and speaks the same epidemic gossip protocol.
From that single channel, higher-level behaviour *emerges*:

- 🕸️ **Self-organising membership** — nodes discover each other from a seed and
  keep a live view of the swarm using heartbeat-based failure detection.
- 🔁 **Replicated shared state** — a last-writer-wins CRDT key/value store that
  converges on every node regardless of message order, loss or duplication.
- 🐝 **Emergent distributed tasks** — any node can submit work; the swarm
  decides *collectively and deterministically* which node runs it. No scheduler.
- 👑 **Leaderless leader election** — a leader is derived as a pure function of
  live membership, with automatic failover when it dies.
- 🔌 **Pluggable transport** — real TCP for deployment, an in-memory transport
  for running an entire swarm (and its tests) inside one process.
- 🧪 **An experimental toolkit** — a chaos/partition harness, push-sum
  aggregation, stigmergic load balancing, a self-healing Plumtree/HyParView
  overlay, and a live terminal dashboard (see [Experimental features](#experimental-features)).

It is intentionally small, dependency-free (pure Python standard library) and
readable — a working laboratory for distributed-systems ideas.

## Install

```bash
pip install -e .            # runtime (no third-party deps)
pip install -e ".[test]"    # + pytest / pytest-asyncio for the test suite
```

Requires Python 3.9+.

## Quick start (library)

```python
import asyncio
from suprime import SwarmNode

async def main():
    # First node — the seed.
    a = await SwarmNode(node_id="a").start()
    # More nodes bootstrap from it and discover the rest via gossip.
    b = await SwarmNode(node_id="b", seeds=[a.address]).start()

    # Replicated state converges across the swarm.
    a.store.set("motd", "hello swarm")
    await asyncio.sleep(1)
    print(b.store.get("motd"))          # -> "hello swarm"

    # Consensus: everyone agrees on the same leader.
    print(a.leader, b.leader)           # -> "a" "a"

    await a.stop(); await b.stop()

asyncio.run(main())
```

## Quick start (CLI, real TCP)

Open three terminals:

```bash
# Terminal 1 — the seed, also a worker for the "sum" task kind
python -m suprime run --port 7001 --id alpha --worker sum

# Terminals 2 & 3 — join the swarm
python -m suprime run --port 7002 --id beta  --seed 127.0.0.1:7001 --worker sum
python -m suprime run --port 7003 --id gamma --seed 127.0.0.1:7001 --worker sum
```

Each node periodically prints the elected leader, its live peers and the
replicated store. Kill the leader (`Ctrl-C`) and watch the rest fail over.

## Run the in-process demo

```bash
python examples/local_swarm.py
```

Forms a 5-node swarm in a single process and demonstrates discovery, state
replication, distributed task execution and leader failover end to end.

## Architecture

```
          ┌──────────────────────── SwarmNode ────────────────────────┐
          │                                                            │
   app msgs│   GossipService ──pushes──► membership + store digests    │
   ◄───────┤        │                                                  │
          │        ▼                                                  │
          │   PeerTable        DistributedStore ──►  TaskBoard        │
          │  (failure          (LWW CRDT)            (claim/execute)   │
          │   detection)             │                    │           │
          │        │                 └──── LeaderView ◄───┘           │
          │        ▼                    (elect_leader)                 │
          └────────┼───────────────────────────────────────────────── ┘
                   ▼
              Transport  ──  InMemoryTransport | TcpTransport
```

| Module | Responsibility |
| --- | --- |
| `suprime/identity.py` | Stable, sortable node identities. |
| `suprime/message.py` | JSON wire envelope shared by all protocols. |
| `suprime/transport.py` | Pluggable delivery: in-memory + length-prefixed TCP. |
| `suprime/peers.py` | Membership table + heartbeat failure detection. |
| `suprime/store.py` | Last-writer-wins CRDT key/value store. |
| `suprime/gossip.py` | Epidemic dissemination of membership and state. |
| `suprime/tasks.py` | Decentralised task submission, claiming and execution. |
| `suprime/consensus.py` | Deterministic leader election over live members. |
| `suprime/node.py` | The `SwarmNode` that composes it all. |
| `suprime/cli.py` | `python -m suprime run` / `dashboard`. |
| `suprime/chaos.py` | Fault injector: latency, drops, duplication, partitions. |
| `suprime/aggregate.py` | Push-sum decentralised aggregation (avg/sum/count). |
| `suprime/plumtree.py` | Self-optimising epidemic broadcast trees. |
| `suprime/hyparview.py` | Bounded-degree, self-healing partial-view membership. |
| `suprime/dashboard.py` | Live terminal dashboard of a chaos swarm. |

### How coordination emerges

There is no broker. On each round a node bumps a heartbeat counter and pushes a
digest — its membership view plus its slice of the store — to a random *fanout*
of peers. Receivers merge everything. Because the store merge is a CRDT
(commutative, associative, idempotent) and the failure detector, task board and
leader election are all **pure functions of that replicated state**, every node
independently reaches the same conclusions as the state converges.

Distributed tasks show this off: a submitted task is a store entry; workers add
their own *claim* entries; after claims propagate for a short grace period every
node computes the same winner (earliest claim, ties broken by node id) and only
that node executes. Execution is at-least-once, so handlers should be
idempotent.

## Experimental features

Beyond the core swarm, SUPRIME ships a set of research-grade building blocks.
See `examples/experimental.py` for all of them in one run.

### Chaos harness

Wrap any transport in a `ChaosTransport` bound to a shared `ChaosController` to
inject latency, jitter, message drops, duplication and **network partitions** at
runtime — then heal them and watch the swarm reconverge.

```python
from suprime import ChaosController, ChaosTransport
chaos = ChaosController(drop_rate=0.1, latency=0.02)
node = SwarmNode(transport=ChaosTransport(inner, chaos), ...)
chaos.partition(group_a_addrs, group_b_addrs)   # split the brain
chaos.heal()                                    # ...and reunite it
```

### Push-sum aggregation

Compute swarm-wide **averages, sums and counts** with no coordinator, converging
exponentially fast. Mass is conserved, so the estimate is unbiased under
reordering.

```python
from suprime import PushSumAggregator
agg = PushSumAggregator(node)
agg.average("load", local_load)      # every node → estimate() converges to the mean
```

### Stigmergic load balancing

Nodes stamp a load-proportional delay into their task claims, so **idle nodes win
work** — self-balancing coordination through the shared claim medium, no node
ever querying another.

```python
node.tasks.set_load_model(lambda: current_load(), weight=0.01)
```

### Self-healing topology (Plumtree + HyParView)

For swarms too large for full membership: **HyParView** keeps a bounded active
view (an overlay of ~log N links) that self-heals on failure, and **Plumtree**
broadcasts over it as a spanning tree with lazy-gossip repair — O(N) message
copies instead of O(N·fanout), while staying robust.

```python
from suprime import HyParView, PlumtreeBroadcast
overlay = HyParView(node)
bcast = PlumtreeBroadcast(node, neighbors=lambda: list(overlay.active))
await bcast.broadcast({"event": "..."})   # reaches everyone, exactly once
```

### Live dashboard

```bash
python -m suprime dashboard --nodes 8
```

Runs an in-process chaos swarm and renders membership, the leader, replicated
state, a push-sum aggregate and chaos counters in real time — scripted to go
steady → partitioned → healed so you can watch it split and reconverge.

## Tests

```bash
pytest -q
```

The suite runs whole swarms deterministically over the in-memory transport
(driving gossip rounds by hand against a manual clock) and also verifies the
real TCP transport end to end. It covers discovery, bidirectional replication,
distributed task execution, leader failover, chaos partition/heal reconvergence,
push-sum convergence, stigmergic balancing, and the Plumtree/HyParView overlay.

## Status

Experimental. This is a compact reference implementation for exploring swarm and
gossip-based distributed-systems techniques, not a hardened production runtime
(no encryption, authentication or Byzantine-fault tolerance yet).

## License

MIT
