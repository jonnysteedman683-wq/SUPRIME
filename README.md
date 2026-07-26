# SUPRIME

**A powerful experimental swarm network** — a decentralised, gossip-based mesh
of autonomous nodes that self-organise with no central coordinator.

Every node runs the same code and speaks the same epidemic gossip protocol.
From that single channel, higher-level behaviour *emerges*:

- 🕸️ **Self-organising membership** — nodes discover each other from a seed and
  keep a live view of the swarm using heartbeat gossip plus **SWIM-style active
  probing** (direct + indirect `PING`/`PING-REQ`) that confirms liveness before
  eviction, so slow-but-alive peers aren't falsely dropped.
- 🔁 **Replicated shared state** — a last-writer-wins CRDT key/value store that
  converges on every node regardless of message order, loss or duplication.
- 🐝 **Emergent distributed tasks** — any node can submit work; the swarm
  decides *collectively and deterministically* which node runs it. No scheduler.
- 👑 **Leaderless leader election** — a leader is derived as a pure function of
  live membership, with automatic failover when it dies.
- 🔌 **Pluggable transport** — real TCP for deployment, an in-memory transport
  for running an entire swarm (and its tests) inside one process.
- 📈 **Adaptive gossip** (opt-in) — per-round fanout scales as ~log₂(N) with the
  swarm size so dissemination stays logarithmic as it grows, with a temporary
  boost right after membership churn (`SwarmNode(adaptive_gossip=True)`).
- 🧪 **An experimental toolkit** — a chaos/partition harness, push-sum
  aggregation, stigmergic load balancing, a self-healing Plumtree/HyParView
  overlay, and a live terminal dashboard (see [Experimental features](#experimental-features)).
- 🔬 **A research toolkit** — causal CRDTs (incl. a collaborative-text RGA),
  HMAC-authenticated transport with proof-of-work Sybil resistance,
  Byzantine-tolerant trust-weighted agreement, Merkle anti-entropy, decentralised
  federated learning, topic pub/sub, and a benchmark suite (see
  [Advanced toolkit](#advanced-toolkit)).
- 🛡️ **Trust & production hardening** — pure-Python Ed25519 signed transport and
  ChaCha20 encryption, WAL+snapshot persistence, metrics/observability, a
  distributed KV database, and a deterministic simulation tester (see
  [Production & trust](#production--trust)).
- 🧠 **Collective-AI agent layer** — give each node a pluggable *brain* and a
  fleet of brains behaves as one resilient collective: shared memory, work
  distribution, and quorum voting (see [Collective AI](#collective-ai)).

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
| `suprime/crdt.py` | CRDT toolkit: G/PN-counters, OR-set, LWW-map, vector clocks, MV-register. |
| `suprime/rga.py` | RGA sequence CRDT for collaborative text/logs. |
| `suprime/replicate.py` | Replicate any CRDT across the swarm over gossip. |
| `suprime/security.py` | HMAC-authenticated transport + proof-of-work identities. |
| `suprime/byzantine.py` | Trust-weighted, quorum-based Byzantine-tolerant agreement. |
| `suprime/antientropy.py` | Merkle-style delta reconciliation for the store. |
| `suprime/learning.py` | Decentralised federated learning (gossip SGD). |
| `suprime/pubsub.py` | Topic publish/subscribe over Plumtree. |
| `suprime/bench.py` + `svg.py` | Scale/perf benchmarks + dependency-free SVG charts. |
| `suprime/crypto.py` | Pure-Python Ed25519 signatures + ChaCha20 cipher. |
| `suprime/persistence.py` | WAL + snapshot durability and crash recovery. |
| `suprime/metrics.py` | Counters/gauges, Prometheus export, structured logging. |
| `suprime/kvstore.py` | Distributed KV database with quorum reads/writes. |
| `suprime/simulation.py` | Deterministic simulation testing (DST) harness. |

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

## Advanced toolkit

Deeper, research-grade building blocks. See `examples/advanced.py` for a tour.

### CRDT toolkit + causal consistency

Mergeable data types that converge under any ordering: `GCounter`, `PNCounter`,
`ORSet`, `LWWMap`, plus `VectorClock` and an `MVRegister` that *surfaces*
concurrent writes instead of silently dropping one. Replicate any of them across
the swarm with `CRDTReplicator`.

```python
from suprime import PNCounter, CRDTReplicator
rep = CRDTReplicator(node)
votes = rep.register("votes", PNCounter(node.id))
votes.increment(3)        # converges to the swarm-wide total everywhere
```

### Collaborative text (RGA)

`RGA` is a sequence CRDT: concurrent inserts/deletes from many nodes converge to
one document — a shared, editable log with no central authority.

```python
from suprime import RGA
doc = RGA(node.id); doc.append("h"); doc.insert(1, "i")   # -> "hi"
```

### Authentication + Sybil resistance

`SecureTransport` HMAC-signs every frame with a shared cluster key and drops
anything forged or tampered. `mint_identity` / `verify_identity` add a
hashcash-style proof of work so creating fake identities is deliberately costly.

```python
from suprime import SecureTransport, mint_identity, verify_identity
node = SwarmNode(transport=SecureTransport(inner, b"cluster-key"), ...)
ident = mint_identity("node-x", difficulty=16)   # costly to mint, cheap to check
assert verify_identity(ident, min_difficulty=16)
```

### Byzantine-tolerant agreement

`ByzantineConsensus` accepts a value only when it holds a **trust-weighted
quorum**; nodes that vote against the accepted value lose reputation, so liars
get progressively ignored.

```python
from suprime import ByzantineConsensus
byz = ByzantineConsensus(node, quorum=0.5)
byz.vote("answer", 42)
byz.accepted("answer")     # -> 42 despite a minority of lying nodes
```

### Merkle anti-entropy

Reconcile stores by exchanging **only diffs**: nodes compare per-bucket Merkle
hashes and transfer full entries just for buckets that differ — O(diff) bandwidth
instead of O(state). Pair with `SwarmNode(gossip_store=False)` to let gossip
carry membership while anti-entropy carries state.

### Decentralised federated learning

`GossipLearner` trains a model on each node's private data and gossip-averages
weights with peers — the swarm converges on a shared model as if the data were
pooled, but data never leaves its node.

```python
from suprime import GossipLearner, LinearModel
GossipLearner(node, LinearModel(dim), local_data, lr=0.3)
```

### Pub/sub

`PubSub` is a decentralised topic event bus over Plumtree — publishes reach the
whole swarm efficiently, delivered locally only to a topic's subscribers.

### Benchmarks

```bash
python -m suprime bench --out report.html
```

Runs scaling benchmarks in-process and writes an HTML report with SVG charts:
gossip convergence vs swarm size (~O(log N)), push-sum error decaying to zero,
and Plumtree using **N−1** payload copies per broadcast vs flooding's N·(N−1)
(80–97% fewer messages as the swarm grows).

## Production & trust

### Real cryptography (pure Python, no deps)

`crypto.py` implements Ed25519 signatures (RFC 8032) and ChaCha20 (RFC 8439).
`SignedTransport` gives **public-key authentication** — each message carries the
sender's key and signature, and is rejected unless the signature verifies *and*
the sender's id is the fingerprint of that key, so no node can impersonate
another (no shared secret needed). `EncryptedTransport` adds ChaCha20
encrypt-then-HMAC confidentiality.

```python
from suprime import SignedTransport, EncryptedTransport, crypto
sk, pk = crypto.generate_keypair()
node = SwarmNode(transport=SignedTransport(inner, sk, pk),
                 node_id=crypto.fingerprint(pk))
```

For production throughput, install the optional `cryptography` extra and swap the
primitives; the pure-Python versions keep the core dependency-free.

### Persistence (WAL + snapshot)

`PersistenceManager` gives crash-durable state: every committed entry (local or
replicated) is appended to a write-ahead log, periodically compacted into a
snapshot. On restart it replays them; a torn tail record from a crash mid-write
is safely skipped.

```python
from suprime import PersistenceManager
pm = PersistenceManager(node.store, "./data")
pm.recover()   # restore prior state
pm.attach()    # durably record all future writes
```

### Observability

`node.metrics` exposes counters and gauges (ticks, gossip sent, messages
received, live peers, store size) with a Prometheus exporter; `StructuredLogger`
emits JSON log records.

### Robustness

A node is defensive against bad input and buggy extensions: a malformed or
malicious message can't crash it or drop its connection (it's counted as
`bad_messages` and ignored), and a throwing app handler or tick hook is isolated
(`handler_errors` / `hook_errors`) so it never starves the others. Memory is
bounded too — the de-dup and Plumtree caches are FIFO-capped, and deleted-key
tombstones can be reaped with `store.collect_garbage(min_age)` or automatically
via `SwarmNode(tombstone_gc_after=…)`.

### Distributed KV database

`KVStore` turns the swarm into a Dynamo-style database with **tunable
consistency**: fast local `put`/`get` (eventually consistent) or
`quorum_put`/`quorum_get` (synchronous quorum with read-repair). Pick `W + R > N`
for read-your-writes.

```python
from suprime import KVStore
db = KVStore(node)
await db.quorum_put("user:1", {"name": "ada"}, w=3)
value = await db.quorum_get("user:1", r=1)   # W+R > N ⇒ sees the write
```

It also offers **TTL expiry** (an absolute expiry replicates with the key, so
every node expires it consistently — lazily on access and via `sweep_expired()`),
**range queries** (`db.range("a", "m")`), and **secondary indexes** whose entries
live in the replicated store, so any node can answer an index query:

```python
db.put("session:1", token, ttl=30)           # expires in 30s, everywhere
db.create_index("by_city", lambda v: v["city"])
db.put("u1", {"name": "ada", "city": "london"})
db.query_index("by_city", "london")          # -> ["u1"]
```

### Deterministic simulation testing

`simulation.py` runs the whole swarm on a single seeded schedule of drops,
reordering, latency, partitions and crash/restart — reproducibly. The test suite
asserts convergence, exactly-once execution and no lost writes across many seeds;
a failure reproduces from its seed. (This harness already caught and drove a real
fix — bootstrap-JOIN retry for nodes whose initial join was dropped.)

## Collective AI

SUPRIME isn't itself an AI — it's the coordination layer you run AI *on top of*.
The `Agent` wraps a node with a pluggable **brain** (`brain(task, agent)` — a
rule, a local model, or an LLM call) and wires it to the swarm so a fleet of
brains behaves as one resilient collective: they **think** (execute work),
**remember** (a shared blackboard), **communicate** (pub/sub), and **decide
together** (trust-weighted quorum voting that outvotes wrong or adversarial
members).

```python
from suprime import Agent, build_agents

def classify(question):            # a "brain" — swap for a model or LLM call
    return "spam" if "buy now" in question else "ham"

agents = build_agents(nodes)
for a in agents:
    a.answer_with(classify)

qid = await agents[0].ask("buy now cheap pills")
# ...swarm settles...
agents[0].consensus(qid)           # -> "spam" (ensemble, robust to a faulty agent)
```

Other faculties: `assign()/result()` (distributed work via the task board),
`remember()/recall()` (shared memory), `announce()/listen()` (pub/sub),
`vote()/consensus()` (collective decisions). Making it a *real* AI is just
choosing the brain — e.g. `a.answer_with(lambda q: call_llm(q))`. See
`examples/collective_ai.py`.

## Deployment

Every knob is an environment variable (`SUPRIME_*`, see `suprime/config.py`), so
a node runs the same locally, in Docker, or in a cluster:

```bash
# run a node straight from the environment
SUPRIME_PORT=7000 SUPRIME_PERSIST_DIR=./data python -m suprime serve
SUPRIME_PORT=7001 SUPRIME_SEEDS=127.0.0.1:7000 python -m suprime serve
```

A `Dockerfile` and `docker-compose.yml` bring up a 3-node swarm (a persistent
seed plus two joiners) with one command:

```bash
docker compose up
```

Set `SUPRIME_CLUSTER_KEY` (64 hex chars = 32 bytes) to encrypt all traffic, and
`SUPRIME_PERSIST_DIR` to make a node's state durable across restarts. The TCP
transport auto-reconnects, gzip-compresses large frames, and guards against
oversized frames; the Ed25519 layer transparently uses the `cryptography`
library when the `crypto` extra is installed, falling back to pure Python.

## Tests

```bash
pytest -q
```

Lint runs in CI via `ruff` (pyflakes + syntax rules) alongside the test matrix.

The suite runs whole swarms deterministically over the in-memory transport
(driving gossip rounds by hand against a manual clock) and also verifies the
real TCP transport end to end. Coverage spans discovery, replication, distributed
tasks, leader failover, SWIM probing (silent-but-alive peers survive, dead peers
are still evicted, indirect escalation), chaos partition/heal, push-sum,
stigmergy, the Plumtree/HyParView overlay, CRDT convergence (incl. Hypothesis property tests of
the merge laws), RGA collaborative text, Ed25519/ChaCha20 transports, proof of
work, Byzantine agreement, Merkle anti-entropy, federated learning, pub/sub, the
KV database (quorum + read-repair), persistence/crash-recovery, metrics, and the
deterministic simulation tester across many seeds. CI (`.github/workflows/ci.yml`)
runs it on Python 3.9–3.12.

## Status

Experimental. This is a compact reference implementation for exploring swarm and
gossip-based distributed-systems techniques, not a hardened production runtime.
It ships integrity/authentication (HMAC), proof-of-work Sybil resistance and
trust-weighted Byzantine tolerance, but not transport encryption or full BFT
consensus; public-key identities (Ed25519) would need a third-party crypto
dependency.

## License

MIT
