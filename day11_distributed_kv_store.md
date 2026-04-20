# Day 11 — Design a Distributed Key-Value Store

## Day Summary

Every system you've built so far has used a key-value store — Redis for caching, etcd for cluster membership, Kafka's offset store under the hood. Today you build one from scratch. The challenge isn't any single piece — consistent hashing, replication, quorum reads — you've seen all of these individually. The challenge is making them work together correctly when nodes fail, clocks drift, and networks partition. The Amazon Dynamo paper (2007) is the canonical reference for this problem, and almost everything in today's design traces back to it. The mental model shift from prior days: you're no longer a user of a storage engine choosing its consistency knobs — you're the person who has to implement those knobs, and that requires understanding what's happening inside the machine when a write arrives, when two replicas disagree, and when a node that was dead comes back to life.

---

## Pre-read Checklist

- **Consistent hashing ring and vnodes** (Day 6) — The entire key distribution and replication placement strategy is built on this. Know `ring.get_node(key)`, `ring.get_replica_nodes(key, rf=3)`, and why rack-aware placement matters.
- **CAP theorem, quorum math (W + R > N), and conflict resolution** (Day 7) — Today implements these from scratch. Know the difference between LWW, vector clocks, and CRDTs before reading the conflict resolution section.
- **LSM-tree basics from Cassandra** (Day 3) — The storage engine section builds on the MemTable → SSTable → compaction pipeline you saw when comparing PostgreSQL (B-tree) to Cassandra (LSM). Today goes deeper into how it actually works.
- **Replication lag and hinted handoff** (Days 3, 7) — Today's sloppy quorum is a formalization of the availability-under-failure behavior you first saw with Cassandra. Know what a hint is and why there's a 3-hour window.
- **Gossip protocol intuition** (Day 7's AP system behavior) — Today implements the gossip mechanism that lets an AP cluster discover node failures without a central coordinator. The connection to etcd (CP, Raft-based) vs. Cassandra (AP, gossip-based) is the CAP trade-off made concrete.

---

## The Problem, Stated Precisely

**Context:** Build a distributed key-value store that supports `get(key)`, `put(key, value)`, and `delete(key)`. Think Redis Cluster or Cassandra's core, not a full SQL engine.

### Functional Requirements
- `put(key, value)` — write a key-value pair; overwrite if key exists
- `get(key)` — return the value for a key, or `NOT_FOUND`
- `delete(key)` — mark a key as deleted (tombstone); it must not be returned by `get`
- Keys and values are arbitrary byte arrays; max value size is 10 MB
- Configurable consistency level per operation (eventual, quorum, strong)

### Non-Functional Requirements

| Parameter | Target |
|---|---|
| Read/write throughput | 1 million ops/sec sustained |
| Total data | 10 TB |
| Nodes | 10 nodes to start; must support adding/removing without downtime |
| Replication factor | 3 (every key stored on 3 nodes) |
| Write latency | p99 < 10ms (quorum write) |
| Read latency | p99 < 5ms (quorum read) |
| Availability | 99.99%; survives any 1-node failure with no impact; survives 2-node failure with degraded consistency |
| Durability | No data loss after a write is ACKed |
| Node failure detection | < 10 seconds to detect a failed node |

### Scale Math Check

```
10 TB across 10 nodes = 1 TB per node
With RF=3: each node stores data for its own range + 2 neighbors = ~3 TB raw per node
→ Provision 4 TB SSD per node

1M ops/sec across 10 nodes = 100K ops/sec per node
A single NVMe SSD: ~500K random reads/sec, ~200K random writes/sec
→ Comfortable within single-node SSD limits; bottleneck will be CPU and network
```

---

## Capacity Estimation

### Memory Requirements (Per Node)

```
MemTable (write buffer, in memory before flush to disk):
  Target: absorb 30 seconds of writes before flushing
  Write rate per node: 100K ops/sec × avg value size 1KB = 100 MB/sec
  30-second buffer: 100 MB/sec × 30s = 3 GB MemTable per node
  → Provision 16 GB RAM per node; 3 GB MemTable, rest for OS page cache

WAL (Write-Ahead Log, on disk):
  Every write goes to WAL before MemTable
  WAL write rate: 100 MB/sec
  WAL retention: keep last 10 minutes (before MemTable flush completes)
  WAL disk space: 100 MB/sec × 600s = 60 GB per node → dedicated SSD
```

### Network Bandwidth

```
Intra-cluster replication (RF=3 means every write goes to 3 nodes):
  Each write: 1 write received + 2 replica writes sent
  Per-node write rate: 100K writes/sec × 1 KB avg = 100 MB/sec
  Replica fan-out: 100 MB/sec × 2 replicas = 200 MB/sec outbound per node

Total cluster inbound: 1M writes/sec × 1 KB = 1 GB/sec
Total cluster internal replication: 1M writes/sec × 2 replicas × 1 KB = 2 GB/sec

Gossip overhead:
  Each node gossips with 3 random peers every 1 second
  Gossip message size: ~1 KB (node state summary)
  Per-node gossip: 3 KB/sec outbound — negligible
```

### SSTable Storage

```
SSTables accumulate on disk as MemTables flush.
Compaction reduces them, but at any time there may be multiple SSTable generations.

Write amplification from compaction (tiered): ~10× in the worst case
  Raw data: 1 TB/node
  With compaction overhead: up to 2 TB/node in transient storage during compaction
  → Provision 4 TB SSD per node (1 TB data + 1 TB compaction headroom + WAL)
```

---

## Core Approaches

### 1. Consistent Hashing for Key Distribution

**What it does:** Maps every key to a node (and two replica nodes) so that adding or removing a node only moves a small fraction of keys.

You built this in Day 6. The KV store applies it at the storage layer instead of the cache layer, but the mechanism is identical:

```python
ring = ConsistentHashRing(nodes=["node1","node2",...,"node10"], vnodes=150)

def route_write(key: str) -> list[str]:
    return ring.get_replica_nodes(key, rf=3)  # [primary, replica1, replica2]

def route_read(key: str) -> list[str]:
    return ring.get_replica_nodes(key, rf=3)  # read from any/all 3
```

**One key difference from Day 6:** In the cache layer, losing a key meant a cache miss (the DB had the real data). Here, losing a key means losing the data. This is why RF=3 with rack-aware placement is non-negotiable, not optional.

**Token range ownership:** Each node is responsible for a contiguous arc of the hash ring (its "token range"). When a node starts up, it announces its token range to the cluster via gossip. When a client writes `key="user:123"`, the client library hashes the key, finds the token range owner, and sends the write to that node (the coordinator). The coordinator forwards to the other 2 replicas.

---

### 2. Replication

**What it does:** Stores every key on 3 different physical nodes so the cluster survives node failures without data loss.

**The coordinator pattern:**

```
Client writes key K with value V.
  ↓
Coordinator node (owns K's token range):
  1. Writes K→V to its own storage engine (WAL + MemTable)
  2. Sends K→V to Replica1 (next node clockwise)
  3. Sends K→V to Replica2 (node after Replica1)
  4. Waits for W ACKs (configurable: 1, 2, or 3)
  5. Returns success to client once W ACKs received
  Replica1 and Replica2 continue writing asynchronously (if W < 3)
```

**Rack-aware placement (mandatory for production):**

```
10 nodes across 3 racks:
  Rack A: node1, node2, node3, node4
  Rack B: node5, node6, node7
  Rack C: node8, node9, node10

Key K's replicas: node2 (Rack A), node6 (Rack B), node9 (Rack C)
→ A complete rack failure still leaves 2 of 3 replicas alive.

Without rack-awareness:
  Key K's replicas: node2, node3, node4 — all on Rack A
  → Rack A power failure = all 3 replicas gone = data loss
```

**Synchronous vs. asynchronous replication:**

| W value | Meaning | Durability | Write latency |
|---|---|---|---|
| W=1 | Coordinator ACKs immediately after local write | Low — a crash before replication = data loss | ~1ms (local disk) |
| W=2 | Wait for 1 replica to ACK | Medium — survives coordinator crash | ~3ms (local + 1 network RTT) |
| W=3 | Wait for all replicas to ACK | High — survives any single node failure | ~5ms (local + 2 network RTTs) |

For this system's p99 < 10ms SLA: W=2 (quorum for N=3) is the correct default. W=3 risks latency violations if one replica is slow.

---

### 3. The LSM-Tree Storage Engine

**What it does:** Handles the actual on-disk storage on each node. Optimized for high write throughput by turning random writes into sequential ones.

**The problem with B-trees for write-heavy workloads:** A B-tree update requires finding the exact page on disk where the key lives and modifying it in-place. With 100K writes/sec hitting random keys across a 1TB dataset, this means 100K random disk writes/sec — SSDs can handle this, but HDDs cannot, and even SSDs degrade under sustained random I/O.

**The LSM-tree approach — make all writes sequential:**

```
Write path:
  Step 1 — WAL (Write-Ahead Log):
    Append key-value pair to a sequential log file on disk.
    This is O(1) — just append to the end. Fast even on HDDs.
    Purpose: durability. If the process crashes, replay the WAL on restart.

  Step 2 — MemTable:
    Also write to an in-memory sorted map (typically a red-black tree or skip list).
    Keeps keys sorted in memory for fast reads.
    Size limit: when MemTable reaches ~3 GB, flush it to disk.

  Step 3 — SSTable (Sorted String Table):
    Flushing the MemTable writes it as an immutable, sorted file on disk.
    The file is sorted by key — all key-value pairs written in key order.
    One SSTable file per flush. Multiple SSTables accumulate over time.

Read path:
  1. Check MemTable (in memory, O(log N) in the sorted map)
  2. Check SSTables from newest to oldest (binary search each file's index)
     → Use a Bloom filter per SSTable to skip files that don't contain the key
  3. Return the first matching value found (newest version wins)
```

**Why SSTables are sorted:** Sorted files allow binary search by key. Each SSTable has a sparse index in memory: `{first_key → file_offset, key_every_100_rows → offset, ...}`. Finding a key requires: binary search the sparse index to find the nearest offset, then read forward ~100 entries to find the exact key. This is ~2 disk reads — far cheaper than a B-tree traversal.

**Compaction — the maintenance job:**

```
Problem: over time, you accumulate 50 SSTable files. Reading a key
requires checking all 50 files (even with Bloom filters, this is slow).
Also: deleted keys (tombstones) and overwritten keys waste space.

Compaction: merge multiple SSTables into one larger SSTable.
  - Read N SSTables simultaneously (merge sort — all are already sorted)
  - For duplicate keys: keep only the latest version
  - For tombstones: drop the tombstone if it's older than the GC grace period
  - Write the merged result as a new SSTable
  - Delete the old SSTables

After compaction: fewer files, less read amplification, reclaimed space.

Compaction strategies:
  Size-tiered: merge SSTables of similar size. Simple. Write amplification: ~10×.
    Used by Cassandra. Good for write-heavy workloads.
  Leveled: maintain a hierarchy of levels; each level is 10× larger than the previous.
    Lower write amplification (~3-4×) but more I/O during compaction.
    Used by LevelDB/RocksDB. Good for read-heavy workloads.
```

**Bloom filters per SSTable:** Before binary-searching an SSTable for a key, check the SSTable's Bloom filter. If the filter says "definitely not in this file," skip it entirely. This reduces read amplification from O(N SSTables) to O(1) in the common case (key exists in only one SSTable).

```python
# On SSTable creation:
bloom = BloomFilter(capacity=1_000_000, error_rate=0.01)
for key in sstable_keys:
    bloom.add(key)
sstable.bloom_filter = bloom  # stored in memory (per SSTable)

# On read:
for sstable in reversed(sstables):  # newest first
    if sstable.bloom_filter.might_contain(key):
        value = sstable.binary_search(key)
        if value is not None:
            return value
return NOT_FOUND
```

---

### 4. Gossip Protocol for Cluster Membership

**What it does:** Lets nodes discover which other nodes are alive, which are dead, and what data ranges each node owns — without any central coordinator.

**Why not a central coordinator?** A central coordinator is a single point of failure. If the coordinator goes down, no node knows the cluster state. etcd and ZooKeeper are centralized coordinators — they're CP, highly reliable, but they add operational complexity and become bottlenecks at scale. For a large AP KV store, gossip lets every node be equal.

**How gossip works:**

```
Every 1 second, each node does:
  1. Pick 3 random nodes from the cluster member list
  2. Send each of them a Gossip message:
     {
       "sender": "node3",
       "timestamp": 1711234567,
       "cluster_state": [
         {"node": "node1", "status": "UP", "last_seen": 1711234565, "token_range": [0, 1073741824]},
         {"node": "node2", "status": "UP", "last_seen": 1711234566, "token_range": [1073741824, 2147483648]},
         {"node": "node3", "status": "UP", "last_seen": 1711234567, "token_range": [...], "load": 0.72},
         ...
       ]
     }
  3. Receive gossip messages from other nodes; merge into local state
     (take the highest last_seen timestamp per node — eventual consistency)
```

**Failure detection:** If a node hasn't been seen in > 10 seconds (10 gossip rounds with no updates), mark it as `SUSPECT`. After 30 seconds without recovery, mark it as `DOWN` and redistribute its key ranges to its neighbors. The 10-second threshold gives you < 10 second failure detection — meeting the NFR.

**Why gossip converges:** In a 10-node cluster with each node gossiping to 3 peers per second, information about a new failure propagates to all nodes in O(log N) rounds = ~3.3 seconds. This is called "epidemic spread" — like a rumor spreading through a group.

**What gossip carries:**
- Node status (UP / SUSPECT / DOWN)
- Token range ownership (which node owns which ring arc)
- Schema version (all nodes must agree on the data format)
- Load information (used for request routing decisions)

**What gossip does NOT replace:** Gossip is for cluster membership. It is not a coordination protocol. You cannot use gossip to achieve consensus (two nodes could both believe they're the primary for a key range). For coordination decisions (leader election, distributed locks), you need a consensus protocol like Raft — which is why etcd exists separately.

---

### 5. Vector Clocks for Conflict Detection

**What it does:** Tracks the causal history of every write so that when two replicas disagree on the value of a key, you can tell whether one value is definitively newer or whether they're truly in conflict.

**Why you need this:** With W=2 and RF=3, a write succeeds when 2 of 3 replicas ACK. If the network partition isolates one replica, that replica can accept a different write to the same key. When the partition heals, two replicas have different values. Without vector clocks, you can't tell which is newer — NTP clock skew means timestamps are unreliable at millisecond precision.

**How vector clocks work:**

```
A vector clock is: {node_id: sequence_number} for every node that has written this key.

Initial state: key "user:123" = "alice", written by node1
  value: "alice"
  vector_clock: {node1: 1}

Client updates the value on node2 (reading the vector clock first):
  new value: "alice_smith"
  vector_clock: {node1: 1, node2: 1}  (node2 increments its own counter)

During a partition, another client updates the value on node3
(which only knows the original value):
  new value: "alice_jones"
  vector_clock: {node1: 1, node3: 1}  (node3 increments its own counter)

Partition heals. node1 merges both versions:
  Version A: "alice_smith", {node1:1, node2:1}
  Version B: "alice_jones", {node1:1, node3:1}

  Neither version dominates the other:
    A's clock {node1:1, node2:1} is not ≥ B's clock {node1:1, node3:1} in all positions.
    → TRUE CONFLICT. Both versions are returned to the client for resolution.

  If Version B had been {node1:1, node2:1, node3:1} (written AFTER seeing A):
    B's clock dominates A's clock in all positions.
    → B is simply newer. Discard A. No conflict.
```

**Conflict resolution strategies:**

| Strategy | How | Use when |
|---|---|---|
| Last-Write-Wins (LWW) | Keep the version with the highest wall-clock timestamp | Data loss acceptable; simple to implement |
| Client-side merge | Return both versions to the client; client decides | Shopping carts, user-editable data |
| CRDT merge | Use a CRDT-safe data structure (G-Counter, OR-Set) | Counters, sets, anything with a commutative merge |
| Application logic | The application knows the merge semantics for its data | Complex domain objects |

For this KV store, the default is LWW (simple, predictable), with the option for clients to read vector clocks and submit conflict-resolved writes.

---

### 6. Merkle Trees for Anti-Entropy

**What it does:** Lets two nodes efficiently compare their data and find which keys are out of sync — without transferring all their data.

**The problem it solves:** After a node recovers from a crash, it may have missed writes that happened while it was down (hinted handoff handles some of this, but not all — hints expire after 3 hours). You need a way to find and fix the divergence. Comparing key-by-key would transfer terabytes. Merkle trees make the comparison take kilobytes.

**How Merkle trees work:**

```
Divide the key space into ranges (e.g., 64 buckets).
For each bucket:
  hash = SHA256 of all (key, value) pairs in that bucket, sorted by key

Build a binary tree over those bucket hashes:
  Leaf nodes: hash of each bucket's data
  Internal nodes: hash of (left_child_hash + right_child_hash)
  Root: single hash representing the entire dataset

To compare two nodes:
  1. Exchange root hashes. If they match: nodes are in sync. Done.
  2. If they differ: exchange child hashes of the root.
     → Find which subtree differs. Recurse.
  3. Continue until you've identified the differing leaf buckets.
  4. Transfer only the key-value pairs in the differing buckets.

For a 1TB dataset split into 64 buckets:
  Each bucket: ~15 GB
  Finding the divergence: O(log 64) = 6 hash comparisons = ~6 KB transferred
  Fixing the divergence: transfer only the ~15 GB bucket(s) that differ
  vs. naive: transfer all 1 TB to check
```

**When anti-entropy runs:** Merkle tree comparison runs as a background job (`nodetool repair` in Cassandra), typically scheduled nightly. It's not on the hot path. The system is eventually consistent between runs — hinted handoff and read repair handle the short-term gaps.

---

### 7. Sloppy Quorum and Hinted Handoff

**What it does:** Keeps writes succeeding even when one of the target replica nodes is temporarily down, by writing to a substitute node that will forward the write when the original node recovers.

**Without sloppy quorum:** A write to key K targets nodes [node2, node5, node8]. node5 is down. With strict quorum (W=2), you can still write to node2 and node8 — you only need 2. But what about the data node5 missed?

**With hinted handoff:**

```
node5 is DOWN.
Coordinator writes K→V to node2 (primary) and node8 (replica2).
W=2 is satisfied. Returns success to client.

But: coordinator also writes K→V to node3 (node5's neighbor on the ring),
     with a "hint": {intended_for: "node5", write_timestamp: T}

node3 stores this hint in a special hints table.

node5 comes back UP (detected via gossip within 10 seconds).
node3 sees node5 is UP.
node3 replays the hint: sends K→V to node5.
node5 applies the write.
node3 deletes the hint.
```

**The 3-hour window:** Hints are stored for a maximum of 3 hours by default (Cassandra's default). If node5 doesn't recover within 3 hours, the hint expires and is deleted. node5's data is now permanently out of sync for the keys it missed. This is why Merkle tree anti-entropy (`nodetool repair`) runs regularly — to catch data that fell outside the hint window.

**Sloppy quorum vs. strict quorum:**

| | Strict Quorum | Sloppy Quorum |
|---|---|---|
| node5 is down | Write still succeeds (W=2 met by node2+node8) | Same — plus hint stored |
| node5 recovers | It's behind; needs anti-entropy to catch up | Catches up immediately from hints |
| Availability | Same | Same (hint is extra, not required for W) |
| Consistency | node5 may serve stale reads until repaired | node5 caught up within seconds of recovery |

Sloppy quorum is strictly better than strict quorum in the presence of transient failures. The cost is hint storage (small — hints are just key-value pairs in a local table, not the full data).

---

## System Architecture Walkthrough

### Write Path (`put("user:123", "{name: alice}")`)

1. **Client** sends the write to any node (client has a copy of the ring and routes to the coordinator for this key). Or sends to any node which proxies to the coordinator.

2. **Coordinator** receives the write.
   - Generates a vector clock entry: `{coordinator_id: next_sequence}`.
   - Appends to WAL (sequential disk write, ~0.2ms).
   - Writes to MemTable (in-memory sorted map, ~0.01ms).
   - Sends the write (key, value, vector clock, timestamp) to Replica1 and Replica2 in parallel.

3. **Replica1 and Replica2** each:
   - Append to their own WAL.
   - Write to their own MemTable.
   - Send ACK back to coordinator.

4. **Coordinator** receives ACKs.
   - W=2: waits for 1 replica ACK (already has its own). Total: ~3ms.
   - Returns success to client.
   - Replica2's ACK arrives asynchronously — write completes there within ~1ms of the initial write.

5. **Background:** When MemTable hits 3 GB, flush to SSTable. Run compaction periodically.

### Read Path (`get("user:123")`)

1. **Client** routes to the coordinator for this key.

2. **Coordinator** sends read request to all 3 replicas simultaneously (speculative — don't wait for all 3, just the fastest R=2).

3. **Each replica** checks its MemTable, then SSTables (Bloom filter first, then binary search). Returns value + vector clock.

4. **Coordinator** receives R=2 responses.
   - Compare vector clocks: if one dominates the other, return the dominating value.
   - If they match: return the value.
   - If true conflict (neither dominates): trigger read repair, return both to client (or use LWW as default).

5. **Read repair (background):** If any replica returned a stale value, coordinator sends the latest value back to that replica to bring it up to date. This is async — client doesn't wait for it.

### Node Failure and Recovery

**Node3 fails (detected via gossip in ~10 seconds):**
- Gossip marks node3 as `SUSPECT` after 10s, `DOWN` after 30s.
- Writes targeting node3 now use hinted handoff: coordinator writes to node4 with hint `{intended_for: node3}`.
- Reads targeting node3 route to node3's replicas (the ring walk returns node1 and node5 as the other replica holders). R=2 still satisfied with 2 available nodes.
- No client-visible impact if RF=3 and only 1 node is down.

**Node3 recovers:**
- Gossip marks node3 as `UP`.
- node4 replays all hints for node3 (the writes that happened during the outage). Hint replay completes in seconds.
- If node3 was down > 3 hours: run `repair` (Merkle tree anti-entropy) to catch up the remaining divergence.

---

## Data Model

### On-Disk Key-Value Entry

```
Each key-value pair stored in SSTables has this format:

| Field          | Size     | Notes                                        |
|----------------|----------|----------------------------------------------|
| key_length     | 4 bytes  | Length of the key byte array                 |
| key            | variable | Raw bytes; max 64 KB                         |
| value_length   | 4 bytes  | 0 = tombstone (deleted key)                  |
| value          | variable | Raw bytes; max 10 MB                         |
| vector_clock   | variable | Serialized {node_id: seq} map                |
| timestamp      | 8 bytes  | Wall-clock ms; used for LWW as tiebreaker    |
| checksum       | 4 bytes  | CRC32 of key+value; detect corruption        |

Tombstone: value_length = 0, no value bytes.
  A tombstone means "this key was deleted."
  It is NOT removed from SSTables until compaction, and even then only
  after the GC grace period (10 days by default) — to prevent deleted
  keys from being "resurrected" by stale replicas that haven't seen the delete.
```

### SSTable File Layout

```
SSTable file on disk:
  [Data block]: sorted key-value entries (see above format)
  [Index block]: sparse index {every-100th-key → file_offset}
  [Bloom filter block]: serialized Bloom filter for all keys in this SSTable
  [Footer]: offsets of each block; magic number for validation

In memory per SSTable:
  - Sparse index (small: 1 entry per 100 keys)
  - Bloom filter (180 MB for 100M keys at 1% FPR — but usually much smaller per file)
  - File descriptor (open file handle for reads)
```

### Hints Table (Per Node, Local PostgreSQL or RocksDB)

| Field | Type | Notes |
|---|---|---|
| hint_id | UUID | PK |
| intended_for | VARCHAR | Node ID of the down replica |
| key | BYTES | The key to replay |
| value | BYTES | The value (or tombstone) |
| vector_clock | JSONB | Must be replayed with original vector clock |
| created_at | TIMESTAMPTZ | Hints expire after 3 hours |
| replayed_at | TIMESTAMPTZ | Nullable; set when hint is successfully delivered |

### Gossip State Table (In-Memory Per Node)

```python
cluster_state = {
    "node1": NodeState(status="UP",    last_seen=1711234567, token_start=0,          token_end=429496729,  load=0.65),
    "node2": NodeState(status="UP",    last_seen=1711234566, token_start=429496730,  token_end=858993458,  load=0.71),
    "node3": NodeState(status="DOWN",  last_seen=1711234477, token_start=858993459,  token_end=1288490187, load=None),
    ...
}
# node3 hasn't been seen for 90 seconds → marked DOWN
# Writes targeting node3's range now use hinted handoff to node4
```

---

## Interview Questions with Model Answers

### Q1 (Mid) — "Why does an LSM-tree have better write performance than a B-tree?"

**Model answer:** A B-tree stores data in fixed-size pages on disk. Every write has to find the exact page where the key belongs and modify it in-place. With millions of random keys, this means millions of random disk writes — which are slow on HDDs (10ms seek time) and cause write amplification even on SSDs. An LSM-tree avoids in-place updates entirely. Every write is first appended to a sequential WAL (no seeking — just write to the end of a file), then inserted into an in-memory sorted structure. The actual disk write happens when the in-memory structure fills up and gets flushed as a new immutable SSTable — again, a sequential write. Sequential writes are 10–100× faster than random writes on HDDs and significantly better for SSD endurance. The trade-off is that reads become more complex: you might have to check multiple SSTables to find the latest version of a key. Bloom filters and compaction keep this manageable, but LSM-trees have higher read amplification than B-trees.

**Follow-up:** "When would you choose a B-tree storage engine over an LSM-tree?"

**Pitfall:** Saying LSM-trees are always better. B-trees are better for read-heavy workloads where you need predictable read latency, range scans over sorted data (B-trees keep data sorted on disk, making range scans a single sequential read), and lower read amplification. PostgreSQL uses a B-tree for a reason — OLTP workloads are often more read-heavy than write-heavy, and B-trees give you point lookups and range scans in a single disk read.

---

### Q2 (Mid) — "What is a tombstone and why can't you just delete a key immediately from all SSTables?"

**Model answer:** A tombstone is a special marker that says "this key was deleted." Instead of finding and removing the key from all SSTables (which could be across many files, requiring a full compaction), you write a tombstone record — a key entry with an empty value — to the MemTable and WAL. Future reads that encounter a tombstone during their SSTable search stop and return `NOT_FOUND`, even if older SSTables still contain a value for that key (newest record wins). Tombstones are cleaned up during compaction, but only after a configurable grace period (10 days in Cassandra). The grace period exists because of replication lag: if you delete a key and immediately compact away its tombstone, a stale replica that was offline might come back online and "resurrect" the deleted key — it would see no tombstone and no newer value, so it would serve the old value as if the delete never happened. The grace period ensures that even the slowest replica has had enough time to see the tombstone before it disappears.

**Follow-up:** "What happens if you accumulate too many tombstones without compacting?"

**Pitfall:** Not knowing that tombstone accumulation causes read performance to degrade because reads have to traverse more records to determine a key is deleted. Cassandra has a `tombstone_warn_threshold` and `tombstone_failure_threshold` config for this reason.

---

### Q3 (Senior) — "Walk me through what happens when a client writes a key during a network partition that splits the cluster into two groups of 5 nodes."

**Model answer:** With RF=3 and W=2, I need to track which partition the key's 3 replicas are in. Say the key hashes to replicas [node2, node5, node8]. The partition splits into [node1–node5] and [node6–node10]. node2 and node5 are in partition A; node8 is in partition B. A write arriving at a coordinator in partition A can reach node2 and node5 — that's W=2, so the write succeeds. The write is not sent to node8 (unreachable). With sloppy quorum, a hint is stored at node6 (node8's neighbor in the ring) for later replay. Meanwhile, if a conflicting write arrives at a coordinator in partition B, it can only reach node8 — W=2 not satisfied — the write fails with a quorum error. When the partition heals, node8 is behind by the writes it missed. Gossip detects node8's state within 10 seconds of healing. node6 replays its hints to node8. For any conflicts (if a write somehow succeeded on both sides), vector clocks identify them and the client resolves them (or LWW applies). The key correctness property: with W=2 and RF=3, only one partition can satisfy quorum for a given key's replicas — you can't have two partitions both successfully writing different values to the same key under these settings.

**Follow-up:** "What if you changed to W=1 before the partition? Now what happens?"

**Pitfall:** Not working through the scenario. With W=1, both partitions can accept writes to the same key. partition A writes "alice", partition B writes "bob" to the same key — both succeed. On healing, you have a true conflict. Vector clocks detect this, but resolution requires either LWW (one value lost) or client-side merge (more complex). This is the exact trade-off of moving from CP to AP behavior by lowering W.

---

### Q4 (Senior) — "How does Merkle tree anti-entropy find divergence between two nodes without transferring all their data?"

**Model answer:** Each node builds a Merkle tree over its data by dividing the key space into buckets (say 64 buckets of equal token range size), hashing all the key-value pairs in each bucket, and then building a binary tree where each internal node is the hash of its two children. The root hash represents a fingerprint of the entire dataset. To find divergence, node1 and node2 exchange their root hashes first — if they match, they're in sync and we're done. If they differ, they exchange the hashes of their root's two children. Whichever child hash differs is the subtree containing the divergence. They recurse down until they reach leaf nodes (individual buckets), which identifies exactly which data ranges are out of sync. Only those ranges need to be transferred. For a 1 TB dataset with 64 buckets, finding the divergent bucket requires at most 6 round-trips of exchanging hashes (log₂ 64), each message carrying 64 bytes — under 400 bytes of total comparison traffic to identify which 15 GB bucket needs repair. The efficiency only works because the hash tree is precomputed and maintained incrementally — each write updates the affected bucket's hash and propagates up the tree.

**Follow-up:** "How often do you rebuild the Merkle tree and what's the cost?"

**Pitfall:** Saying "rebuild from scratch every time." The correct answer is incremental maintenance: every write updates the affected leaf bucket hash (rehash the bucket) and updates parent hashes up the tree — O(log 64) = 6 hash operations per write. At 100K writes/sec, that's 600K SHA256 operations/sec, which a modern CPU handles easily. Full tree validation (rebuilding from scratch by re-scanning all data) runs occasionally as a sanity check but is not the hot path.

---

### Q5 (Staff/Principal) — "You need to support strong consistency (linearizable reads) on this KV store that was designed for eventual consistency. What changes do you make and what do you give up?"

**Model answer:** Linearizable reads require that every read reflects all writes that completed before the read started — globally, across all nodes. The AP design today doesn't guarantee this: a read with R=2 might miss a write that completed on only 2 of 3 replicas if one of those 2 replicas is temporarily behind. To achieve linearizability, I need two changes. First, reads must go through a single authoritative source per key range — the primary node. The coordinator reads only from the primary (the node that owns the key's token range), not from replicas. This gives you always-fresh data from the primary, but the primary becomes a bottleneck and a SPOF for reads. Second, writes must be synchronous to all replicas before ACK (W=3), so the primary is always up-to-date. The price: write latency increases to the slowest replica's response time (p99 ~8–10ms vs ~3ms for W=2), read throughput is bounded by a single primary node (no replica read distribution), and the system goes read-unavailable if the primary fails until a new primary is elected (which requires a consensus protocol like Raft — adding significant complexity). This is essentially the design of etcd: linearizable, Raft-based, CP. The trade-off is real: you get correctness at the cost of availability and latency. For a general-purpose KV store, I'd expose both modes — eventual consistency as the default (fast, available) and strong consistency as an opt-in (slow, correct) — and let clients choose per-operation.

**Follow-up:** "Can you achieve linearizable reads without routing all reads through the primary?"

**Pitfall:** Saying "no." The answer is: yes, with lease-based reads. The primary holds a time-bounded lease. Any replica can serve linearizable reads as long as it holds a valid lease from the primary, because the lease guarantees it has seen all writes up to the lease grant time. This is how Google Spanner and CockroachDB achieve linearizable reads without always hitting a single primary — but it requires tight clock synchronization (TrueTime in Spanner, HLC in CockroachDB).

---

## Trade-offs to Articulate

1. **"I chose LSM-tree over B-tree for the storage engine because at 100K writes/sec per node, B-tree in-place updates would require 100K random disk writes/sec — even on NVMe SSDs this approaches device limits and causes write amplification that degrades SSD endurance. LSM-tree converts all writes to sequential appends. The trade-off I'm accepting is read amplification: a `get` may need to check multiple SSTable files, mitigated by Bloom filters. For write-heavy KV workloads, this trade-off is correct."**

2. **"I chose gossip protocol over a centralized coordinator (like ZooKeeper) for cluster membership because a central coordinator is a single point of failure and a scalability bottleneck at 10+ nodes. Gossip achieves O(log N) convergence with no single point of failure. The trade-off I'm accepting is that gossip is eventually consistent — there's a ~3 second window during which different nodes have different views of cluster state. During this window, a write may be routed to a node that doesn't yet know another node is down, triggering hinted handoff rather than a clean retry."**

3. **"I chose W=2, R=2 (quorum) as the default over W=1, R=1 (eventual) because at W=1, a node crash immediately after ACKing a write loses the data before it replicates — RPO > 0. Quorum gives us W+R=4 > N=3, guaranteeing a linearizable read will always see the latest write. The trade-off I'm accepting is ~3ms extra write latency (one network RTT to wait for a replica ACK) and the possibility of write unavailability if 2 of 3 replicas are simultaneously down."**

4. **"I chose vector clocks over LWW for conflict detection because LWW silently discards writes based on wall-clock timestamps, and NTP clock skew of ±50ms means two writes that are 'simultaneous' from a business perspective can have an arbitrary winner. Vector clocks detect true conflicts and surface them to the client or application for correct resolution. The trade-off I'm accepting is storage overhead (vector clock stored with every key-value pair — ~50 bytes extra per entry) and application complexity (clients must handle conflict resolution responses)."**

5. **"I chose sloppy quorum with hinted handoff over strict quorum because strict quorum would fail writes targeting a token range with any replica down, reducing availability. With hinted handoff, writes succeed and are stored temporarily at a substitute node, which replays them when the original node recovers. The trade-off I'm accepting is that hints stored at substitute nodes expire after 3 hours — if a node is down longer than that, the hints are lost and Merkle tree repair is required to reconcile the divergence."**

6. **"I chose size-tiered compaction over leveled compaction because our workload is write-heavy (100K writes/sec per node). Size-tiered compaction has lower write amplification (~10× vs ~30× for leveled), meaning compaction I/O competes less with foreground writes. The trade-off I'm accepting is higher space amplification (more temporary files during compaction) and slower reads than leveled compaction when many SSTable levels exist — mitigated by Bloom filters and aggressive compaction scheduling during low-traffic hours."**

---

## Failure Modes and Resilience Patterns

### 1. Compaction Falling Behind Write Rate
- **Symptom:** Read latency climbs from 5ms p99 to 200ms over 48 hours. Disk usage grows faster than the data ingestion rate. Alert fires on "SSTable file count > 50" per node.
- **Root cause:** Compaction is running but can't keep up with the write rate. SSTables accumulate. Every read must check more and more files, even with Bloom filters (each Bloom filter check is a memory access — at 50 files, that's 50 memory lookups before finding the right SSTable).
- **Detection:** Monitor SSTable file count per node; `compaction_pending_tasks` metric; read latency trend.
- **Mitigation:** Increase compaction thread count (from 1 to 4 threads) during off-peak hours. If write rate is genuinely unsustainable: add more nodes (reducing per-node write rate via consistent hashing redistribution) or rate-limit ingest. Short-term: force a major compaction (`nodetool compact`) to merge all SSTables into one — expensive but immediately reduces read amplification.

### 2. Tombstone Accumulation Causing Read Timeouts
- **Symptom:** `get` calls for specific key ranges time out (> 30s). Other key ranges work fine. Investigation shows those ranges have millions of tombstones from a bulk delete operation run 2 days ago.
- **Root cause:** A batch delete wrote 50M tombstones. Compaction hasn't run long enough to clean them up (compaction respects the 10-day GC grace period). Every `get` in that range scans through all the tombstones to determine the final state.
- **Detection:** `tombstone_warn_threshold` log warnings; per-key-range read latency breakdown.
- **Mitigation:** Short-term: run a targeted compaction on the affected token ranges. Long-term: for bulk deletes, use TTL-based expiration instead of explicit deletes — the storage engine handles TTL cleanup more efficiently. Alternatively, redesign the data model so bulk-deletable data is in a separate key range that can be dropped entirely (partition drop is O(1) vs. tombstone scan).

### 3. Hint Replay Storm After Extended Outage
- **Symptom:** A node recovers after a 2-hour outage. Within 30 seconds of recovery, its disk I/O goes to 100% and write latency across the cluster spikes. Gossip shows the recovered node as `UP` but it's not serving reads reliably.
- **Root cause:** All nodes that stored hints for the recovering node are replaying hints simultaneously. At 100K writes/sec during the 2-hour outage, there are ~720M hints to replay. All neighboring nodes start sending at full speed simultaneously → I/O saturation on the recovering node.
- **Detection:** Recovering node disk write rate > 90% capacity; cluster-wide write latency increase at time of node recovery.
- **Mitigation:** Rate-limit hint replay: each hint-holding node sends at most 10 MB/sec to the recovering node. At 10 nodes × 10 MB/sec = 100 MB/sec total replay rate, 720M × 1KB hints = 720 GB — recovers in ~2 hours at 100 MB/sec. This is slow but prevents I/O saturation. Monitor hint replay progress with `hints_pending` metric; alert when complete.

### 4. Gossip Partition (Split-Brain)
- **Symptom:** The cluster appears to split. 5 nodes think they own certain token ranges; 5 other nodes think the same. Writes to the same key succeed on both sides. After the network issue resolves, there are conflicting values for thousands of keys.
- **Root cause:** A network partition isolates two groups of 5 nodes for 5 minutes. Both groups' gossip state diverges — each group marks the other's nodes as DOWN and re-maps their token ranges.
- **Detection:** After partition heals, Merkle tree comparison shows widespread divergence. Vector clock conflicts spike.
- **Mitigation:** With W=2 and RF=3: both partitions cannot achieve quorum for the same key simultaneously (only one side has 2 of the 3 replicas for any given key). This is the crucial property that prevents true split-brain data conflicts — the CAP theorem at work. What remains after healing is a set of keys where one partition wrote and the other didn't (not conflicts, just staleness) — cleaned up by hinted handoff and Merkle repair.

### 5. MemTable Memory Pressure Causing Flush Cascade
- **Symptom:** Write latency spikes to 500ms every ~30 seconds in a regular pattern. Disk I/O spikes at the same interval. Memory usage is steady at ~14 GB (near the 16 GB node limit).
- **Root cause:** MemTable fills to 3 GB and triggers a flush. During the flush (which is synchronous — the MemTable is copied to a new "flushing MemTable" while writes continue to a new empty MemTable), if writes arrive faster than the flush can complete, a second MemTable starts filling. Eventually both are full → write stall (writes block until a flush completes). The regular 30-second pattern indicates the flush is completing just in time before the second MemTable fills.
- **Detection:** Write latency heatmap shows regular periodic spikes; `memtable_flush_pending` metric > 0.
- **Mitigation:** Increase MemTable flush threshold to 2 GB (flush more frequently, smaller flushes, less bursty I/O). Add dedicated flush I/O thread priority (separate from compaction threads). If the node is genuinely overloaded, reduce per-node write rate by adding more nodes.

---

## How This Connects Forward

- **Day 12 (Web Crawler):** The crawler's URL frontier — a persistent, distributed queue of URLs to crawl — is implemented as a KV store where the key is the URL and the value is crawl metadata (status, last crawled, priority). The LSM-tree write path handles the high append rate naturally.
- **Day 14 (Distributed ID Generation):** A Snowflake ID generator needs each worker node to persist its last-used sequence number across restarts. The KV store's WAL guarantees durability — the same durability primitive that makes Snowflake's counter safe across crashes.
- **Day 15 (Search System):** Elasticsearch's internal shard storage uses an LSM-tree (Lucene's storage engine). Understanding today's SSTable/MemTable/compaction cycle directly explains why Elasticsearch shard sizes matter, why segment merging happens, and why read performance degrades when too many segments accumulate.
- **Day 18 (Distributed File System):** HDFS metadata (which blocks live on which data node) is stored in a KV-like structure. The gossip-based cluster membership from today directly maps to HDFS's DataNode heartbeat mechanism.
- **Day 21 (Capstone):** The distributed KV store is the storage primitive underneath almost every system in the curriculum. The capstone problem will require you to choose between a CP store (etcd-style, linearizable) and an AP store (Dynamo-style, tunable) — the decision framework from Day 7 applied to the internals you built today.
