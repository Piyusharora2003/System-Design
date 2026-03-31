# Day 7 — CAP Theorem, ACID, BASE & Consistency Models

## Day Summary

Days 1–6 built real systems while deferring a precise answer to the question that underlies every storage decision: *what contract does your system make with its callers about what they'll read back after a write?* Day 7 closes that debt. The mental model shift is from thinking about consistency as a binary ("consistent or not") to treating it as a **spectrum of guarantees** with well-defined costs — and mapping every system you've already designed to a specific point on that spectrum. CAP is widely misunderstood as a choice between two goods; the more precise framing is PACELC: even without a partition, every system trades latency for consistency. The real test here is being able to look at a product requirement ("users must never see their own deleted post") and immediately name the consistency model it demands, the quorum configuration that enforces it, and the conflict resolution strategy that handles the failure case — then justify why you *didn't* pay for a stronger guarantee than necessary.

---

## Pre-read Checklist

- **Replication factor and replica placement on the ring** (Day 6) — Quorum math (`W + R > N`) is meaningless without fluency in what N physically represents: RF=3 replicas placed on 3 distinct nodes, ideally across 3 AZs. Today's quorum section builds directly on Day 6's replica walk.
- **Replication lag and read-your-writes** (Day 3) — The LSN-based read-your-writes fix from Day 3 is the practical implementation of the read-your-writes consistency model defined formally today. Know both.
- **At-least-once delivery and idempotency** (Day 4) — At-least-once is an AP system's delivery contract. Idempotency is the application-layer fix for the data anomaly it produces. The connection between message delivery semantics and consistency models is direct.
- **Cassandra fan-out writes with QUORUM** (Day 3) — The `QUORUM` vs `LOCAL_ONE` decision made in Day 3 for Cassandra writes is today's W parameter in concrete form. Understand why you chose W=2 for N=3 at the time.
- **etcd as a CP store for node registry** (Day 6) — You chose etcd over a gossip protocol explicitly because ring correctness required CP guarantees. Today formalizes why — etcd uses Raft (a consensus algorithm) to achieve linearizable reads and writes.

---

## The Problem, Stated Precisely

**Context:** You're making storage engine decisions for three independent subsystems of the social platform — and your interviewer is pressing you to justify the consistency model for each. You need to reason from first principles, not from "Cassandra is eventually consistent" memorized facts.

### Three Concrete Scenarios

**Scenario A — Payment deduction:**
A user purchases a premium subscription. The write deducts $9.99 from their balance. Immediately after, a second service checks the balance to authorize a second purchase. Requirements: zero double-spends, zero lost writes, balance must be correct globally across all nodes within the transaction boundary.

**Scenario B — Social feed:**
A user unfollows another user. The follow-relationship write propagates to the feed service. Requirements: the unfollow must eventually take effect (no messages from the unfollowed user appear indefinitely), but a 2–5 second delay is acceptable. No money, no security boundary — just UX.

**Scenario C — Distributed counter (likes on a post):**
A post receives 50,000 concurrent likes in 10 seconds during a viral moment. Requirements: the final count must be accurate; intermediate reads may be slightly low. No write must be lost. The system must not become unavailable during the spike.

### Non-Functional Requirements Across All Scenarios

| Parameter | Scenario A | Scenario B | Scenario C |
|---|---|---|---|
| Consistency model required | Linearizable | Eventual | Eventual (CRDT-safe) |
| CAP posture | CP | AP | AP |
| Tolerable staleness | 0ms | 2–5s | 500ms |
| Write latency SLA | p99 < 200ms | p99 < 100ms | p99 < 50ms |
| Availability requirement | 99.99% (degraded writes OK, no stale reads) | 99.999% (stale reads OK) | 99.999% (stale reads OK) |
| Conflict resolution | N/A (serialized) | LWW | CRDT (G-Counter) |

---

## Capacity Estimation

Consistency choices have direct throughput and latency cost. Quantify them.

### Quorum Write Latency (Cassandra, N=3, RF=3)

```
Write must be ACKed by W nodes before returning to client.
Network RTT between nodes in same DC: ~0.5ms
Cassandra write path per node: ~1ms (memtable + commit log)

W=1 (LOCAL_ONE):  latency = 1 write path = ~1ms p50; single node failure → data loss risk
W=2 (QUORUM):     latency = max(2 parallel writes) = ~1.5ms p50; survives 1 node failure
W=3 (ALL):        latency = max(3 parallel writes) + stragglers = ~3–5ms p50; any node failure → write unavailable

Read latency (R=2, QUORUM):
  Coordinator sends to all 3, waits for 2 fastest: ~2ms p50, ~8ms p99
  Under partition (1 node unreachable): waits for remaining 2, timeout if both slow: ~50ms p99
```

### Linearizability Cost (PostgreSQL with SSI)

```
Serializable Snapshot Isolation (SSI) overhead vs READ COMMITTED:
  ~10–15% throughput reduction on OLTP workloads (measured, not theoretical)
  Lock contention under high concurrency:
    At 3,500 write QPS, SSI abort rate under contention: ~2–5%
    Retry overhead: adds ~0.5ms avg per conflicting transaction

Two-phase locking (2PL, older isolation):
  Throughput reduction: ~30–40% vs READ COMMITTED
  Deadlock rate at 3,500 QPS: ~0.1% of transactions → requires retry logic
```

### CRDT Counter Throughput (Scenario C)

```
50,000 concurrent like writes in 10 seconds = 5,000 writes/sec
G-Counter: each node maintains its own counter shard; merge at read time
  Write: O(1) — increment local node's shard, no coordination
  Read (merged count): O(N) — sum all node shards; at N=5 nodes, trivial
  Storage per counter: N × 8 bytes = 40 bytes per post counter at N=5 nodes
  Total counter storage for 100M posts: 100M × 40B = 4 GB — fits in Redis
```

---

## Core Approaches

### 1. CAP Theorem — Precise Framing

**The common misstatement:** "CAP says you can only pick two of three." This is wrong. Partition tolerance is not optional — networks *will* partition. The real statement:

> **During a network partition, a distributed system must choose between returning a potentially stale response (AP) or returning an error / blocking until the partition heals (CP).**

There is no "CA" system at scale — a system that sacrifices P is a single-node system. Every distributed storage system is either CP or AP *under partition*. Outside of partition events, both consistency and availability can be achieved simultaneously.

**CP systems — behavior during partition:**
```
Client → Node A (partitioned from Node B)
Node A: "I cannot confirm this read is the latest — Node B might have a newer write.
         Returning error: SERVICE_UNAVAILABLE"

Examples: etcd, ZooKeeper, HBase, Google Spanner
Use when: correctness failure is worse than downtime (financial balances, 
          distributed locks, leader election, configuration management)
```

**AP systems — behavior during partition:**
```
Client → Node A (partitioned from Node B)
Node A: "Serving my local copy. It might be 200ms stale."
        Returns data. No error.

Examples: Cassandra (default), DynamoDB (default), CouchDB, DNS
Use when: availability failure is worse than stale data (social feeds,
          product catalogs, user preferences, DNS resolution)
```

**PACELC — the more complete model:**
CAP only speaks to partition behavior. PACELC extends it:

| Condition | Trade-off |
|---|---|
| **P**artition exists | Choose **A**vailability or **C**onsistency (CAP) |
| **E**lse (no partition) | Choose **L**atency or **C**onsistency |

```
DynamoDB:    PA / EL — AP under partition; trades consistency for low latency normally
Cassandra:   PA / EL — same posture; tunable per-operation
Spanner:     PC / EC — CP under partition; consistent but higher latency normally
MySQL (sync replication): PC / EC — blocks on write until replica ACKs
etcd:        PC / EC — linearizable by default; higher latency
```

**The interview trap:** Saying "I'll use Cassandra because it's highly available" without knowing that Cassandra with `W=ALL, R=ALL` is effectively CP (any node failure makes writes unavailable). Cassandra is *tunable* — its CAP posture is a parameter, not a fixed property.

---

### 2. ACID — What Each Property Actually Costs

**Atomicity:**
- All operations in a transaction succeed, or all are rolled back.
- Implementation: PostgreSQL writes all changes to the WAL before committing. On crash, the WAL is replayed. Uncommitted transactions are rolled back from the WAL during recovery.
- Cost: WAL write is synchronous — adds ~0.2ms per transaction.

**Consistency:**
- The database transitions from one valid state to another. Constraints (foreign keys, UNIQUE, CHECK) are enforced.
- Cost: constraint checking is O(1) with indexes, O(N) without. A FK constraint on every write verifies the referenced row exists — requires an index on the referenced column or it's a full table scan.

**Isolation — the levels and their anomalies:**

| Level | Dirty Read | Non-Repeatable Read | Phantom Read | Write Skew | Cost |
|---|---|---|---|---|---|
| READ UNCOMMITTED | ✅ possible | ✅ | ✅ | ✅ | Minimal |
| READ COMMITTED | ❌ prevented | ✅ | ✅ | ✅ | Low (default in PG) |
| REPEATABLE READ | ❌ | ❌ | ✅ | ✅ | Medium |
| SERIALIZABLE (SSI) | ❌ | ❌ | ❌ | ❌ | ~10-15% overhead |

**Write skew (the anomaly candidates miss):**
```
Transaction 1 (doctor A going off call): 
  SELECT count(*) FROM on_call WHERE shift='night'  → returns 2
  if count >= 2: UPDATE on_call SET active=false WHERE doctor='A'

Transaction 2 (doctor B going off call, concurrent):
  SELECT count(*) FROM on_call WHERE shift='night'  → returns 2
  if count >= 2: UPDATE on_call SET active=false WHERE doctor='B'

Both transactions read count=2, both decide to go off call.
Result: 0 doctors on call. Neither transaction saw the other's write.
This is write skew. Only SERIALIZABLE prevents it.
```

**Durability:**
- Committed data survives crashes.
- Implementation: `fsync()` on the WAL file before returning success. Without fsync (e.g., `synchronous_commit=off` in PostgreSQL), a crash in the ~200ms flush window loses up to 200ms of commits.
- Cost of `synchronous_commit=off`: ~5× write throughput improvement, but RPO = 200ms instead of 0.

---

### 3. BASE — What "Eventually Consistent" Actually Means

BASE is not a property you configure — it's a consequence of choosing AP. The three properties:

- **Basically Available:** The system responds to every request, even if some nodes are down. The response may be stale or incomplete, but it's never an error on the primary path.
- **Soft State:** The system's state can change over time *without input* — replicas converging in the background change what you'd read even if you haven't written anything.
- **Eventually Consistent:** If no new writes occur, all replicas will converge to the same value *eventually*. No bound on "eventually" unless you add synchronization (anti-entropy, read repair).

**What "eventually" looks like in practice:**

```
Cassandra with RF=3, W=1, R=1 (maximum AP):
  Write lands on 1 node. Other 2 nodes receive it via:
    - Hinted handoff: if the coordinator holds a hint, replays within seconds
    - Read repair: on the next R=1 read that happens to hit an out-of-date node,
                   the coordinator compares responses and repairs in the background
    - Anti-entropy (Merkle tree comparison): runs on a schedule (nodetool repair),
                   can take hours on a large cluster
  
  Practical convergence time: milliseconds to seconds under normal operation,
  minutes to hours if a node was partitioned for an extended period.
```

**The "eventually" bound matters:** For social feeds, 5 seconds is fine. For inventory counts, 5 seconds means you've oversold. Know the maximum acceptable staleness for your domain before choosing AP.

---

### 4. Consistency Models Spectrum

Listed from strongest (most expensive) to weakest (most available):

**Linearizability (external consistency):**
- Every operation appears to take effect atomically at a single point in time between its start and end.
- Any read that starts *after* a write completes will see that write, globally, on any node.
- Implementation: requires a global coordinator or consensus protocol (Raft, Paxos). Every read goes through the leader, or uses a leased read.
- Cost: 1–2 round trips to quorum for every operation.
- Use: distributed locks, leader election (etcd), financial balances, any "check-then-act" operation.

```
Timeline:
  t=1: Client A writes x=1 (completes at t=3)
  t=4: Client B reads x → MUST see x=1 (write completed before read started)
  t=2: Client C reads x → MAY see x=0 or x=1 (concurrent with write)
```

**Sequential Consistency:**
- All operations appear in some total order consistent with each process's local order.
- Weaker than linearizability: the "global order" need not match real time — it just needs to be *consistent* across all processes.
- Use: multi-player game state, collaborative editing where causal order matters but real-time global order is too expensive.

**Causal Consistency:**
- If operation A *causally precedes* B (A happened before B, or A's result was observed before B started), all nodes see A before B.
- Causally unrelated operations may be seen in different orders on different nodes.
- Implementation: vector clocks or hybrid logical clocks (HLCs) to track causal dependencies.
- Use: comment threads (reply must appear after the post it replies to), "user edited their profile, then posted" (profile edit causally precedes the post).

**Read-Your-Writes:**
- After you perform a write, you will always see that write in subsequent reads, on any node.
- Doesn't guarantee anything about what *other* clients see.
- Implementation from Day 3: route reads to the primary for a window after a write, OR use WAL LSN comparison to check if the replica has caught up.
- Use: social profiles, settings changes, any "I just changed X, I want to confirm it changed."

**Monotonic Reads:**
- You will never see an older version of data after you've seen a newer one.
- Implementation: pin a client session to a single replica. If the replica falls behind, either wait for it to catch up or error.
- Use: pagination ("don't show me items I've already seen"), any UI where going "back in time" is disorienting.

**Eventual Consistency:**
- All replicas converge to the same value if no new writes occur.
- No guarantees on ordering, recency, or how long convergence takes.
- Use: DNS, social media like counts, product view counters, CDN cache invalidation.

---

### 5. Quorum Reads and Writes

**The math:**

```
N = replication factor (total copies of the data)
W = write quorum (number of nodes that must ACK a write before success)
R = read quorum (number of nodes queried; must see consistent value)

Strong consistency guarantee: W + R > N
  → At least one node in every read set participated in every write set.
  → That node has the latest write. Use timestamps or version vectors to identify it.

Weak consistency (AP): W + R ≤ N
  → Reads may miss the latest write entirely.
```

**Concrete configurations for N=3:**

| W | R | W+R | Guarantee | Write availability | Read availability |
|---|---|---|---|---|---|
| 3 | 1 | 4 > 3 | Strong | Fails if any node down | Survives 2 node failures |
| 2 | 2 | 4 > 3 | Strong | Survives 1 node failure | Survives 1 node failure |
| 1 | 3 | 4 > 3 | Strong | Survives 2 node failures | Fails if any node down |
| 1 | 1 | 2 ≤ 3 | Eventual | Survives 2 node failures | Survives 2 node failures |
| 2 | 1 | 3 = 3 | **Borderline — NOT strong** | Survives 1 node failure | Survives 2 node failures |

**The `W+R=N` trap:** `W=2, R=1` with N=3 gives `W+R=3`, which equals but does not exceed N. This is NOT a strong consistency guarantee. The write set and read set may share 0 nodes. Always use `W + R > N` — strict inequality.

**Read repair in practice:**
```
Client reads with R=2 from Cassandra.
Coordinator sends read to all 3 nodes (speculative).
Node A returns: {value: "alice", version: 5}
Node B returns: {value: "alice", version: 5}
Node C returns: {value: "alice_old", version: 3}  ← stale

Coordinator returns version 5 to client.
Background: coordinator sends version 5 to Node C (read repair).
Node C is now up-to-date. Next read to Node C will be correct.
```

---

### 6. Conflict Resolution

**When conflicts occur:** In AP systems during a partition, two nodes can accept writes to the same key independently. When the partition heals, both writes exist — which one wins?

**Last-Write-Wins (LWW):**
```
Node A: writes {user: "alice", email: "a@x.com"} at T=1000ms (NTP time)
Node B: writes {user: "alice", email: "b@x.com"} at T=1001ms (NTP time)

After partition heals:
  LWW: Node B's write wins (higher timestamp).
  Node A's write is silently discarded.

Risk: NTP clock skew of ±50ms means T=1000 on Node A and T=1001 on Node B
are effectively simultaneous. LWW arbitrarily discards one write.
```

Use when: data loss is acceptable (cache eviction, DNS records, social like counts where approximate is fine). Never use for financial data, user profile writes, or anything where "the last update wins" is not a semantically correct business rule.

**Vector Clocks:**
```
Each write carries a version vector: {nodeId: sequenceNumber}
Node A: {A: 1} → writes email "a@x.com"
Node B: {B: 1} → writes email "b@x.com" concurrently

After partition heals:
  Neither {A:1} nor {B:1} dominates the other → CONFLICT detected.
  System surfaces both versions to the application (or user) for resolution.
  
If writes are causally ordered:
  Node A: {A: 1} → write email "a@x.com"
  Node B reads A's write, then writes: {A: 1, B: 1} → write email "b@x.com"
  {A:1, B:1} dominates {A:1} → B's write wins. No conflict. Correct.
```

Cost: vector clocks require storing one entry per node per key. At N=100 nodes, that's 100 × 8 bytes = 800 bytes overhead per key. DynamoDB's original design used vector clocks; it later shifted to LWW to reduce client complexity — a documented product decision, not a technical necessity.

**CRDTs (Conflict-free Replicated Data Types):**

Data structures designed so that concurrent updates *always* merge correctly without coordination or conflict detection:

```python
# G-Counter (Grow-only Counter) — for Scenario C (likes)
# Each node maintains its own shard of the counter
state = {
    "node-A": 15234,   # likes incremented on node A
    "node-B": 12891,   # likes incremented on node B
    "node-C": 14102,   # likes incremented on node C
}

# Increment: only modify your own shard. O(1), no coordination.
def increment(state, my_node):
    state[my_node] += 1

# Merge two states (on partition heal): take max per node. Always correct.
def merge(state1, state2):
    return {node: max(state1.get(node, 0), state2.get(node, 0))
            for node in set(state1) | set(state2)}

# Read total: sum all shards.
def value(state):
    return sum(state.values())

# This is commutative, associative, and idempotent.
# No matter what order merges happen in, the result is always correct.
```

**CRDT types and use cases:**

| CRDT | Operations | Use case |
|---|---|---|
| G-Counter | Increment only | Like counts, page views, impressions |
| PN-Counter | Increment + Decrement | Inventory levels (with floor at 0 handled separately) |
| G-Set | Add only | Tags, unique visitors |
| 2P-Set | Add + Remove | Shopping cart (with tombstones) |
| LWW-Register | Assign (LWW) | User preferences (single value, LWW acceptable) |
| OR-Set | Add + Remove (no tombstone issues) | Collaborative editing, presence lists |

---

## System Architecture Walkthrough

### Scenario A: Payment Deduction (CP, Linearizable)

**Write path:**
1. Payment service opens a PostgreSQL transaction with `ISOLATION LEVEL SERIALIZABLE`.
2. `SELECT balance FROM accounts WHERE user_id = X FOR UPDATE` — acquires a row-level lock. Concurrent transactions attempting to read-for-update on the same row block until this transaction commits or rolls back.
3. Validate balance ≥ charge amount. If not: rollback, return `INSUFFICIENT_FUNDS`.
4. `UPDATE accounts SET balance = balance - 9.99 WHERE user_id = X`.
5. `INSERT INTO transactions (user_id, amount, type) VALUES (...)`.
6. `COMMIT` — WAL fsynced to disk before returning. Both writes atomic.
7. If a concurrent transaction committed between steps 2 and 6 (SSI detects the conflict): PostgreSQL automatically aborts one transaction with a serialization failure. The payment service retries.

**Why not Cassandra for this?** Cassandra's LWT (Lightweight Transactions, `IF balance > amount`) uses Paxos per row but has no multi-row atomicity. The balance deduction and the transaction log insert cannot be made atomic in Cassandra. PostgreSQL's ACID guarantees are non-negotiable for financial operations.

### Scenario B: Social Feed (AP, Eventual Consistency)

**Write path (unfollow):**
1. App server writes to Cassandra: `DELETE FROM follows WHERE follower_id=A AND followee_id=B` with `W=1` (LOCAL_ONE). Returns immediately.
2. Cassandra propagates to 2 other replicas asynchronously. Convergence: ~50–200ms under normal conditions.
3. The feed service's Kafka consumer (Day 4) eventually receives a `user.unfollow` event and purges B's posts from A's feed materialized view.

**Read path:** If A reads their feed within 200ms of the unfollow: they may still see B's posts. This is acceptable — eventual consistency is the contract, and the UX impact is minimal (one feed refresh cycle).

**What makes this AP safe:** No money changes hands. No security boundary is crossed. The worst case is "user sees a post from someone they unfollowed for 2 seconds." The trade-off — 99.999% availability and 1ms write latency — is correct for this domain.

### Scenario C: Like Counter (AP, CRDT G-Counter)

**Write path:**
1. Like request arrives at any of 5 Redis nodes (consistent hashing, Day 6).
2. Owning node increments its local G-Counter shard: `HINCRBY post:viral_id:likes node-A 1`. O(1), no coordination with other nodes.
3. Returns success immediately. p99 write latency: ~1ms.

**Read path (merged count):**
1. Read all 5 shards: `HGETALL post:viral_id:likes` → `{node-A: 15234, node-B: 12891, node-C: 14102, node-D: 13901, node-E: 14234}`.
2. Sum: 70,362 total likes.
3. Under partition: two nodes have diverged counters. At merge, take max per shard — no count is ever lost.

**Failure during partition:** Node-C is unreachable. Likes routed to Node-C are buffered (hinted handoff in Cassandra, or a Kafka queue in front of Redis). When Node-C recovers, replayed increments are merged using the G-Counter merge function. Zero likes are lost — the CRDT guarantees this mathematically.

---

## Data Model

### Accounts Table (PostgreSQL — Scenario A)

| Field | Type | Notes |
|---|---|---|
| user_id | UUID (PK) | |
| balance | NUMERIC(15,4) | NUMERIC not FLOAT — floating point imprecision is unacceptable for money |
| version | BIGINT | Optimistic lock version counter; incremented on every update |
| updated_at | TIMESTAMPTZ | Audit; not used for conflict resolution (use `version`) |

**Storage:** PostgreSQL with `ISOLATION LEVEL SERIALIZABLE`. `NUMERIC(15,4)` stores up to $99,999,999,999.9999 with 4 decimal places — sufficient for any currency. FLOAT would accumulate rounding error across transactions.

**Access pattern:** `SELECT ... FOR UPDATE` on `user_id` (PK lookup, O(log N) B-tree). Write locks the row for the duration of the transaction. No cross-shard joins needed — one user's balance lives on one shard.

### Follows Table (Cassandra — Scenario B)

| Field | Type | Notes |
|---|---|---|
| follower_id | UUID | Partition key |
| followee_id | UUID | Clustering key |
| created_at | TIMESTAMPTZ | |
| deleted_at | TIMESTAMPTZ | Nullable; soft delete with TTL for tombstone cleanup |

**Storage:** Cassandra with `W=QUORUM, R=ONE` for writes (durability matters), `R=LOCAL_ONE` for reads (eventual consistency acceptable). Partition key is `follower_id` — all follows for a user in one partition, enabling efficient "who does user A follow" queries.

**Tombstone management:** Hard deletes in Cassandra create tombstones that persist until compaction. For unfollow, use a soft delete with a TTL: `UPDATE follows SET deleted_at=now(), active=false WHERE ... USING TTL 2592000` (30-day TTL). Tombstones expire automatically without manual compaction tuning.

### Like Counters (Redis G-Counter — Scenario C)

```
Key: post:{post_id}:likes  (Redis Hash)
Fields: {node-A, node-B, node-C, node-D, node-E} → integer shard values

HSET post:abc123:likes node-A 15234 node-B 12891 node-C 14102

Merge at read time:
  HGETALL post:abc123:likes → sum all values

Storage per counter: 5 fields × (field_name ~8B + value ~8B) ≈ 80B per post
100M posts: 100M × 80B = 8 GB — fits on a single Redis instance; shard if needed.
```

**Why not a single `INCR` key?** A single counter key routes all writes through one Redis node (Day 6's hot key problem). At 50K writes/sec to one key, that node's single-threaded event loop is saturated. G-Counter distributes writes across N nodes, making write throughput scale linearly with node count.

---

## Interview Questions with Model Answers

### Q1 (Mid) — "When would you choose an AP system over a CP system? Give a concrete example."

**Model answer:** The decision comes down to whether a stale read or a write failure is more harmful to the product. For a social feed, a user seeing a post from someone they unfollowed 2 seconds ago is a negligible UX degradation — but if the system returned an error on every feed read during a network partition, that's a P0 incident. I'd choose AP (Cassandra, DynamoDB) for anything in the "social" or "preference" category: follows, likes, view counts, feed content, user settings. I'd choose CP (PostgreSQL with strong isolation, etcd) for anything with a correctness invariant: financial balances, inventory counts below zero being invalid, distributed locks, cluster membership. The question to ask is: "What's the worst case if a user reads a value that's 500ms stale?" If the answer involves money, security, or a hard constraint, choose CP.

**Follow-up:** "What about inventory counts for an e-commerce system?"

**Pitfall:** Saying "AP for everything except money." Inventory is the classic trap — counting down to zero requires a hard invariant (no negative inventory), which demands at minimum an atomic decrement-and-check, pushing toward CP or strong quorum.

---

### Q2 (Mid) — "What's the difference between linearizability and sequential consistency? Why does it matter?"

**Model answer:** Linearizability requires that every operation appears to take effect at a single point in *real time* between its invocation and completion. If write W completes at T=100ms, any read that starts at T=101ms, on any node, must see W. Sequential consistency weakens this: operations appear in some global total order that respects each process's local order, but that global order doesn't have to match wall-clock time. Two processes might see different orderings of each other's operations as long as neither sees an order that violates its own writes. In practice, linearizability is what you need when you're implementing a distributed lock or a compare-and-swap — "is this key still version 5? If so, swap to version 6." Sequential consistency doesn't prevent another process from reading version 4 after version 5 was globally acknowledged, which breaks CAS semantics.

**Follow-up:** "Does Redis provide linearizable reads by default?"

**Pitfall:** Confusing "strong consistency" with "linearizability." Redis Cluster with the default config provides sequential consistency per slot, but not linearizability — a replica can serve a stale read between a write and its replication. Use `WAIT` command to block until W replicas have ACKed for linearizable behavior, at latency cost.

---

### Q3 (Senior) — "Your Cassandra cluster uses W=2, R=2, N=3. A network partition isolates one node. What's the consistency behavior for reads and writes during the partition?"

**Model answer:** With N=3, W=2, R=2, we need 2 nodes to respond for both reads and writes. During a partition that isolates 1 node, the remaining 2-node majority can still satisfy both quorums — W=2 and R=2 are both achievable with 2 available nodes. So the system remains fully operational. The isolated node cannot serve reads or writes meeting quorum requirements — requests to it either time out or fail over to the coordinator. `W+R=4 > N=3`, so strong consistency is maintained throughout. Now change the scenario: the partition isolates 2 nodes. Now only 1 node is reachable — neither W=2 nor R=2 can be satisfied. The system becomes unavailable for reads and writes. This is the CP behavior of a QUORUM configuration — it sacrifices availability to maintain consistency. If you need availability under a 2-node failure, you must drop to W=1, R=1, accepting eventual consistency.

**Follow-up:** "What if you need strong consistency but also need to survive a 2-node failure in a 3-node cluster?"

**Pitfall:** Not recognizing this is mathematically impossible for N=3. To survive 2 failures with strong consistency requires N ≥ 5 (W=3, R=3, W+R=6 > N=5, survives 2 failures with 3 remaining).

---

### Q4 (Senior) — "Explain write skew and why READ COMMITTED isolation doesn't prevent it."

**Model answer:** Write skew occurs when two concurrent transactions each read a shared precondition, both decide it's safe to proceed based on what they read, and both write — but their combined effect violates the precondition that each individually validated. READ COMMITTED only prevents dirty reads (reading uncommitted data). Each transaction sees a snapshot of committed data at the time of each read statement. But in a write skew scenario, both transactions read the same committed state, both decide to write, and by the time either commits, the precondition has been violated by the other's write — but neither transaction can detect this because READ COMMITTED doesn't re-check preconditions at commit time. SERIALIZABLE (SSI in PostgreSQL) tracks the read-set and write-set of each transaction and aborts one if it detects that executing them serially would produce a different outcome than executing them concurrently. This catches write skew. The cost is ~10–15% throughput reduction and occasional serialization failures requiring retry.

**Follow-up:** "Can you prevent write skew without SERIALIZABLE isolation? How?"

**Pitfall:** Not knowing that `SELECT ... FOR UPDATE` on the rows involved in the precondition check converts it to a pessimistic lock that prevents write skew without full serializable isolation — at the cost of higher lock contention.

---

### Q5 (Staff/Principal) — "You're designing a global social platform. User profile writes happen in the US region. A user in Europe reads the profile 50ms after the write. You need read-your-writes consistency globally. How do you implement it without routing all reads through the US?"

**Model answer:** True global linearizability (Spanner-style) requires TrueTime and globally synchronized clocks — operationally expensive. I'd implement read-your-writes without global coordination using a **session token approach**: when the US region commits the profile write, it returns a token encoding the WAL LSN (or Lamport timestamp) of that write — e.g., `{region: "us-east-1", lsn: 8842341}`. The client stores this token in its session. On the next read from the EU region, the client sends the token in the request header. The EU regional cache/replica checks its local replication watermark: if it has replayed LSN ≥ 8842341, it serves the read locally. If not (replication lag), it has two options: (1) wait up to a configurable timeout (e.g., 500ms) for replication to catch up, then serve locally; (2) proxy the read to the US region as a fallback. This bounds the cases where global round-trip is needed to the first ~200ms after a write, while providing a correctness guarantee for the client who just performed the write. The key insight: read-your-writes is a per-*session* guarantee, not a global one — you only need the write's exact LSN to be visible to the session that performed it.

**Follow-up:** "What happens if the client loses their session token (browser tab close, token expiry)?"

**Pitfall:** Treating this as a global linearizability problem and reaching for Spanner/TrueTime, which is the right answer for Google-scale financial systems but massively over-engineered for a social profile. Session-token-based read-your-writes is the correct industry pattern (used by Facebook's TAO, DynamoDB's session consistency mode).

---

## Trade-offs to Articulate

1. **"I chose AP (Cassandra, W=1, R=1) over CP for the follow-relationship writes because at 800 write QPS with a 99.999% availability SLA, a CP system that blocks writes during a network partition would breach the availability target — a partition event lasting 30 seconds causes 24,000 dropped follow events. The trade-off I'm accepting is that an unfollow may take up to 5 seconds to propagate to all replicas, during which the feed service may deliver posts from the unfollowed user."**

2. **"I chose NUMERIC(15,4) over FLOAT for financial balances because IEEE 754 double-precision floating point cannot represent 0.10 exactly — it stores 0.1000000000000000055511... Across millions of transactions, this rounding error accumulates. The trade-off I'm accepting is slightly larger storage (16 bytes vs 8 bytes for DOUBLE) and marginally slower arithmetic operations."**

3. **"I chose SERIALIZABLE isolation for the payment transaction over REPEATABLE READ because REPEATABLE READ does not prevent write skew — two concurrent balance checks can both see a valid balance and both deduct, producing a negative balance. The trade-off I'm accepting is a ~10–15% throughput reduction and a ~2–5% serialization failure rate under high concurrency, which the application handles with idempotent retries."**

4. **"I chose G-Counter (CRDT) over a single Redis INCR for the like counter because at 50,000 concurrent writes/sec to a single key, all traffic routes to one Redis node (consistent hashing, Day 6) — a hot key that saturates one node's single-threaded event loop. G-Counter distributes writes across N nodes with zero coordination. The trade-off I'm accepting is that reads require fetching N shards and summing them — O(N) vs O(1) — and intermediate reads during a partition may undercount by the number of increments buffered on unreachable nodes."**

5. **"I chose vector clocks over LWW for the follow-relationship conflict resolution because LWW with NTP clock skew silently discards writes — a user's 'follow' action could be discarded if its timestamp is 1ms behind a concurrent 'unfollow' with a slightly faster clock. The trade-off I'm accepting is storage overhead (one version vector entry per node per key) and application complexity to handle the detected conflicts — which for follow/unfollow reduces to 'latest explicit action wins' applied to the conflict vector."**

6. **"I chose session-token-based read-your-writes over global linearizability (Spanner-style) for cross-region profile reads because global linearizability requires TrueTime atomic clocks and 2-phase commit across regions — a p99 write latency of ~200ms globally vs ~10ms regionally. The trade-off I'm accepting is that the correctness guarantee is per-session, not global: two different users may see different versions of a profile concurrently, which is an acceptable UX inconsistency for a social profile but not for a financial ledger."**

---

## Failure Modes and Resilience Patterns

### 1. Quorum Write Succeeding but Read Missing the Latest Write
- **Symptom:** A user updates their profile picture. On page refresh (new request), the old picture is shown. Reproducible ~5% of the time. No errors logged.
- **Root cause:** W=2, R=2, N=3. The write was ACKed by nodes A and B. The subsequent read hit nodes B and C. Node C returned a stale version. B returned the new version. The coordinator used the higher-versioned response — but with R=2 and a bug in the coordinator's version comparison (comparing timestamps instead of Cassandra's internal write-time), the stale value won.
- **Detection:** Add a version counter to all profile writes. Alert when a read returns a version lower than the client's last written version (requires client-side tracking — the session token pattern from Q5).
- **Mitigation:** Use Cassandra's built-in write-time metadata for version comparison, not application-layer timestamps. Verify `W + R > N` is enforced strictly — not `W + R = N`.

### 2. Serialization Failure Cascade Under High Load
- **Symptom:** Payment service error rate spikes to 8% during flash sale. Logs show `ERROR: could not serialize access due to concurrent update` on 8% of transactions. Retries succeed on first retry 95% of the time, but the retry load amplifies throughput by ~8%.
- **Root cause:** SERIALIZABLE isolation with SSI aborts conflicting transactions. Under 3,500 concurrent payment QPS targeting the same account (a shared event ticket or a flash sale), abort rate spikes.
- **Detection:** `pg_stat_activity` shows high `serialization_failure` counts; payment service retry counter metric.
- **Mitigation:** Add `SELECT ... FOR UPDATE` at the start of each payment transaction — this converts SSI to pessimistic locking for the payment account row, serializing access at the row level and eliminating serialization failures at the cost of queueing. For shared resources (concert tickets), use a reservation queue pattern rather than direct balance writes.

### 3. CRDT Partition Divergence Beyond Merge Window
- **Symptom:** Post like count drops by 12,000 after a network partition event. Users are confused — the post was going viral.
- **Root cause:** The G-Counter uses `max()` merge, which is safe for a grow-only counter. But if a bug introduced a `min()` merge or a reset operation that wasn't CRDT-safe, partition healing could overwrite higher values with lower ones.
- **Detection:** Like count monitoring — alert on any counter decreasing by more than 100 in a 1-second window (legitimate unlikes are rare and slow).
- **Mitigation:** Test CRDT merge functions with property-based testing: merge(A, B) must equal merge(B, A), merge(merge(A, B), C) must equal merge(A, merge(B, C)), merge(A, A) must equal A (idempotent). Encode these as invariants in CI. Never introduce non-CRDT operations (reset, set-to-value) on CRDT-managed counters.

### 4. Clock Skew Causing LWW Data Loss
- **Symptom:** A user's profile update (email address change) is silently lost 0.1% of the time. Support tickets report "I changed my email, it reverted."
- **Root cause:** LWW conflict resolution using `System.currentTimeMillis()`. NTP clock sync creates ±50ms skew between nodes. A write to node-A at local time T=1000 and a concurrent write to node-B at local time T=999 (1ms behind NTP) — Node A's write loses despite happening later in real time.
- **Detection:** Add a monotonic sequence counter to every write. Validate during read repair that the chosen version has the highest sequence, not just the highest timestamp.
- **Mitigation:** Replace wall-clock LWW with Hybrid Logical Clocks (HLC): each timestamp is `max(local_clock, received_clock) + 1`. HLCs advance monotonically and capture causality without requiring synchronized clocks. Or replace LWW with vector clocks for user-visible mutable data.

### 5. Read Repair Amplifying Load During Recovery
- **Symptom:** After a 30-minute Cassandra node outage, the recovering node's CPU spikes to 100% for 20 minutes. Read latency across the cluster increases.
- **Root cause:** Cassandra's read repair is triggered on every read that hits the recovering node. Every read to the node compares its stale value against the quorum response and issues a background write to repair it. At 52K read QPS hitting the recovering node for 20 minutes, millions of background repair writes are generated simultaneously.
- **Detection:** `ReadRepair` JMX metric spikes on the recovering node; cluster-wide write latency increase.
- **Mitigation:** Set `read_repair_chance=0.0` and `dclocal_read_repair_chance=0.0` on hot tables. Rely on `nodetool repair` (scheduled, rate-limited, Merkle-tree-based) instead of read repair for consistency. Read repair is too bursty at high read QPS — it trades a node outage incident for a recovery incident.

---

## How This Connects Forward

- **Day 8 (URL Shortener):** The URL shortener's 301 vs. 302 redirect choice is a consistency decision: 301 caches at the browser (eventual consistency — the redirect is immutable), 302 forces a fresh lookup (strong consistency — the server always controls the destination). Today's consistency spectrum maps directly to that choice.
- **Day 9 (Search & Analytics):** Elasticsearch is an AP system — it prioritizes availability and partition tolerance over consistency. Index writes are eventually consistent; a document indexed 50ms ago may not appear in search results immediately. Today's BASE model explains why, and what the convergence guarantee is.
- **Day 11 (Distributed Transactions):** 2PC is the protocol that gives ACID atomicity across multiple nodes — it's the CP solution to cross-service transactions. The Saga pattern is the AP alternative: eventual consistency with compensating transactions. Today's CP vs. AP framing is the direct precursor to that design decision.
- **Day 12 (Distributed Key-Value Store):** Designing a Dynamo-style KV store requires implementing every concept from today from scratch: quorum R/W, vector clocks, conflict resolution, read repair, hinted handoff, and anti-entropy. Day 12 is Day 7 + Day 6 fully synthesized.
- **Day 20 (Payment System):** ACID with SERIALIZABLE isolation is the foundation of the payment system's correctness guarantees. The write skew example (two doctors going off call) maps directly to the double-spend problem in payments — same anomaly class, same fix.

---

## Diagrams to Draw

1. **CAP Triangle with System Placement:** Draw the classic triangle but replace the vertices with "CP behavior during partition," "AP behavior during partition," and "CA (single node only)." Place systems on it: etcd/ZooKeeper (CP), Cassandra (AP, default), DynamoDB (AP, default / CP with strong consistency mode), PostgreSQL with sync replication (CP), Redis Cluster (AP, tunable). Add a PACELC annotation to each: latency vs. consistency trade-off outside of partition events.

2. **Quorum Grid — N=3 Configurations:** Draw a 3×3 grid with W (1,2,3) on one axis and R (1,2,3) on the other. Color cells green if `W+R > 3` (strong consistency), yellow if `W+R = 3` (borderline — NOT strong), red if `W+R < 3` (eventual). Mark the (W=2, R=2) cell as "standard QUORUM" and annotate what happens to availability when 1 node fails: does the quorum still succeed?

3. **Write Skew Anomaly Timeline:** Two swim lanes (Transaction 1, Transaction 2). T1: reads on_call count = 2, decides to go off call. T2: reads on_call count = 2 (same committed snapshot), decides to go off call. T1 commits. T2 commits. Final state: 0 on-call doctors. Add a second version of the same diagram with `SELECT FOR UPDATE`: T1 acquires row lock, T2 blocks. T1 commits, count=1. T2 unblocks, reads count=1, aborts.

4. **G-Counter Partition + Merge:** Draw 3 nodes (A, B, C) with a partition isolating C. Show like increments going to A and B during the partition: A=100, B=80. C is stuck at its old value=50 from before the partition. At partition heal: merge using max(A_shard, A_shard_from_C). Show the merged state: {A: 100, B: 80, C: 50_old} → total = 230. Show why `max()` is correct — C's shard value for A (50) is overwritten by A's current shard value (100).

5. **Consistency Models Timeline:** Draw a horizontal timeline with 3 events: Write W1 completes at T=100, Read R1 starts at T=90 (concurrent with write), Read R2 starts at T=110 (after write). For each consistency model (Linearizable, Sequential, Causal, Read-Your-Writes, Eventual), draw which nodes see W1 at R1 time and R2 time. Show that Linearizable requires R2 always sees W1; Eventual allows R2 to still miss W1.
