# Day 3 — Database Design & Scaling

## Day Summary

Day 3 forces a mental model shift from *request routing* (Days 1–2) to *data architecture*. The real question isn't "SQL or NoSQL" — that's a red herring interviewers use to spot shallow candidates. The actual problem is: given a specific access pattern, consistency requirement, and growth trajectory, which storage engine's internal architecture fits your read/write shape without requiring heroic ops effort at scale? Every approach today — replicas, sharding, denormalization — is a direct tradeoff between write amplification, query flexibility, and operational complexity. You'll leave today understanding that scaling a database is less about picking the right product and more about decomposing your schema to match your hottest access patterns.

---

## Pre-read Checklist

- **Stateless app servers** (Day 1) — Replica routing and shard selection must happen at the app or proxy layer, which only works when servers carry no sticky state.
- **L7 load balancing for read/write splitting** (Day 1) — Read replicas require routing SELECT vs. INSERT differently, often at the connection proxy layer using the same routing logic as an L7 LB.
- **Cache-aside and cache invalidation** (Day 2) — Sharding and denormalization create cache invalidation complexity; you need to know which cache keys to bust when a write touches a denormalized row.
- **Write-through vs. write-behind** (Day 2) — The write path design for caching directly conflicts or complements write-behind DB patterns; you must reason about both layers simultaneously.
- **TTL and LRU eviction** (Day 2) — Replica lag (~100ms–1s) interacts with cache TTLs; a cache hit that bypasses a lagging replica can surface data *older* than the replica's lag window.

---

## The Problem, Stated Precisely

**Context:** You're designing the storage layer for a social platform.

### Functional Requirements
- Users can create, read, update, and delete posts
- Users can follow other users and query a list of followers/following
- Users can query their own activity feed (posts by accounts they follow)
- Search posts by hashtag

### Non-Functional Requirements

| Parameter | Target |
|---|---|
| DAU | 50 million |
| Average writes per user per day | 2 (posts, likes, follows) |
| Average reads per user per day | 30 (feed, profile, search) |
| Write QPS (average) | **~1,160 QPS** |
| Write QPS (peak, 3× average) | **~3,500 QPS** |
| Read QPS (average) | **~17,400 QPS** |
| Read QPS (peak) | **~52,000 QPS** |
| Post body size | ~1 KB |
| Storage growth per day | **~1,160 KB/s × 86,400 = ~100 GB/day** |
| Storage growth per year | **~36 TB/year** |
| Read latency SLA | p99 < 50ms for feed, p99 < 200ms for search |
| Write latency SLA | p99 < 100ms |
| Durability | RPO = 0 (no data loss); RTO < 30s |
| Replication factor | ≥ 3 |
| Availability | 99.99% (< 52 min downtime/year) |

---

## Capacity Estimation

### Write QPS
```
50M DAU × 2 writes/day = 100M writes/day
100M / 86,400 seconds ≈ 1,160 writes/sec (average)
Peak = 3× average ≈ 3,500 writes/sec
```

### Read QPS
```
50M DAU × 30 reads/day = 1.5B reads/day
1.5B / 86,400 ≈ 17,400 reads/sec (average)
Peak ≈ 52,000 reads/sec
```

### Storage
```
Writes: 1,160/sec × 1 KB = 1.16 MB/sec ingress
Per day: 1.16 MB/sec × 86,400 ≈ 100 GB/day
Per year: 100 GB × 365 ≈ 36 TB/year

With replication factor 3: 36 TB × 3 = 108 TB raw storage/year
With 20% overhead (indexes, metadata): ~130 TB provisioned/year
```

### Bandwidth
```
Ingress: 1.16 MB/sec
Egress: 17,400 reads/sec × 2 KB avg response = ~34 MB/sec
With CDN offloading 70% of static reads: origin egress ≈ 10 MB/sec
```

### Cache Memory
```
Working set: top 20% of posts serve 80% of reads (Pareto)
Active posts/day: 1,160/sec × 86,400 × 0.20 = ~20M posts
At 2 KB each (post + metadata): 20M × 2 KB = 40 GB
Redis cluster: 3 nodes × 16 GB = 48 GB — covers working set with headroom
```

---

## Core Approaches

### 1. SQL vs. NoSQL Decision Framework

**What it solves:** Choosing a storage engine that fits the data's relational structure and query shape without fighting the engine's internals.

**Technical depth:**
- SQL engines (PostgreSQL, MySQL) use a B-tree index structure internally. B-trees give O(log N) lookup and are optimized for range scans. The query planner chooses among indexes, sequential scans, and hash joins — this only works well if your schema is normalized and your access patterns are predictable.
- NoSQL engines like Cassandra use an LSM-tree (Log-Structured Merge-tree): all writes go to an in-memory memtable, flushed to SSTables on disk, and periodically compacted. This gives O(1) write throughput (no in-place update) at the cost of read amplification (must check multiple SSTables for a key).
- DynamoDB uses a variant of consistent hashing over SST-based storage. Its pricing model punishes full-table scans — schema design must front-load partition key selection.

**Failure modes:**
- SQL: schema migrations become table-locking operations at tens of millions of rows unless you use online schema change tools (pt-online-schema-change, gh-ost).
- NoSQL: choosing the wrong partition key creates hot partitions. In Cassandra, an unbounded partition (e.g. all posts for a trending hashtag in one row) causes a single node to become overloaded — the "wide partition anti-pattern."

**When NOT to use NoSQL:** When you need multi-table ACID transactions (e.g. debit/credit), ad-hoc analytical queries with JOINs, or when your team lacks operational expertise to tune compaction and manage tombstones in Cassandra.

**Interaction with prior days:** The read replica routing from this day maps directly to L7 load balancer concepts from Day 1 — the proxy (e.g., ProxySQL) inspects the SQL verb (`SELECT` vs. `INSERT`) the same way an L7 LB inspects an HTTP verb.

---

### 2. Read Replicas

**What it solves:** Read QPS exceeds what a single primary instance can serve (our target: 52K peak read QPS). A single PostgreSQL instance typically tops out at ~10K–20K simple queries/sec.

**How it works:**
- The primary writes to its WAL (Write-Ahead Log). Replicas stream the WAL in near-real-time (streaming replication) or receive it in bulk (file-based). The replica replays WAL entries to maintain an identical B-tree state.
- Replication lag: the time between a write committing on primary and the replica applying it. Under normal conditions: 10–200ms. Under heavy write load or network partition: seconds to minutes.
- **Monotonic read consistency** problem: if a client reads from replica A, then replica B, and B has more lag than A, the client sees data "go backward in time." Fix: session pinning (route a client's session to a single replica) or read-your-writes (route reads that immediately follow a write to the primary for a configurable window, e.g. 500ms).

**Failure modes:**
- Primary fails → promote the replica with the least lag. Promotion takes 10–30s in managed services (RDS Multi-AZ). In-flight writes at the moment of failure are lost if WAL wasn't fully flushed to a replica (RPO > 0).
- Replica divergence: if a replica falls too far behind (replica lag > `max_standby_streaming_delay`), PostgreSQL pauses it to avoid conflicts. The replica appears online but serves stale data — your monitoring must alert on replica lag, not just replication connection status.

**When NOT to use:** When write QPS is the bottleneck (replicas don't help writes). When your reads are always for data just written (replication lag makes this dangerous without read-your-writes logic).

**Complexity:** O(1) read per query per replica, lag is O(write throughput × network RTT).

---

### 3. Sharding (Horizontal Partitioning)

**What it solves:** Write QPS or total data size exceeds a single instance's capacity. At 36 TB/year and 3,500 write QPS peak, we've exceeded both.

**How it works — three strategies:**

| Strategy | Key | Distribution | Range queries | Hot spots |
|---|---|---|---|---|
| Range sharding | Numeric range (userId 1–1M → shard 1) | Uneven unless traffic is uniform | Efficient (single shard) | Yes — new users cluster on the latest shard |
| Hash sharding | hash(shardKey) % N | Uniform | Must scatter-gather | No |
| Directory-based | Lookup table: key → shardId | Flexible | Configurable | The lookup table itself |

**Hash sharding internals:** Apply MurmurHash3 (non-cryptographic, fast) to the shard key. `shard = murmurhash(userId) % num_shards`. The modulo operation is the brittleness — adding a shard requires rehashing all keys unless you layer consistent hashing on top (Day 6 will cover this precisely).

**Resharding:** When `num_shards` changes, you must migrate data. Online resharding strategies:
1. Double-write to old and new shard during migration, then cutover.
2. Use a proxy layer (Vitess, Citus) that manages shard maps and makes resharding transparent to the application.

**Failure modes:**
- **Hot shard:** A celebrity's userId hashes to shard 3, and all reads for their data hit shard 3. Solution: shard by a compound key (userId + bucket) or use application-level caching (Day 2) to absorb celebrity read traffic.
- **Cross-shard queries:** "Show me all posts created today" requires a scatter-gather across all shards. At N=20 shards, this is 20× latency and 20× DB load. Avoid by denormalizing or using a separate analytics store (covered Day 9).

**When NOT to use:** When your access patterns require frequent cross-shard aggregations — the operational and latency cost dominates. Start with read replicas + caching before adding sharding complexity.

---

### 4. Vertical Partitioning

**What it solves:** A table has wide rows where only 2–3 columns are accessed on 95% of queries. Full-row scans waste I/O and blow your buffer pool.

**How it works:** Split `users` (with 30 columns) into `users_core` (id, username, email — hot, accessed every request) and `users_profile` (bio, website, avatar_url — cold, accessed on profile page only). The hot table's rows are smaller, more fit in a single 8KB page, and the buffer pool caches more of it effectively.

**Interaction with prior days:** Smaller rows → better cache hit rate in Redis (Day 2). The hot columns fit in a compact cache entry with a long TTL; the cold columns get a short TTL or are fetched on demand.

**Failure mode:** Joins between the split tables become necessary, adding latency. Mitigate by caching the assembled object at the application layer.

---

### 5. Connection Pooling

**What it solves:** At 52K read QPS with 20 app server pods, each pod making direct DB connections = 20 × (pool size per pod) connections. PostgreSQL forks a process per connection; at >500 connections, memory and context-switching overhead degrades throughput by 30–40%.

**How it works:** PgBouncer sits between app servers and PostgreSQL in **transaction-mode pooling**: a client connection is bound to a server connection only for the duration of a transaction, then released back to the pool. A single PgBouncer with a 100-connection pool can multiplex thousands of client connections.

**Failure modes:**
- Transaction-mode pooling breaks `SET` statements, advisory locks, and `LISTEN/NOTIFY` — these require session-mode pooling.
- PgBouncer itself becomes a SPOF — run two instances behind a VIP (same pattern as the HA load balancer from Day 1).

**Complexity:** Connection acquisition: O(1) from the pool. Pool saturation under spike → queuing → latency spike. Set `pool_mode=transaction`, `max_client_conn=10000`, `default_pool_size=100`.

---

### 6. Denormalization

**What it solves:** The follower feed query — "give me the 50 most recent posts from users I follow" — requires a JOIN across `follows` (potentially 1,000 rows per user) and `posts`. At 52K read QPS, this JOIN is catastrophic.

**How it works:** Store a precomputed `feed` table per user: `(userId, postId, createdAt)`. On every new post by user A, fan-out to all of A's followers and insert into their feed tables. Read is now a single index scan: `SELECT * FROM feed WHERE userId=X ORDER BY createdAt DESC LIMIT 50`.

**Write amplification:** A user with 1M followers generates 1M writes per post. This is the "celebrity problem" — covered in Day 8 (News Feed).

**When NOT to use:** When the write amplification is unbounded (high-follower accounts). Hybrid: fan-out on write for normal users, fan-out on read for celebrities (merge at read time).

---

### 7. Multi-Master Replication

**What it solves:** A single primary is a write bottleneck and a single point of failure for writes. Multi-master allows writes to be accepted at any node.

**How it works:** Each master asynchronously replicates its writes to all other masters. Conflict detection is necessary when two masters accept concurrent writes to the same row. Resolution strategies:
- **Last-write-wins (LWW):** The write with the highest timestamp wins. Simple but can lose data if clocks are skewed.
- **Application-level merge:** The application receives both conflicting versions and decides (e.g., CRDTs for counters, user-visible conflict for edits).

**Failure modes:** LWW with NTP clock drift can silently lose writes. Circular replication bugs can create infinite loops. Multi-master is operationally complex — use it only when write HA is strictly required and you have a conflict resolution strategy.

**When NOT to use:** For most OLTP systems, a promoted read replica with automatic failover (RDS Multi-AZ, Patroni) achieves write HA with far less complexity.

---

## System Architecture Walkthrough

### Write Path

1. Client sends `POST /posts` to the L7 load balancer (from Day 1).
2. LB routes to one of N stateless app server pods.
3. App server validates the request, constructs the post object, and begins a DB write.
4. The write lands on the **primary** PostgreSQL instance (PgBouncer routes `INSERT` statements to the primary connection pool).
5. PostgreSQL writes to the WAL, commits, and ACKs the app server. Latency budget: ~5ms.
6. A background worker reads the new post from an outbox table and fans out to follower feed tables. This is async — the write response does not wait for fan-out.
7. The WAL entry streams to 2 read replicas asynchronously. Replication lag: ~50–200ms.
8. The app server also writes the post to Redis (write-through, from Day 2) and publishes a `post.created` event for downstream consumers (search indexing, notifications).

### Read Path (Feed)

1. Client sends `GET /feed` to the LB.
2. LB routes to an app server pod.
3. App server checks Redis for `feed:{userId}`. On cache hit: return immediately (p99 ~2ms).
4. On cache miss: query the `feed` table on a **read replica** via PgBouncer read pool.
5. `SELECT postId FROM feed WHERE userId=? ORDER BY createdAt DESC LIMIT 50` — single index scan, ~5–15ms.
6. Fetch post bodies for the 50 postIds: Redis pipeline (batch GET, ~3ms) or replica JOIN.
7. Populate Redis cache for `feed:{userId}` with TTL=30s.
8. Return to client. Total p99 (cache miss path): ~25ms — within SLA.

### Failure Handling

- **Primary DB failure:** Patroni detects primary loss via etcd consensus, promotes the replica with the shortest lag within ~15s. PgBouncer is reconfigured to point to the new primary. Writes that were in-flight during failover fail with a connection error — app servers retry with exponential backoff.
- **Replica failure:** PgBouncer's health check removes the dead replica from the pool. Reads redistribute to surviving replicas. Add alert on replica count dropping below 2.
- **Shard hot spot:** App-layer rate limiting per userId, combined with Redis absorbing the read load (Day 2 cache-aside), prevents a hot userId from overwhelming a shard.

### Bottleneck Progression

| Scale | Bottleneck | Solution |
|---|---|---|
| 1K write QPS | Single primary write throughput | Acceptable; no action |
| 10K write QPS | Primary I/O saturation | Add connection pooling, tune WAL settings |
| 50K write QPS | Single primary CPU ceiling | Shard by userId (8 shards) |
| 500K write QPS | Per-shard CPU | Increase shard count; add Vitess proxy layer |

---

## Data Model

### Entity: `posts`

| Field | Type | Notes |
|---|---|---|
| post_id | UUID (primary key) | Shard key for hash sharding |
| user_id | UUID | FK to users; indexed |
| body | TEXT (≤ 280 chars) | Stored in PostgreSQL; larger blobs → object store |
| created_at | TIMESTAMPTZ | Indexed (DESC) for feed queries |
| hashtags | TEXT[] | PostgreSQL GIN index for hashtag search |

**Storage:** PostgreSQL — ACID guarantees for writes, B-tree on `(user_id, created_at)` for timeline queries. Sharded by `hash(user_id) % N`.

### Entity: `follows`

| Field | Type | Notes |
|---|---|---|
| follower_id | UUID | Compound PK |
| followee_id | UUID | Compound PK |
| created_at | TIMESTAMPTZ | |

**Storage:** PostgreSQL. Access patterns: "who does user X follow" (index on `follower_id`) and "who follows user X" (index on `followee_id`). This table is read only during fan-out and profile views — a natural candidate for a read replica query.

### Entity: `feed` (denormalized)

| Field | Type | Notes |
|---|---|---|
| user_id | UUID | Partition key |
| post_id | UUID | Sort key |
| created_at | TIMESTAMPTZ | For ordering |

**Storage:** Cassandra or DynamoDB — this table is write-heavy (fan-out) and accessed only by partition key + sort key. No JOINs, no transactions needed. LSM-tree write path matches the insert-only fan-out pattern. Partition key is `user_id`; sort key `created_at DESC` enables efficient feed reads.

### Entity: `users_core`

| Field | Type | Notes |
|---|---|---|
| user_id | UUID | PK |
| username | VARCHAR(50) | Unique index |
| email | VARCHAR(255) | Unique index |
| created_at | TIMESTAMPTZ | |

**Storage:** PostgreSQL, vertically partitioned from `users_profile`. Cached aggressively in Redis with TTL=300s — this data changes infrequently.

---

## Interview Questions with Model Answers

### Q1 (Mid) — "Why not just add more read replicas instead of sharding?"

**Model answer:** Read replicas scale read throughput but do nothing for write throughput or storage. At 3,500 peak write QPS and 36 TB/year, a single primary will hit both I/O saturation (~2,000 write QPS for a typical instance) and disk limits (~20TB per instance for cost-effective SSD). Sharding is the only option to scale writes horizontally. That said, I'd add replicas first — they're far simpler operationally — and only introduce sharding when the primary's write ceiling is measured, not predicted.

**Follow-up:** "What's your shard key strategy and how do you handle resharding?"

**Pitfall:** Candidates say "just add more replicas" without recognizing the write throughput ceiling.

---

### Q2 (Mid) — "How do you handle the replication lag problem for read-your-writes consistency?"

**Model answer:** After a user submits a post, any subsequent request from that same client to read their timeline should see the post they just wrote. I'd tag the write response with the primary's WAL LSN (log sequence number). On the next read, the app server checks if the designated read replica's `pg_last_wal_replay_lsn()` is ≥ the tagged LSN; if not, it routes the read to the primary for up to 500ms, then falls back to the replica. This bounds the extra primary read load to a short window per write event.

**Follow-up:** "What happens under a replica lag spike of 10 seconds?"

**Pitfall:** Answering "just use sticky sessions" without acknowledging that this re-creates the stateful server problem from Day 1.

---

### Q3 (Senior) — "Your hash-sharded system needs to scale from 8 shards to 16. How do you do this with zero downtime?"

**Model answer:** Naive `key % N` remapping is unacceptable — it moves ~50% of all data. I'd use a proxy layer like Vitess or implement a two-phase migration: (1) introduce a logical shard map in a config store (ZooKeeper/etcd) mapping old shards to new shards; (2) double-write to both old and new shard for all new writes; (3) backfill old data from old shard to new shard using a background migrator that reads in chunks and writes idempotently; (4) once backfill lag reaches zero, flip the read shard map atomically; (5) drain double-writes. Each step is independently rollback-safe.

**Follow-up:** "How do you ensure the backfill doesn't overwhelm the source shard?"

**Pitfall:** Proposing consistent hashing as the solution without explaining that you still need a migration tool — consistent hashing only minimizes remapping, it doesn't automate data movement.

---

### Q4 (Senior) — "Cassandra is masterless with eventual consistency. How does that affect the feed reads?"

**Model answer:** Cassandra's consistency is tunable per-operation. For feed reads, I'd use `QUORUM` for reads and `QUORUM` for writes — with a replication factor of 3, that means 2 nodes must agree, giving linearizability within Cassandra's eventual consistency model (`R + W > N`). The trade-off is a latency increase (~5ms) vs. `LOCAL_ONE`, but for a feed this is acceptable. For the fan-out writes during a post creation spike, I'd drop to `LOCAL_ONE` with a background repair job to reconcile — acceptable because a missing feed item for 100ms is tolerable.

**Follow-up:** "What happens if a Cassandra node is down during a fan-out write?"

**Pitfall:** Not knowing that Cassandra's hinted handoff mechanism (replaying missed writes to a recovered node) has a default 3-hour window, after which repairs are needed.

---

### Q5 (Staff/Principal) — "How would you redesign the data layer if 0.1% of users each have >5M followers and generate 30% of all writes?"

**Model answer:** The celebrity write amplification problem breaks the fan-out-on-write model. I'd implement a hybrid approach: maintain a `is_celebrity` flag (set when followers > threshold, e.g. 500K). For celebrity posts, skip fan-out entirely on write. At read time, the feed service fetches the user's precomputed feed (from fan-out-on-write for normal accounts) and merges it with a lightweight pull of the last 50 posts from each celebrity they follow — a separate `celebrity_posts` index partitioned by `(user_id, created_at)` for efficient range reads. The merge is done in-memory in the feed service (~50 celebrities × 50 posts = 2,500 records in memory, sorted merge in O(N log K)). This converts 5M writes per celebrity post into O(1) writes at creation time and O(K) work per feed read, where K is celebrity count followed.

**Follow-up:** "How do you handle the case where a user follows 100 celebrities and 100 regular users simultaneously?"

**Pitfall:** Treating this as purely a write problem and missing the read-time merge complexity and latency implications.

---

## Trade-offs to Articulate

1. **"I chose hash sharding over range sharding because at this scale, write QPS is uniform across userIds, which means range sharding's hot-spot risk on new users (who cluster on the highest range shard) outweighs its benefit of efficient range queries. The trade-off I'm accepting is that cross-shard range queries (e.g., 'all posts from today') require scatter-gather across all shards."**

2. **"I chose Cassandra for the feed table over PostgreSQL because at 1,160 fan-out writes/sec average (and potentially millions/sec for celebrity posts), Cassandra's LSM-tree append-only write path avoids B-tree page splits and lock contention that would kill write throughput on PostgreSQL at this scale. The trade-off I'm accepting is losing ACID transactions and ad-hoc query flexibility."**

3. **"I chose fan-out-on-write for regular users over fan-out-on-read because at 52K peak read QPS, computing the feed at read time for every request (joining follows × posts) would require cross-shard scatter-gather that would collapse the DB layer. The trade-off I'm accepting is write amplification — 1 post creates N writes where N is follower count — which is bounded for non-celebrity users."**

4. **"I chose PgBouncer transaction-mode pooling over session-mode because at 20 app server pods × 50 connections each, session mode would open 1,000 server-side PostgreSQL connections, adding ~2GB RAM overhead on the DB server and degrading throughput by ~30%. The trade-off I'm accepting is that session-scoped SQL features (advisory locks, prepared statements in some drivers) are unavailable and require workarounds."**

5. **"I chose async fan-out (via outbox pattern) over synchronous fan-out because at peak write QPS, fanning out to followers synchronously in the write request would add O(follower_count × DB_write_latency) to the user's perceived write latency — unacceptable for the p99 < 100ms SLA. The trade-off I'm accepting is that follower feeds have eventual consistency: a new post may not appear in all follower feeds for up to 2–5 seconds."**

6. **"I chose denormalization of the feed table over normalized JOIN queries because at 52K read QPS, a JOIN across follows (avg 500 rows) and posts (indexed) at query time would require ~26B index lookups/day, saturating even a 16-core replica. The trade-off I'm accepting is storage amplification (50M users × 50 cached feed rows × 100 bytes = ~250 GB extra) and write complexity for feed maintenance."**

---

## Failure Modes and Resilience Patterns

### 1. Primary DB Failover During Write Spike
- **Symptom:** 100% write error rate for 15–30s; read traffic continues from replicas.
- **Root cause:** Primary crash (OOM, hardware failure) during peak write load.
- **Detection:** `pg_stat_replication` shows 0 connected standbys; Patroni leader election alert.
- **Mitigation:** Patroni + etcd for automatic leader election. PgBouncer `pause` command holds client connections during switchover rather than failing them. App-layer retry with exponential backoff + jitter absorbs the 15s gap.

### 2. Cassandra Hot Partition (Celebrity Post Fan-Out)
- **Symptom:** Single Cassandra node CPU spikes to 100%; fan-out queue depth grows unboundedly.
- **Root cause:** All fan-out writes for a celebrity's 10M followers hash to partitions owned by one or two nodes.
- **Detection:** Cassandra `nodetool tpstats` shows write dropped messages; per-node request rate metrics.
- **Mitigation:** Apply celebrity threshold — disable fan-out for accounts with >500K followers. Implement rate-limited fan-out workers that spread writes over 60 seconds instead of bursting.

### 3. Replica Lag Spike Causing Stale Feed Reads
- **Symptom:** Users see posts "disappear" after refresh; feed reads return data from 30s ago.
- **Root cause:** Replica replication lag spikes during a write burst or network partition.
- **Detection:** Alert on `pg_stat_replication.write_lag > 1s` on any replica.
- **Mitigation:** Read-your-writes routing (write clients to primary for 500ms after a write). Additionally, Redis feed cache TTL (30s) means stale data is bounded by the TTL rather than lag.

### 4. Shard Imbalance After Schema Migration
- **Symptom:** One shard receives 3× normal write QPS; latency on that shard spikes.
- **Root cause:** A migration that changed the shard key distribution logic or a bulk import of users in a narrow hash range.
- **Detection:** Per-shard write QPS dashboard; alert when any shard exceeds 1.5× the mean.
- **Mitigation:** Vitess's resharding can split a hot shard into two online. Short-term: redirect overflow to a shadow shard in the proxy config.

### 5. Connection Pool Exhaustion Under Thundering Herd
- **Symptom:** DB errors spike (`too many connections`); p99 latency jumps from 15ms to 5s.
- **Root cause:** Deployment event causes all app pods to restart simultaneously; each pod's connection pool opens all connections at startup, exceeding PgBouncer's `max_client_conn`.
- **Detection:** PgBouncer `SHOW POOLS` shows `cl_waiting > 0`; alert threshold at `cl_waiting > 50`.
- **Mitigation:** Stagger pod rollouts (max surge = 25% in Kubernetes rolling update). Connection pool warm-up: open connections lazily, not eagerly at pod start. PgBouncer `reserve_pool_size` handles burst.

---

## How This Connects Forward

- **Day 4 (Message Queues):** The async fan-out outbox pattern used for feed writes is the entry point into event-driven architecture. Day 4 formalizes at-least-once delivery, dead letter queues, and exactly-once semantics for these background workers.
- **Day 6 (Consistent Hashing):** The hash sharding approach used today breaks at resharding time. Day 6 directly addresses this with consistent hashing rings and virtual nodes — replacing `hash(key) % N` with a ring that minimizes key remapping when N changes.
- **Day 8 (News Feed Design):** The celebrity fan-out problem introduced here is the central design challenge of Day 8. Day 8 builds the hybrid fan-out-on-write / fan-out-on-read solution in full, including the merge service and `celebrity_posts` index.
- **Day 9 (Search & Analytics):** The `hashtags` GIN index on the `posts` table works for low-volume search but fails at scale. Day 9 introduces Elasticsearch as a dedicated search index and covers the dual-write pattern from PostgreSQL → Elasticsearch.
- **Day 11 (Distributed Transactions):** Multi-master conflict resolution and the fan-out outbox pattern create distributed consistency challenges that Day 11 resolves with Saga and 2PC patterns.

---

## Diagrams to Draw

1. **Read Replica Write/Read Split:** Draw the full request path — client → L7 LB → app server → PgBouncer → primary (writes) / replica pool (reads). Show the WAL streaming from primary to 2 replicas. Include replication lag arrow and read-your-writes session tracking at the app server.

2. **Hash Sharding with Proxy Layer:** Draw 3 app servers → Vitess/proxy → 4 PostgreSQL shards. Show how `hash(userId) % 4` maps to a specific shard. Draw a second state where shard 2 is split into 2A and 2B during online resharding, with double-write arrows.

3. **Cassandra Feed Fan-Out Write Path:** Draw a post creation event → outbox table → fan-out worker pool → Cassandra cluster (RF=3, 3 nodes). Show the QUORUM write touching 2 of 3 nodes. Show what happens when one Cassandra node is down (hinted handoff).

4. **Hybrid Fan-Out Architecture (Celebrity vs. Regular):** Draw two user types writing a post. Regular user: fan-out worker → N feed table inserts. Celebrity user: write only to `celebrity_posts` table, skip fan-out. At read time: merge service queries both `feed` table (normal follow activity) and `celebrity_posts` (for each followed celebrity), merges sorted results in-memory.

5. **PgBouncer Connection Multiplexing:** Draw 50 app server threads → PgBouncer (transaction mode, pool size = 20) → PostgreSQL (20 server connections). Show 50 client connections being multiplexed over 20 server connections. Include the failure path: PgBouncer becomes a SPOF → draw active-passive PgBouncer pair behind a VIP (same pattern as Day 1's HA load balancer).
