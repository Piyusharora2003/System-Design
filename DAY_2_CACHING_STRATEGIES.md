# Day 2 — Caching Strategies

---

## 1. Day Summary

Day 2 shifts your thinking from "how do I handle traffic" (Day 1) to "how do I avoid doing the same work twice." Caching is the single highest-leverage performance optimization in system design — it can reduce latency by 100× and cut database load by 90%. But caching introduces the hardest problem in computer science: **cache invalidation**. Today forces you to internalize that every caching decision is a bet on staleness tolerance, and that the "right" caching strategy depends entirely on the read/write ratio, consistency requirements, and failure characteristics of your specific system. If you can't explain the difference between cache-aside and write-through, articulate when write-behind is worth the data-loss risk, and design a stampede-proof caching layer from scratch, you'll struggle with every system from Day 8 onward.

---

## 2. Pre-read Checklist

| Concept | Day Covered | Why It's Required |
|---|---|---|
| Horizontal scaling & stateless servers | Day 1 | Caching only works across a fleet if every server talks to the same external cache (Redis), not local memory |
| Load balancing algorithms | Day 1 | Understanding how requests are distributed helps you reason about cache hit rates — sticky sessions affect per-server local cache effectiveness |
| Redis as an external session store | Day 1 | You already used Redis for sessions; now you're expanding it to general-purpose caching |
| Health checks and failure detection | Day 1 | Cache node failures need the same detection and failover patterns |
| SSL/TLS termination at LB | Day 1 | CDN caching (covered today) terminates TLS at the edge — same concept, different location |

---

## 3. The Problem, Stated Precisely

### Functional Requirements

| Requirement | Detail |
|---|---|
| Reduce database read latency | From ~5–50ms (DB) to < 1ms (cache) for repeated reads |
| Support multiple caching patterns | Cache-aside, read-through, write-through, write-behind |
| Cache invalidation | TTL-based and event-driven invalidation |
| CDN caching for static assets | Serve images, CSS, JS from edge nodes globally |
| Cache eviction | Bounded memory; evict entries when full |

### Non-Functional Requirements

| Metric | Target |
|---|---|
| Cache read latency (p99) | < 1 ms |
| Cache hit ratio | ≥ 80% for hot data |
| Total read QPS (from all clients) | 500,000 QPS |
| DB read QPS after caching | ≤ 50,000 QPS (90% offload) |
| Cache capacity | 100 GB across the cluster |
| Cache availability | 99.99% |
| CDN latency (edge to user) | < 10 ms |
| Staleness tolerance | ≤ 60 seconds for most data; ≤ 0 for financial data |

---

## 4. Capacity Estimation

### Traffic

| Metric | Calculation | Result |
|---|---|---|
| Total read QPS | Given | 500,000 QPS |
| Write QPS | 10:1 read-to-write ratio → 500K / 10 | 50,000 QPS |
| Cache hit rate target | 80% | — |
| Cache-served reads | 500K × 0.80 | 400,000 QPS (served from cache) |
| DB-served reads (misses) | 500K × 0.20 | 100,000 QPS → still high; need 90%+ hit rate |
| Target effective hit rate | 90% | — |
| DB-served reads at 90% | 500K × 0.10 | 50,000 QPS ✓ |

### Storage

| Metric | Calculation | Result |
|---|---|---|
| Average cached object size | 1 KB (JSON payload: user profile, product listing) | — |
| Hot dataset (unique keys accessed/day) | 50 million keys | — |
| Total cache memory needed | 50M × 1 KB | ~50 GB |
| With overhead (Redis metadata ~30%) | 50 GB × 1.3 | ~65 GB |
| Cluster sizing | 3 primary nodes × 32 GB + 3 replicas | ~192 GB provisioned |

### Bandwidth

| Metric | Calculation | Result |
|---|---|---|
| Cache reads | 400K QPS × 1 KB response | 400 MB/s ≈ 3.2 Gbps |
| Cache writes (misses populated) | 100K QPS × 1 KB | 100 MB/s ≈ 800 Mbps |
| Peak (3×) | 3.2 Gbps × 3 | ~10 Gbps from cache cluster |

### CDN

| Metric | Calculation | Result |
|---|---|---|
| Static asset requests | 200,000 QPS (images, CSS, JS) | — |
| Average asset size | 50 KB | — |
| CDN egress bandwidth | 200K × 50 KB = 10 GB/s | ~80 Gbps (served from edge, not origin) |
| CDN cache hit ratio | 95%+ (static assets rarely change) | — |
| Origin fetch rate | 200K × 0.05 = 10K QPS | 10,000 QPS to origin |

---

## 5. Core Approaches

### 5.1 Cache-Aside (Lazy Loading)

**Problem it solves:** Only cache data that's actually requested. Avoids filling the cache with never-read data.

**How it works:**
```
READ:
1. App checks cache: GET cache:key
2. Cache HIT  → return value
3. Cache MISS → query DB, write result to cache (SET cache:key value EX ttl), return value

WRITE:
1. App writes to DB
2. App deletes the cache key (invalidation)
   — NOT updates. Delete forces the next read to repopulate with fresh data.
```

**Why delete, not update on write?** Updating the cache on write introduces a race condition: two concurrent writes could update the DB in order A→B but update the cache in order B→A, leaving stale data in the cache indefinitely. Deleting forces the next read to fetch the authoritative value from the DB.

**Failure modes:**
- **Cold start / cold cache:** After a cache restart, every request misses — all traffic hits the DB simultaneously (thundering herd). Mitigate with cache warming: pre-populate hot keys before routing traffic.
- **Stale data window:** Between the DB write and the cache delete, a concurrent read can fetch the old value from cache. This window is typically < 1ms — acceptable for most systems, not for financial data.
- **Cache delete failure:** If the delete fails (network issue), the stale entry remains until TTL expires. Mitigate with a retry queue or TTL as a safety net.

**When NOT to use:** When you need the cache to always be warm and consistent (use write-through instead). When write latency matters more than read latency (cache-aside adds no overhead to writes, but doesn't help them either).

**Builds on Day 1:** Redis, which you introduced as a session store, is the same infrastructure — now used as a general-purpose cache.

---

### 5.2 Read-Through

**Problem it solves:** Simplifies application code by making the cache itself responsible for fetching on miss.

**How it works:** The cache is configured with a "loader function." When a key is missing, the cache calls the loader (which queries the DB), stores the result, and returns it. The application always reads from the cache — it never talks to the DB directly for reads.

```
App → Cache.get(key)
  → Cache HIT: return value
  → Cache MISS: cache calls loader(key) → DB query → cache stores result → return value
```

**Technical detail:** Typically implemented via caching libraries (Guava Cache in Java, Caffeine, or cache middleware like NCache) rather than raw Redis, because Redis itself doesn't have a built-in loader callback. Some managed caches (AWS ElastiCache) support lazy loading patterns.

**Difference from cache-aside:** In cache-aside, the app manages the miss logic. In read-through, the cache manages it. The app code is simpler, but the cache layer is more complex.

**When NOT to use:** When you need fine-grained control over what gets cached and when. When the cache technology doesn't support loaders (raw Redis).

---

### 5.3 Write-Through

**Problem it solves:** Guarantees the cache is always consistent with the DB by writing to both synchronously.

**How it works:**
```
WRITE:
1. App sends write to cache layer
2. Cache writes to DB synchronously
3. Cache updates its own entry
4. Returns success only after both complete
```

**Consistency guarantee:** The cache is never stale — every write updates both stores atomically (from the application's perspective). No invalidation needed.

**Failure modes:**
- **Write latency doubles:** Every write now has `cache_write_latency + db_write_latency`. For write-heavy systems, this is costly.
- **Cache fills with unread data:** Every written record is cached, even if it's never read again. Wasted memory. Mitigate with a TTL to evict stale entries.
- **Complexity in the cache layer:** The cache must understand the DB write protocol. This isn't raw Redis `SET` — it requires a cache framework or proxy.

**When NOT to use:** Write-heavy workloads where double latency is unacceptable. When most data written is never read (e.g., log ingestion).

---

### 5.4 Write-Behind (Write-Back)

**Problem it solves:** Absorbs write spikes by writing to cache immediately and flushing to DB asynchronously.

**How it works:**
```
WRITE:
1. App writes to cache (fast, ~0.5ms)
2. Return success to client immediately
3. Background worker flushes dirty entries to DB in batches (every N seconds or N entries)
```

**Performance advantage:** Write latency is reduced to cache-write latency only. DB writes are batched, reducing connection overhead and transaction costs. 100 individual INSERTs become one bulk INSERT.

**Data structures:** The cache maintains a "dirty list" — a queue of keys modified since the last flush. A background thread processes this queue. Some systems use a write-ahead log (WAL) in the cache to survive cache crashes.

**Failure modes:**
- **Data loss on cache crash:** If the cache node dies before flushing dirty entries, those writes are lost. This is the critical tradeoff. Mitigate with: (a) replication — the cache replica has the dirty data. (b) WAL — write to a durable log before acknowledging.
- **Eventual consistency:** Between the cache write and the DB flush, the DB is behind. Any direct DB reader sees stale data. This is acceptable only if all reads go through the cache.
- **Flush failure:** If the DB is temporarily down, the dirty queue grows unboundedly. Need a circuit breaker and bounded queue with backpressure.

**When NOT to use:** For financial or transactional data where data loss is unacceptable. When other systems read directly from the DB and need fresh data.

**Forward connection (Day 4):** Write-behind is conceptually similar to the event-driven async patterns in message queues — decouple the fast acknowledgment from the slow persistence.

---

### 5.5 Cache Eviction Policies

| Policy | Mechanism | Best For | Weakness |
|---|---|---|---|
| **TTL (Time to Live)** | Entry expires after N seconds regardless of access | Predictable staleness bound; simple to configure | Doesn't adapt to access patterns; hot keys expire unnecessarily |
| **LRU (Least Recently Used)** | Evict the entry not accessed for the longest time | General-purpose; works well when recent data is likely to be accessed again | Poor for scan workloads — a one-time scan evicts hot entries |
| **LFU (Least Frequently Used)** | Evict the entry accessed the fewest total times | Skewed access patterns (Zipfian); keeps persistently popular items | Slow to adapt to new popular items; requires frequency counters per key |
| **Random** | Evict a random entry | Very low overhead; surprisingly effective for uniform distributions | No intelligence; can evict hot keys |

**LRU implementation (Redis `allkeys-lru`):** Redis approximates LRU by sampling K random keys (default K=5) and evicting the least recently used among the sample. True LRU would require a linked list of all keys (too expensive). This approximation is > 95% as effective as true LRU in practice.

**LFU in Redis (`allkeys-lfu`):** Redis 4.0+ tracks a logarithmic access counter per key (8 bits, decays over time). This prevents old-but-once-popular keys from never being evicted.

**How to choose:**
- Use TTL as a **safety net** alongside any other policy — ensures no entry lives forever even if the eviction policy doesn't touch it.
- Use LRU as the **default** for most workloads.
- Use LFU when a small set of keys accounts for the vast majority of traffic (power-law distribution).

---

### 5.6 CDN Caching

**Problem it solves:** Static assets (images, CSS, JS, video) should not be served from your origin servers. Users worldwide should get sub-10ms latency.

**How it works:** A CDN (CloudFront, Cloudflare, Akamai) operates a network of edge nodes (PoPs — Points of Presence) distributed globally. On first request, the edge node fetches the asset from the origin, caches it, and serves it. Subsequent requests from the same region are served from the edge — no origin hit.

**Cache-Control headers:**
```http
Cache-Control: public, max-age=31536000, immutable
```
- `public`: CDN and browser can cache.
- `max-age=31536000`: cache for 1 year (for versioned assets like `app.a1b2c3.js`).
- `immutable`: don't revalidate even on browser refresh.

For mutable content:
```http
Cache-Control: public, max-age=60, stale-while-revalidate=30
```
- `max-age=60`: fresh for 60 seconds.
- `stale-while-revalidate=30`: serve stale for up to 30s while fetching a fresh copy in the background.

**Cache invalidation at CDN:**
- **URL versioning (cache-busting):** Append a hash to the filename (`style.abc123.css`). A new deploy generates a new filename — the CDN treats it as a new object. This is the preferred approach.
- **Purge API:** Explicitly invalidate a cache key at all edge nodes. Slow (propagation takes seconds to minutes across 200+ PoPs) and expensive (rate-limited). Use sparingly.

**Builds on Day 1:** CDN edge nodes are essentially a globally distributed L7 load-balancing + caching layer. TLS termination also happens at the CDN edge — same concept as LB TLS termination, just closer to the user.

---

### 5.7 Cache Invalidation Strategies

**"There are only two hard things in Computer Science: cache invalidation and naming things."** — Phil Karlton

| Strategy | Mechanism | Consistency | Complexity |
|---|---|---|---|
| **TTL-based** | Key expires after N seconds | Eventual (stale for up to TTL) | Very low |
| **Event-driven delete** | On DB write, explicitly `DEL cache:key` | Near-immediate | Medium (requires pub/sub or direct call) |
| **Event-driven update** | On DB write, `SET cache:key new_value` | Near-immediate | High (race conditions on concurrent writes) |
| **Version-based** | Key includes a version number; increment on write | Strong (old version never served) | Medium |

**Cache stampede (thundering herd on expiry):**

When a popular key expires, hundreds of concurrent requests simultaneously miss the cache and all query the DB for the same key. This can overwhelm the DB.

**Solution 1 — Mutex lock:**
```python
value = cache.get(key)
if value is None:
    if cache.setnx(f"lock:{key}", 1, ex=5):  # acquire lock
        value = db.query(key)
        cache.set(key, value, ex=TTL)
        cache.delete(f"lock:{key}")           # release lock
    else:
        sleep(50ms)                            # wait, then retry
        value = cache.get(key)
```
Only the first request computes the value; others wait and get the cached result.

**Solution 2 — Probabilistic early expiration (XFetch):**
```python
# Each read has a small probability of refreshing the cache BEFORE TTL expires
remaining_ttl = cache.ttl(key)
if random() < BETA * math.log(random()) * -1 * compute_time / remaining_ttl:
    # proactively refresh
    value = db.query(key)
    cache.set(key, value, ex=TTL)
```
Spreads the refresh across time, preventing a simultaneous stampede. `BETA` is a tuning parameter (typically 1.0). `compute_time` is how long the DB query takes — longer queries refresh earlier.

**Solution 3 — Stale-while-revalidate (application level):**
Store the value with a "soft TTL" and a "hard TTL." Between soft and hard TTL, serve the stale value but trigger an async background refresh. After hard TTL, block and recompute.

---

## 6. System Architecture Walkthrough

### The Read Path (cache-aside, the most common pattern)

1. **Client sends GET /api/products/42** to the LB (via Day 1's horizontally scaled fleet).
2. **App server receives the request.** Computes cache key: `product:42`.
3. **Redis lookup:** `GET product:42`. Round-trip: ~0.5ms.
4. **Cache HIT:** Return the cached JSON directly. Total latency: ~2ms. **Skip to step 8.**
5. **Cache MISS:** Query PostgreSQL: `SELECT * FROM products WHERE id = 42`. Latency: ~10ms.
6. **Populate cache:** `SET product:42 <json> EX 300` (5-minute TTL). Latency: ~0.5ms.
7. **Return the response** to the client. Total latency: ~15ms (first request) vs ~2ms (cached).
8. **LB forwards the response** back to the client.

### The Write Path (cache-aside invalidation)

1. **Client sends PUT /api/products/42** with updated data.
2. **App server writes to PostgreSQL:** `UPDATE products SET price = 29.99 WHERE id = 42`. Latency: ~10ms.
3. **App server deletes cache key:** `DEL product:42`. Latency: ~0.5ms.
4. **Return 200 OK.** The next read will miss the cache and repopulate with fresh data.

**Why delete after write, not before?** If you delete before write and the write fails, the cache is empty and the next read repopulates with the old value — correct but unnecessary cache miss. If you delete after write and the delete fails, the cache has stale data — mitigated by TTL as a safety net.

### CDN Path (static assets)

1. **Client requests `GET /static/logo.png`** — DNS resolves to the nearest CDN edge node (e.g., CloudFront PoP in Mumbai).
2. **Edge cache HIT:** Return the asset. Latency: ~5ms. **Done.**
3. **Edge cache MISS:** Edge fetches from origin (your S3 bucket or origin server). Latency: ~100ms. Edge caches the asset with `Cache-Control: max-age=31536000`. Subsequent requests from the same region are served in ~5ms.

### Failure Handling at Each Layer

| Layer | Failure | Impact | Mitigation |
|---|---|---|---|
| **Redis primary** | Node crash | All cache reads miss → DB overloaded | Redis Sentinel promotes replica in ~10s. Circuit breaker returns stale/degraded responses during failover. |
| **Cache miss spike** | Popular key expires | Thundering herd on DB | Mutex lock or XFetch probabilistic early expiration |
| **CDN edge** | PoP goes down | Requests routed to next-nearest PoP | CDN provider handles this; latency increases by ~20ms |
| **Origin overloaded** | Too many CDN cache misses | Origin returns 503 | Origin shield (intermediate CDN cache layer) absorbs miss storms |
| **Cache and DB inconsistency** | Delete fails after write | Stale data served until TTL | TTL safety net; retry queue for failed deletes; monitor cache-vs-DB divergence |

### Bottlenecks at Scale

| Bottleneck | Scale Trigger | Mitigation |
|---|---|---|
| Single Redis node throughput | > 100K QPS per node | Redis Cluster: shard across 6+ nodes by key hash |
| Hot key on one shard | One product/post goes viral | Replicate hot key across shards; client-side local cache with 1s TTL |
| Cache memory exhaustion | Working set exceeds provisioned memory | Scale cluster; tune TTLs; switch eviction from `noeviction` to `allkeys-lru` |
| Network bandwidth from cache | > 10 Gbps sustained egress | Multiple read replicas; compress large values (gzip JSON before caching) |

---

## 7. Data Model

### Cached Object (Redis)

| Field | Type | Example | Purpose |
|---|---|---|---|
| Key | String | `product:42` | Namespace-prefixed key for collision avoidance |
| Value | String (serialized JSON) | `{"id":42,"name":"Widget","price":29.99}` | The cached object |
| TTL | Integer (seconds) | 300 | Auto-expiry; balances freshness vs hit rate |

**Key naming convention:** `<entity>:<id>` for single objects, `<entity>:list:<filter_hash>` for query results. Namespace prevents collisions: `user:42` vs `product:42`.

**Serialization choice:** JSON for human readability and debugging. MessagePack or Protobuf for 30–50% size reduction in high-throughput systems. The tradeoff: binary is smaller and faster to deserialize, but harder to inspect via `redis-cli`.

**Why Redis over Memcached?**

| Dimension | Redis | Memcached |
|---|---|---|
| Data structures | Strings, hashes, lists, sorted sets, streams | Strings only |
| Persistence | RDB snapshots + AOF | None |
| Replication | Built-in primary-replica | None (client-side sharding only) |
| Eviction policies | 8 policies (LRU, LFU, TTL, random, etc.) | LRU only |
| Memory efficiency | Higher overhead per key (~50 bytes metadata) | More memory-efficient per key (~48 bytes) |
| Throughput | ~100K ops/s single-threaded + I/O threads | Higher multi-threaded throughput for simple GETs |
| Use when | You need data structures, persistence, or pub/sub | Pure key-value caching at maximum throughput |

**Access patterns:** `GET` by key (O(1)), `SET` with TTL (O(1)), `DEL` (O(1)), `MGET` for batch reads (O(N) where N = number of keys). No range queries, no joins — if you need those, you're using the wrong tool.

**Scaling:** Redis Cluster hashes keys into 16,384 slots using CRC16. Each node owns a subset of slots. Adding a node triggers slot migration (live resharding). This links directly to **Day 6 (Consistent Hashing)** — Redis Cluster's slot-based distribution is a form of partitioned hashing.

### CDN Cache Entry

| Field | Type | Example | Purpose |
|---|---|---|---|
| Cache key | URL path | `/static/logo.a1b2c3.png` | Full URL including version hash |
| Value | Binary blob | Image bytes | The asset itself |
| TTL | From `Cache-Control` header | 31,536,000 seconds (1 year) | Controlled by origin headers |
| Edge location | PoP identifier | `IAD-01` (Virginia PoP) | Where this copy is cached |

**Storage choice:** CDN is managed infrastructure (CloudFront, Cloudflare). You don't choose the storage engine — the CDN provider optimizes this. Your control surface is the `Cache-Control` header and purge API.

---

## 8. Interview Questions with Model Answers

### Q1 (Mid-Level): What is cache-aside and when would you use it?

**Model Answer:** Cache-aside (lazy loading) means the application checks the cache first. On a miss, it reads from the database, writes the result to the cache with a TTL, and returns. On subsequent reads, the cache serves the data directly. I'd use cache-aside when the read-to-write ratio is high (10:1 or more), the system can tolerate short staleness windows (bounded by TTL), and I want to avoid caching data that's never read. It's the default pattern for most web applications because it's simple, doesn't require the cache to know about the DB, and only caches data that's actually in demand.

**Follow-up:** "What happens when a cached key expires and 1,000 requests arrive simultaneously for it?"

**Trap:** Candidates describe cache-aside correctly but don't mention the stampede problem. A complete answer must address TTL expiry under high concurrency.

---

### Q2 (Mid-Level): What's the difference between write-through and write-behind caching?

**Model Answer:** Write-through writes to the cache and database synchronously — the client receives a response only after both writes succeed. The cache is always consistent with the DB, but write latency doubles. Write-behind writes to the cache immediately and returns success, then asynchronously flushes to the DB in the background. Write latency drops to cache-write latency only, and DB writes can be batched. The tradeoff is risk: if the cache node crashes before flushing, those writes are lost. I'd use write-through for data where consistency is critical (user settings, auth tokens) and write-behind for high-throughput writes where best-effort durability is acceptable (analytics events, view counters).

**Follow-up:** "How would you prevent data loss in a write-behind cache?"

**Trap:** Candidates say write-behind is "faster with no downsides." The data loss risk is the defining tradeoff and must be explicitly stated.

---

### Q3 (Senior): You have a product page that receives 50,000 reads/second. The product's price gets updated once per hour. Design the caching strategy.

**Model Answer:** I'd use cache-aside with a 5-minute TTL. At 50K reads/sec and a 300s TTL, the cache miss rate is negligible — one miss every 5 minutes per cache node. When the price is updated (once per hour), the write path deletes the cache key, and the next read repopulates with the new price. The staleness window is at most ~1ms (between DB write and cache delete). For this specific key, I'd also apply XFetch (probabilistic early expiration) to prevent even the small stampede on TTL expiry — at 50K QPS, even a 100ms window of cache misses means 5,000 DB hits. If strict price consistency is required (e.g., at checkout), the checkout service reads directly from the DB for the authoritative price, bypassing the cache entirely.

**Follow-up:** "What if this product goes viral and 500K reads/sec hit the same cache key — which is stored on a single Redis shard?"

**Trap:** Candidates design a great caching strategy but don't address the hot-key problem. At 500K QPS on a single key, the Redis shard CPU saturates. The answer is to replicate the key across multiple shards or use client-side local caching with a 1-second TTL.

---

### Q4 (Senior): Explain how you'd implement event-driven cache invalidation across multiple services that share a database.

**Model Answer:** When Service A writes to a shared database table, it publishes an invalidation event to a Kafka topic (e.g., `cache.invalidation`). Every service that caches data from that table subscribes to this topic. On receiving an event like `{table: "products", id: 42, action: "UPDATE"}`, each service deletes the relevant cache keys from its local Redis. This decouples invalidation from the write path — Service A doesn't need to know which services cache its data. I'd keep TTL as a safety net (if an event is dropped, the cache still expires). For ordering guarantees, I'd partition the Kafka topic by entity ID so all events for `product:42` go to the same partition and are processed in order.

**Follow-up:** "What happens if the Kafka consumer lags and invalidation events are processed late?"

**Trap:** Candidates describe publish/subscribe but don't address failure modes: consumer lag, duplicate events (need idempotent deletes, which `DEL` naturally is), or event loss. TTL as a safety net is the key insight.

---

### Q5 (Staff/Principal): You're designing a multi-region system where users can write in any region. How do you keep the cache consistent across regions?

**Model Answer:** This is a fundamentally hard problem because you're combining multi-master writes with distributed caching. My approach: each region has its own Redis cluster caching local reads. On a write in Region A, the application invalidates the local cache immediately, writes to the local DB, and publishes a cross-region invalidation event via a global Kafka topic (replicated to all regions via MirrorMaker or Confluent Cluster Linking). Region B's consumer receives the event and deletes the cache key in Region B's Redis. The staleness window is the cross-region replication lag (~100–500ms). For data that cannot tolerate this window (financial, identity), I'd skip caching entirely and read from the primary DB region with a cross-region DB read. The key tradeoff: I'm accepting eventual consistency in the cache layer to avoid the latency and complexity of synchronous cross-region cache invalidation, which would negate the performance benefit of caching in the first place.

**Follow-up:** "How do you handle the 'split-brain' scenario where both regions think they have the latest version?"

**Trap:** Candidates try to make the cache strongly consistent across regions, which defeats the purpose of regional caching. The correct answer is to accept eventual consistency in the cache (bounded by TTL) and ensure the database layer handles conflict resolution (vector clocks, LWW — covered in Day 7). The cache reflects whatever the DB has; it doesn't introduce new conflicts.

---

## 9. Trade-offs to Articulate

1. **"I chose cache-aside over write-through because our read-to-write ratio is 50:1, which means write-through would cache hundreds of thousands of entries that are never read, wasting memory. The tradeoff I'm accepting is that the first read after a write is always a cache miss (~15ms instead of ~2ms)."**

2. **"I chose a 300-second TTL over a 30-second TTL because at 500K read QPS, a shorter TTL means 10× more cache misses hitting the database, which means the DB needs 10× more read capacity. The tradeoff I'm accepting is that users may see data up to 5 minutes stale — acceptable for product listings, not for inventory counts."**

3. **"I chose event-driven invalidation over TTL-only because some data (pricing, user permissions) cannot be stale for 5 minutes, which means TTL alone would cause incorrect authorization decisions. The tradeoff I'm accepting is the operational complexity of maintaining a Kafka-based invalidation pipeline and handling cases where events are delayed or lost."**

4. **"I chose Redis over Memcached because we need sorted sets for leaderboard caching and pub/sub for invalidation broadcast, which means Memcached's string-only model is insufficient. The tradeoff I'm accepting is ~30% higher memory overhead per key due to Redis's richer metadata structures."**

5. **"I chose XFetch (probabilistic early expiration) over mutex locks for stampede prevention because at 50K QPS on a single key, a mutex creates a queueing bottleneck where thousands of requests wait for one thread to populate the cache, which means tail latency spikes to hundreds of milliseconds. The tradeoff I'm accepting is occasional redundant DB queries (multiple early refreshes) — which is far cheaper than a synchronized bottleneck."**

6. **"I chose URL-versioned cache busting for CDN assets over purge-based invalidation because purge takes seconds to minutes to propagate across 200+ global PoPs, which means users in some regions would see stale assets for an unpredictable duration. The tradeoff I'm accepting is that every deploy generates new URLs, meaning users re-download all changed assets — but since assets are typically < 500KB and bandwidth is cheap, this is acceptable."**

---

## 10. Failure Modes and Resilience Patterns

### 1. Redis Primary Node Crash

| Aspect | Detail |
|---|---|
| **Symptoms** | Cache hit rate drops to 0%; DB QPS spikes 5–10×; latency increases from 2ms to 50ms+ |
| **Root cause** | OOM kill, hardware failure, kernel panic on the Redis server |
| **Detection** | Redis Sentinel detects missed pings within 5s; app servers log connection errors; alert on `cache_hit_rate < 50%` |
| **Mitigation** | Redis Sentinel promotes a replica to primary within 10–30s. App servers reconnect automatically via Sentinel-aware client. During the failover window, app servers fall through to DB — circuit breaker throttles DB connections to prevent overload |

### 2. Cache Stampede on Hot Key Expiry

| Aspect | Detail |
|---|---|
| **Symptoms** | DB query spike for a single key; query latency for that key jumps to 500ms+; potentially cascading to other queries via connection pool exhaustion |
| **Root cause** | A key with 50K QPS expires at exactly its TTL; all requests simultaneously miss |
| **Detection** | Alert on DB QPS spike exceeding 3× baseline within a 10s window; query-level monitoring shows one specific query spiking |
| **Mitigation** | XFetch for proactive refresh; mutex lock as fallback. Post-incident: identify all keys with QPS > 10K and apply XFetch policy. Consider "infinite TTL + event-driven invalidation" for the hottest keys |

### 3. CDN Origin Overload (Cache Miss Storm)

| Aspect | Detail |
|---|---|
| **Symptoms** | Origin server returns 503; CDN serves stale content or errors; alerts on origin error rate |
| **Root cause** | A CDN purge was issued for a popular asset, or a deploy changed many URLs simultaneously, causing all PoPs to re-fetch from origin |
| **Detection** | Origin QPS exceeds its capacity threshold; CDN dashboard shows cache hit rate drop |
| **Mitigation** | Use an **origin shield** — an intermediate CDN caching layer that absorbs requests from all edge PoPs. Only the shield fetches from origin. Also: stagger deploys, avoid mass purges, use `stale-while-revalidate` to serve stale while re-fetching |

### 4. Cache-DB Inconsistency After Failed Delete

| Aspect | Detail |
|---|---|
| **Symptoms** | Users see outdated data for specific records; no DB errors, no cache errors — the system *appears* to work but data is wrong |
| **Root cause** | App successfully wrote to DB but the subsequent `DEL cache:key` failed (Redis network timeout), leaving a stale entry |
| **Detection** | Hard to detect in real time. Use a reconciliation job that samples cache entries and compares with DB values. Alert on divergence rate > 0.1% |
| **Mitigation** | TTL as safety net ensures stale data self-heals within N seconds. For critical data: use a transactional outbox — write the invalidation to a DB table in the same transaction as the data write, then a background worker processes the outbox and deletes cache keys. This guarantees the invalidation is not lost |

### 5. Memory Exhaustion on Redis Cluster

| Aspect | Detail |
|---|---|
| **Symptoms** | Redis returns `OOM command not allowed when used memory > maxmemory`; new writes fail; cache stops accepting new entries |
| **Root cause** | Working set grew beyond provisioned memory; keys have long TTLs or no eviction policy is set (`maxmemory-policy noeviction`) |
| **Detection** | Redis `used_memory` metric approaching `maxmemory`; alert at 80% threshold |
| **Mitigation** | Set `maxmemory-policy allkeys-lru` so Redis automatically evicts least-recently-used keys when full. Monitor eviction rate — if evictions are high, the cluster needs more nodes. Reduce TTLs for low-value keys. Review key sizes (some values may be unexpectedly large — use `MEMORY USAGE key` to audit) |

---

## 11. How This Connects Forward

| Concept from Day 2 | Required In | Dependency |
|---|---|---|
| **Cache-aside pattern** | Day 8 (URL Shortener) | Hot URL lookups are the textbook cache-aside use case; 80/20 Pareto caching |
| **Cache eviction (LRU/LFU)** | Day 11 (Key-Value Store) | Building a distributed key-value store requires implementing eviction policies at the storage engine level |
| **CDN caching + Cache-Control headers** | Day 15 (Video Streaming) | Video segments are cached at CDN edge; ABR relies on CDN for global delivery |
| **Cache stampede prevention** | Day 19 (Distributed Cache) | Day 19 goes deep on thundering herd, hot keys, and stampede — all introduced here |
| **Write-behind pattern** | Day 4 (Message Queues) | Async write-behind is structurally identical to producing to a message queue for async processing |
| **TTL and staleness tolerance** | Day 7 (CAP/Consistency Models) | TTL-based caching is a pragmatic implementation of eventual consistency; the TTL defines the staleness bound |
| **Event-driven invalidation via Kafka** | Day 4 (Event-Driven Architecture), Day 10 (Notifications) | The cache invalidation pipeline is a specialized form of event-driven architecture |

---

## 12. Diagrams to Draw (Descriptions Only)

### Diagram 1: Cache-Aside Read/Write Flow
Draw two parallel flow diagrams — one for READ and one for WRITE. **READ:** Client → App Server → decision diamond "Cache HIT?" → YES → return cached value. NO → DB query → populate cache (SET with TTL) → return value. **WRITE:** Client → App Server → DB write → DEL cache key → return 200 OK. Label each arrow with the Redis/DB command and the latency (~0.5ms for cache, ~10ms for DB).

### Diagram 2: Cache Stampede and Mutex Lock Solution
Draw a timeline with 5 concurrent requests arriving at the same moment (when key expires). Without protection: all 5 hit the DB simultaneously (draw 5 parallel arrows to DB). With mutex lock: Request 1 acquires the lock, queries DB, populates cache. Requests 2–5 hit the lock, wait 50ms, retry, and get the cached value. Only 1 DB query instead of 5.

### Diagram 3: Multi-Layer Caching Architecture
Draw layers top to bottom: **Browser Cache** (local, fastest) → **CDN Edge** (regional, ~5ms) → **Application Cache (Redis)** (~1ms from app server) → **Database** (~10ms). Show a request traversing each layer, hitting at the CDN level (for static) or the Redis level (for dynamic). Label each layer with its capacity, latency, and what it caches.

### Diagram 4: Write-Behind Async Flush
Draw: Client → App Server → Redis (cache write, return 200 immediately). Then separately: Background Worker reads the "dirty queue" from Redis and writes batches to DB. Draw a red "X" on the Redis node showing data loss risk if it crashes before the flush. Draw a replicated Redis node as the mitigation.

### Diagram 5: CDN Cache Flow with Origin Shield
Draw: User → nearest CDN Edge PoP → decision "Edge HIT?" → YES → return asset. NO → **Origin Shield** (intermediate CDN layer) → decision "Shield HIT?" → YES → return to Edge, Edge caches it. NO → Origin Server (S3), fetch asset → Shield caches → Edge caches → return to User. Label each hop with latency. Show multiple Edge PoPs all pointing to one Shield node, not directly to origin.

---
