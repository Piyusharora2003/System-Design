# Day 8 — Design a URL Shortener (bit.ly / TinyURL)

## Day Summary

The URL shortener is the canonical "deceptively simple" interview problem. Its surface area is small — take a long URL, return a short code, redirect on lookup — but every layer hides a non-trivial decision: how you generate IDs determines whether short codes are guessable; how you cache determines whether your DB survives a viral link; how you shard determines whether you can resize the cluster without downtime. What this problem really tests is whether you can apply the foundational machinery from Days 1–7 in a concrete, end-to-end design under strict latency and scale constraints, and articulate each decision as a deliberate trade-off rather than a default. The mental model shift from prior days: you're no longer designing subsystems in isolation — you're composing them into a coherent product under a 1000:1 read-to-write asymmetry, which forces aggressive caching, disciplined sharding, and a redirect path that should almost never touch the database.

---

## Pre-read Checklist

- **Consistent hashing for shard routing** (Day 6) — The URL DB is sharded by short code hash. `ring.get_node(short_code)` is the shard lookup in the write and read paths. Know the vnode distribution and resharding mechanics cold.
- **Cache-aside, LRU eviction, and hot key handling** (Days 2, 6) — The top 20% of short URLs drive 80% of traffic. The Redis hot-key scatter pattern from Day 6 applies directly here for viral links. Cache TTL strategy determines DB load under traffic spikes.
- **Bloom filter existence checks** (today's new concept — build on the probabilistic data structures intuition from Day 2's cache stampede prevention).
- **301 vs. 302 as a consistency decision** (Day 7) — 301 is "eventual consistency at the browser level" (the redirect is immutable, the browser caches it forever). 302 is "strong consistency" (every redirect hits your service). Name it that way in the interview.
- **Read replica routing and replication lag** (Day 3) — The redirect read path should hit read replicas, not the primary. Replication lag of 100–200ms is acceptable for a URL that was shortened 10 minutes ago; it's not acceptable for a URL shortened 50ms ago and immediately shared.
- **Kafka fan-out for analytics** (Day 4) — Every redirect generates an analytics event (timestamp, short code, referrer, user agent). This is a high-volume append-only workload; Kafka is the right write path, not the URL DB.

---

## The Problem, Stated Precisely

### Functional Requirements
- Given a long URL, generate a unique short code (7 characters, Base62) and store the mapping.
- Given a short code, redirect to the original long URL with HTTP 302.
- Optional: custom aliases (`bit.ly/my-brand`), expiration dates, per-link analytics (click count, referrer, geography).
- Short codes must not be guessable/enumerable by sequential increment.
- Same long URL submitted twice may or may not return the same short code (discuss trade-off).

### Non-Functional Requirements

| Parameter | Target |
|---|---|
| URLs stored | 100 million total |
| Write QPS (new URLs shortened) | ~115 QPS average; ~350 QPS peak (3×) |
| Redirect QPS | ~115,000 QPS average; ~350,000 QPS peak |
| Read:write ratio | **~1,000:1** |
| Redirect latency SLA | **p99 < 50ms** (including cache miss path) |
| Shorten latency SLA | p99 < 500ms |
| Short code length | **7 characters (Base62)** |
| Short code namespace | 62⁷ ≈ **3.52 trillion** — effectively unbounded at this scale |
| Durability | RPO = 0 (no URL mapping lost after ACK) |
| Availability | 99.99% for redirects; 99.9% for shortening |
| Analytics lag SLA | < 30s (near-real-time click counts) |
| URL expiration | Optional; TTL configurable per link |

---

## Capacity Estimation

### Write Side (URL Shortening)

```
100M URLs over the product lifetime (not per day).
New URLs/day: assume 10M URLs/year → ~27,400/day → ~0.32 URLs/sec average.

Realistic burst model (viral campaign, product launch):
  Peak: ~350 new URLs/sec (uses a pre-allocated ID range from Snowflake worker)
```

### Read Side (Redirects)

```
10 billion redirects/day:
  10,000,000,000 / 86,400 = ~115,740 redirects/sec average
  Peak (3×): ~350,000 redirects/sec

Cache hit rate target: 99% (Pareto: top 1% of URLs drive ~80% of traffic;
top 20% drive ~99% of traffic)

Cache miss rate: 1% → ~1,157 DB reads/sec at average load
                        ~3,500 DB reads/sec at peak
(Well within a 3-shard PostgreSQL replica pool at ~10K QPS each)
```

### Storage

```
Per URL record:
  short_code:  7 bytes
  long_url:    avg 200 bytes (URLs vary 50–2000 bytes; use 200 as p50)
  user_id:     16 bytes (UUID)
  created_at:  8 bytes
  expires_at:  8 bytes (nullable)
  metadata:    ~50 bytes (title, tags)
  Total:       ~290 bytes/record

100M URLs × 290 bytes = 29 GB raw
With indexes (short_code unique, long_url hash for dedup):
  +~20% = ~35 GB total
  Fits on a single PostgreSQL instance; shard for write throughput, not storage.

Redis cache:
  Hot URL entry: short_code (7B key) + long_url (200B value) + TTL overhead = ~250B
  Cache 20M hot URLs (top 20%): 20M × 250B = 5 GB
  → 2× Redis nodes at 4 GB each with headroom. One ring shard per node (Day 6).

Analytics (Kafka → ClickHouse/S3):
  Per click event: ~500 bytes (short_code, timestamp, IP, referrer, UA, country)
  10B events/day × 500B = 5 TB/day raw
  Compressed (LZ4, ~5:1): ~1 TB/day in cold storage
  7-day hot analytics: ~7 TB in ClickHouse
```

### Bandwidth

```
Redirect response: HTTP 302 with Location header
  Response size: ~500 bytes (headers only, no body)
  115,740 redirects/sec × 500B = ~58 MB/sec egress (manageable at L7 LB)

Shorten request/response:
  350 QPS × ~1KB avg = negligible

Analytics Kafka ingest:
  115,740 events/sec × 500B = ~58 MB/sec to Kafka brokers
```

---

## Core Approaches

### 1. Base62 Encoding

**What it solves:** Generating a short, URL-safe, human-typable alias from a numeric ID.

**How it works:**
```python
ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
BASE = 62

def encode(num: int) -> str:
    if num == 0:
        return ALPHABET[0]
    result = []
    while num > 0:
        result.append(ALPHABET[num % BASE])
        num //= BASE
    return ''.join(reversed(result))

def decode(s: str) -> int:
    num = 0
    for char in s:
        num = num * BASE + ALPHABET.index(char)
    return num

# encode(1)         → "b"
# encode(62)        → "ba"    (carries over like base-10 place value)
# encode(3521614606207) → "zzzzzzz"  (max 7-char code)
```

**Namespace math:**
```
6-char code: 62⁶ = 56.8 billion  — sufficient for 100M URLs, but thin margin
7-char code: 62⁷ = 3.52 trillion — 35,000× our 100M target; effectively unbounded
8-char code: 62⁸ = 218 trillion  — overkill; adds one character to every link

Choose 7. Never 6 — the namespace fills if the product succeeds.
```

**Failure mode — leading zero equivalent:** Base62 has no leading-zero ambiguity (unlike Base64), but `encode(0)` returns `"a"` (index 0). The first valid URL gets code `"b"`. Reserve `"a"` to `"aaaaaaa"` as a test/admin namespace rather than exposing them to users.

---

### 2. ID Generation Strategies

**The central design decision:** how you generate the numeric ID that gets Base62-encoded determines uniqueness guarantees, predictability, and throughput.

#### Strategy A: Counter-Based (Snowflake-style)

```
Snowflake ID structure (64-bit integer):
  [1 bit sign][41 bits timestamp ms][10 bits worker ID][12 bits sequence]

41-bit timestamp: covers 2^41 ms = 69 years from epoch
10-bit worker ID: 1,024 unique workers
12-bit sequence: 4,096 IDs per ms per worker = 4.096M IDs/sec per worker

At 350 peak QPS: a single Snowflake worker handles this with room for 10,000×.

Base62(Snowflake ID):
  Max Snowflake ID ≈ 2^63 ≈ 9.2 × 10^18
  encode(9.2 × 10^18) → 11 characters in Base62
  → Too long for 7 chars. Must use a smaller ID space.
```

**Fix — dedicated counter service:**
```
Use PostgreSQL sequence: CREATE SEQUENCE url_id_seq START 1;
SELECT nextval('url_id_seq') → returns 1, 2, 3, ...

At 100M URLs, max ID = 100,000,000
encode(100,000,000) = "FXsGnG" (6 chars) — within 7-char budget.

Distributed counter: run 3 counter service nodes, each pre-allocating ranges:
  Node 1: IDs 1–10,000
  Node 2: IDs 10,001–20,000
  Node 3: IDs 20,001–30,000
  (Refill on exhaustion, similar to HiLo algorithm)
```

**Predictability problem:** `encode(1000001)` = `"4c92"`. An adversary who gets code `"4c92"` can decode it, increment, and enumerate `"4c93"`, `"4c94"` — accessing all shortened URLs in order. At scale, this is a privacy and security risk (private document links, internal campaign URLs).

**Fix for predictability:** Apply a bijective shuffle (Feistel cipher or Knuth shuffle on the ID space) before encoding:
```python
# Feistel network: maps ID → shuffled_ID bijectively (no collisions, reversible)
def shuffle(n: int, rounds: int = 3, key: int = 0xDEADBEEF) -> int:
    # 2-round Feistel on 28-bit halves (for IDs up to 2^56)
    L, R = (n >> 28) & 0xFFFFFFF, n & 0xFFFFFFF
    for _ in range(rounds):
        L, R = R, L ^ (hash(R ^ key) & 0xFFFFFFF)
    return (L << 28) | R
```
Sequential IDs `1, 2, 3` → shuffled `847291, 2918472, 91847` → Base62 codes look random.

#### Strategy B: Hash-Based (MD5 of long URL)

```python
import hashlib, base64

def shorten_hash(long_url: str) -> str:
    digest = hashlib.md5(long_url.encode()).digest()  # 16 bytes
    # Take first 7 chars of Base64url encoding (43 chars total from 16 bytes)
    b64 = base64.urlsafe_b64encode(digest).decode()
    return b64[:7]  # e.g. "dQw4w9W"
```

**Advantages:** Same long URL always produces the same short code (natural deduplication — submitting the same URL twice returns the existing code without a DB round-trip after Bloom filter check).

**Collision probability:**
```
7 chars from 64-char Base64url alphabet = 64⁷ ≈ 4.4 trillion possibilities
At 100M URLs: birthday paradox collision probability ≈ n²/2N
  = (10^8)² / (2 × 4.4 × 10^12) ≈ 10^16 / 8.8 × 10^12 ≈ 1,136 expected collisions

Not negligible. Must handle with:
  1. Collision detected (DB UNIQUE constraint violation)
  2. Append a suffix and retry: hash(long_url + "1"), hash(long_url + "2")
  3. Max 3 retries before falling back to counter-based ID
```

**When NOT to use hash-based:** When the same long URL should produce different short codes for different users (analytics tracking, A/B testing). Hash-based is deterministic — user A and user B shortening the same URL get the same code and can't have separate analytics.

**Chosen approach:** Counter-based with Feistel shuffle. Predictable throughput, zero collision risk, analytics-compatible (each user gets a unique code even for the same destination).

---

### 3. Bloom Filter for Existence Checks

**What it solves:** Before inserting a new short code, check if the code already exists without a DB read on every write. At 350 write QPS, a DB read per write is fine — the Bloom filter earns its keep on the dedup path: "has this long URL been shortened before?"

**How it works:**
```
Bloom filter: a bit array of size m, with k hash functions.
To insert key: set bits at positions h1(key), h2(key), ..., hk(key).
To query key: check if ALL of h1(key), h2(key), ..., hk(key) are set.
  → All set: PROBABLY exists (false positive possible)
  → Any unset: DEFINITELY does not exist (no false negatives)

False positive rate: p ≈ (1 - e^(-kn/m))^k
  where n = number of items inserted, m = bit array size, k = hash functions

Sizing for 100M URLs, p = 0.1% false positive rate:
  m = -n × ln(p) / (ln(2))²
    = -100M × ln(0.001) / 0.480
    = -100M × (-6.908) / 0.480
    ≈ 1.44 billion bits = 180 MB

  k = (m/n) × ln(2) = 14.4 × 0.693 ≈ 10 hash functions

180 MB fits in Redis. Store as a Redis BITSET.
Use MurmurHash3 with 10 different seeds for the k hash functions.
```

**Interaction with the dedup flow:**
```
New shorten request for long_url L:
  1. Bloom.check(L):
     → MISS (definitely new): skip DB lookup, generate new ID, insert.
     → HIT (probably exists): query DB for SELECT short_code WHERE long_url = L
        → Found: return existing short_code (true positive)
        → Not found: false positive — generate new ID, insert (rare: 0.1% of hits)
```

**Failure mode — Bloom filter not persisted:** On Redis restart, the Bloom filter is empty. All queries return MISS. For the next few minutes, every shorten request that's a re-submission of an existing URL creates a duplicate entry. Mitigation: persist the Bloom filter to disk on shutdown (`SAVE` in Redis), or rebuild from DB on startup (100M records, ~60 seconds at 1.5M reads/sec from DB).

**When NOT to use:** When false positives are unacceptable (e.g., a security system where "key exists" triggers action). For URL dedup, a false positive causes an extra DB read — tolerable.

---

### 4. Redirect Type: 301 vs. 302

**The consistency framing from Day 7 applied directly:**

| Behavior | 301 Permanent | 302 Temporary |
|---|---|---|
| Browser caches | Yes — indefinitely | No — every visit hits your service |
| Analytics (click tracking) | Broken after first visit per browser | Works on every redirect |
| URL changeability | Effectively immutable — browsers won't re-check | Can change destination any time |
| Infrastructure load | Near-zero repeat traffic | Full redirect QPS maintained |
| Consistency model | Eventual (browser has stale mapping forever) | Strong (server always authoritative) |
| CDN cacheability | Yes — CDN can cache 301 responses | No — CDN passes through to origin |

**Decision:** Use **302** by default. The 1,000:1 read:write ratio and 99% cache hit rate mean the infrastructure cost of repeat traffic is absorbed by Redis, not the DB. Analytics is a core product feature for URL shorteners — 301 breaks it after the first visit per browser. The only case for 301 is a "link permanence" guarantee product (archival, print media) where analytics are not required and CDN caching is the cost optimization lever.

**Hybrid approach for power users:** Allow link owners to set `redirect_type=permanent` on a per-link basis. Store `redirect_type` in the URL record. Serve 302 by default; serve 301 when `redirect_type=permanent` and the link has no expiration date.

---

### 5. Caching for Reads

**Architecture (from Day 2, applied at 350K peak QPS):**

The redirect path must hit Redis, not the DB. At 350K QPS and p99 < 50ms, a DB read (~5–15ms) on every request consumes the full latency budget before network overhead is even counted.

```
Cache key:   short_code (7 bytes)
Cache value: long_url (avg 200 bytes) + redirect_type (1 byte) + expires_at (8 bytes)
Cache TTL:   24 hours (long — URL mappings almost never change)

Hot key problem: a viral link gets 500K redirects in 1 hour, all routing to 1 Redis node.
Solution (Day 6 scatter pattern):
  Store 10 copies: "dQw4w9:0" through "dQw4w9:9" on 10 different nodes.
  Read: GET "dQw4w9:{random(10)}"
  Write (on URL creation): SET all 10 copies.
  Write (on URL update — rare): SET all 10 copies.
```

**Cache warming on miss:**
```
redirect(short_code):
  long_url = redis.get(short_code)  # ~0.5ms
  if long_url:
      return redirect(long_url, 302)  # total: ~1ms

  # Cache miss path (~1% of requests)
  record = db.query("SELECT long_url, redirect_type, expires_at
                     FROM urls WHERE short_code = ?", short_code)
  if not record:
      return 404  # short code doesn't exist

  if record.expires_at and record.expires_at < now():
      return 410 Gone  # link expired — don't cache this

  redis.setex(short_code, 86400, record.long_url)  # warm cache
  return redirect(record.long_url, record.redirect_type)
```

**TTL strategy:** 24-hour TTL with no active invalidation. URL mappings are effectively immutable — a short code, once created, points to the same destination forever (for 302 redirect). The only invalidation event is expiration (`expires_at`), which the application checks at read time without needing cache invalidation.

---

### 6. Sharding Strategy

**From Day 6:** Use consistent hashing on `short_code` to route to the correct DB shard. With 100M URLs at ~35 GB, 3 shards of ~12 GB each are sufficient for storage. Shard count is chosen for write throughput (350 peak writes/sec, trivially handled by even 1 shard) and future growth.

```python
# Shard routing at the application layer (or via Vitess proxy)
shard_node = ring.get_node(short_code)
db_conn = connection_pool[shard_node]
record = db_conn.query("SELECT * FROM urls WHERE short_code = ?", short_code)
```

**Shard key choice:** `short_code` (not `user_id`, not `long_url`). The dominant access pattern is redirect lookup by short code — this must be a shard-local operation. If sharded by `user_id`, a redirect lookup requires knowing which user shortened the code, which means either a scatter-gather or a secondary index table.

**Cross-shard operations:**
- "Show me all URLs shortened by user X": scatter-gather across all shards, or maintain a separate `user_url_index` table (unsharded, or sharded by `user_id`) that stores `(user_id, short_code)` pairs. At 100M URLs across ~1M users, this table is small enough to live unsharded on a single PostgreSQL instance.

---

## System Architecture Walkthrough

### Write Path (URL Shortening)

1. Client `POST /shorten {long_url: "https://example.com/very-long-path"}` → L7 LB → app server pod (stateless, Day 1).

2. **Validation:** Check URL format. Check length ≤ 2,048 chars (IE/Chrome limit). Reject non-HTTP(S) schemes.

3. **Bloom filter check:** `bloom.check(long_url)`. If HIT (possible duplicate): query `user_url_index` table for this user's existing short code for this long URL. If found, return existing code. If not (false positive), proceed to step 4.

4. **ID generation:** Call the counter service (3 nodes, pre-allocated ranges). Get next ID in O(1) from local range buffer. Apply Feistel shuffle: `shuffled_id = shuffle(raw_id)`. Encode: `short_code = base62_encode(shuffled_id)`.

5. **DB write:** Route to shard via `ring.get_node(short_code)`. `INSERT INTO urls (short_code, long_url, user_id, created_at, expires_at, redirect_type) VALUES (...)`. PostgreSQL WAL fsync before ACK (RPO=0).

6. **Index write:** `INSERT INTO user_url_index (user_id, short_code, created_at)` — on the unsharded index DB.

7. **Bloom filter update:** `bloom.add(long_url)`.

8. **Cache prime (optional):** `redis.setex(short_code, 86400, long_url)`. Amortizes the first redirect's cache miss.

9. **Kafka publish:** Emit `url.created` event to Kafka for downstream consumers (analytics, search indexing). Async — write response does not wait.

10. Return `{short_url: "https://sho.rt/dQw4w9W"}` to client. Total p99: ~80ms (DB write dominates).

### Read Path (Redirect)

1. Client `GET /dQw4w9W` → L7 LB. The LB can be configured to serve redirects directly from Redis without hitting an app server (NGINX + Redis module) — eliminates one network hop.

2. **App server path** (if not using NGINX+Redis):
   a. `redis.get("dQw4w9W")` → hit 99% of the time → `302 Location: https://example.com/...`. Total time: ~2ms.
   b. On miss: DB read via shard lookup. Populate cache. Return 302. Total: ~15–30ms.

3. **Expiration check:** If `expires_at` is stored in the cache value (it should be — include it in the serialized cache entry), check at the app layer. Expired links return `410 Gone` without a DB read.

4. **Analytics event:** Publish `{short_code, timestamp, ip, referrer, user_agent}` to Kafka `url.redirect` topic. This is fire-and-forget — the redirect response is returned before Kafka ACK. Use Kafka producer with `acks=1` for analytics (some loss acceptable; latency critical).

### Failure Handling

- **Redis node failure:** Consistent hashing (Day 6) reroutes to the next node. Cache miss rate spikes temporarily. DB absorbs the increased read load — at 1% steady-state miss rate, even a 10× spike (10% miss rate) is 35K DB reads/sec at peak, within replica capacity.
- **Counter service node failure:** Two remaining counter nodes continue serving from their pre-allocated ranges. No ID generation interruption. When the failed node recovers, it fetches the next unallocated range from the PostgreSQL sequence — no gaps, no duplicates.
- **DB shard failure:** Primary fails → Patroni promotes read replica (~15s, Day 3 pattern). Writes queue at the app layer (retry with backoff). Reads continue from the surviving replicas.
- **Bloom filter data loss (Redis restart):** Short-term: all shorten requests skip the dedup check and go directly to the DB for duplicate detection. A UNIQUE constraint on `(user_id, long_url)` in the DB acts as the hard correctness guarantee — the Bloom filter is an optimization, not the source of truth.

### Bottleneck Progression

| Scale | Bottleneck | Solution |
|---|---|---|
| 10K redirect QPS | None | Single Redis node, single DB |
| 100K redirect QPS | Redis single-node throughput (~100K ops/sec) | Redis Cluster (3 nodes, consistent hashing) |
| 1M redirect QPS | App server CPU (302 response construction) | NGINX serving redirects directly from Redis |
| 10M redirect QPS | Redis network bandwidth | CDN caching 301 responses for high-traffic links; edge redirect |

---

## Data Model

### `urls` Table (PostgreSQL, sharded by `short_code`)

| Field | Type | Notes |
|---|---|---|
| short_code | CHAR(7) PRIMARY KEY | Shard key; B-tree index (point lookup only) |
| long_url | TEXT | Avg 200 bytes; max 2,048 bytes enforced at app layer |
| user_id | UUID | FK to users (unsharded); indexed for user dashboard queries |
| redirect_type | SMALLINT | 301 or 302; default 302 |
| created_at | TIMESTAMPTZ | Indexed for analytics aggregation |
| expires_at | TIMESTAMPTZ | Nullable; NULL = never expires |
| click_count | BIGINT | Denormalized counter — updated by analytics consumer asynchronously |
| title | VARCHAR(200) | OG title of destination page (fetched async at creation) |

**Storage engine:** PostgreSQL. `short_code` is always a 7-character string — fixed width, excellent B-tree locality. Each shard holds ~33M rows (~12 GB). Access pattern is pure point lookup (`WHERE short_code = ?`) — no range scans needed on this table.

**Why not Cassandra?** The redirect read path is a point lookup by primary key — both PostgreSQL and Cassandra handle this in O(log N). The write path is 350 QPS — trivially handled by PostgreSQL. Cassandra's AP model would allow a redirect to a non-existent URL to temporarily return a cached "miss" during a partition — unacceptable. PostgreSQL's CP model ensures a `404` is always correct.

### `user_url_index` Table (PostgreSQL, unsharded)

| Field | Type | Notes |
|---|---|---|
| user_id | UUID | Partition key of composite PK |
| short_code | CHAR(7) | Sort key of composite PK |
| long_url_hash | CHAR(32) | MD5 of long_url for dedup lookup: `WHERE user_id=? AND long_url_hash=?` |
| created_at | TIMESTAMPTZ | |

**Access patterns:**
- "Has user X already shortened this URL?" → `WHERE user_id=X AND long_url_hash=md5(long_url)` — O(log N), two-column composite index.
- "Show user X's link history" → `WHERE user_id=X ORDER BY created_at DESC LIMIT 20` — paginated, cursor-based (Day 5).

At 100M URLs across ~1M users: avg 100 links/user. Table size: 100M × ~80 bytes = 8 GB — fits on a single unsharded PostgreSQL instance with room to grow.

### Redis Cache Schema

```
Key:   {short_code}           e.g. "dQw4w9W"
Value: MessagePack-encoded {long_url, redirect_type, expires_at}
TTL:   86400 seconds (24 hours)

Hot-key scatter copies:
Key:   {short_code}:{0..9}    e.g. "dQw4w9W:3"
Value: same as above
TTL:   86400 seconds

Bloom filter:
Key:   "bloom:long_urls"
Type:  Redis BITSET (180 MB for 100M URLs at 0.1% FPR)
```

### Analytics Events (Kafka → ClickHouse)

```json
{
  "event_type":  "url.redirect",
  "short_code":  "dQw4w9W",
  "timestamp":   1711234567890,
  "ip_hash":     "sha256(ip + salt)",   // privacy: never store raw IP
  "referrer":    "https://twitter.com",
  "user_agent":  "Mozilla/5.0...",
  "country":     "US",                  // resolved at edge via GeoIP
  "device_type": "mobile"               // parsed from user agent
}
```

**Kafka topic:** `url.redirect`, 32 partitions (partitioned by `short_code` for per-link ordering), RF=3, 7-day retention. Consumed by ClickHouse for analytics aggregation and by a real-time counter service that updates `urls.click_count` in batches every 30 seconds.

---

## Interview Questions with Model Answers

### Q1 (Mid) — "How do you ensure two users shortening the same long URL don't get duplicate entries in the database?"

**Model answer:** There are two distinct dedup problems: dedup within a user's own links (same user shortening the same URL twice), and global dedup across all users. For within-user dedup, I maintain a `user_url_index` table with a composite unique constraint on `(user_id, long_url_hash)` — a UNIQUE violation at insert time tells me this user already shortened this URL, and I return the existing code. For global dedup (optional product decision), I use a Bloom filter on `long_url` to quickly check if the URL has ever been shortened. A Bloom filter hit triggers a DB lookup to confirm. A miss guarantees the URL is new. The DB's UNIQUE constraint on `short_code` is the hard correctness guarantee — the Bloom filter is a latency optimization that avoids unnecessary DB reads on the 99.9% of requests that are genuinely new URLs.

**Follow-up:** "Should the same long URL always return the same short code?"

**Pitfall:** Answering definitively "yes" or "no" without noting the product trade-off. Same code = simpler dedup, lower storage, but shared analytics — user A and user B can't have separate click counts for the same destination. Different codes = per-user analytics, higher storage, more complex dedup. The right answer depends on the product requirement.

---

### Q2 (Mid) — "Why would you choose 302 over 301 for redirects, even though 301 reduces infrastructure load?"

**Model answer:** 301 caches at the browser permanently — once a user visits a short link, their browser never contacts our service again for that link. This eliminates our ability to track analytics (click counts, referrers, device types) after the first visit, to change the destination URL if the long URL becomes invalid or moves, and to enforce link expiration (an expired link still redirects via the browser's cached 301). For a URL shortener whose value proposition includes analytics and link management, 301 destroys product capabilities to save infrastructure cost. The infrastructure cost argument is mitigated by a 99% Redis cache hit rate — the repeat redirect traffic costs ~0.5ms of Redis read time, not a DB round-trip. I'd offer 301 as an explicit opt-in for users who want browser-caching behavior and don't need analytics, not as the default.

**Follow-up:** "A customer's link goes viral and generates 5M redirects per hour. At what point do you reconsider 302?"

**Pitfall:** Not recognizing that at extreme scale, the correct answer is CDN caching with short TTLs (e.g., `Cache-Control: max-age=60`) rather than switching to 301. A CDN cache with 60-second TTL gives 99.99% cache hit rate at the edge while retaining analytics and changeability.

---

### Q3 (Senior) — "Your counter service is the single source of sequential IDs. How do you make it fault-tolerant without creating gaps in the ID sequence?"

**Model answer:** I run 3 counter service nodes, each holding a pre-allocated range of IDs in memory (e.g., 10,000 IDs per range). When a node exhausts its range, it atomically fetches the next range from a PostgreSQL sequence (`SELECT nextval('url_id_seq')` is atomic and gap-free within PostgreSQL). If one counter node fails, the other two continue serving from their pre-allocated ranges — no interruption. The IDs that were in the failed node's in-memory range are lost (they were never assigned to a URL), creating gaps in the sequence. Gaps are acceptable because we're not using sequential IDs as an ordering mechanism — we're Base62-encoding them into opaque short codes. The important invariant is uniqueness, not contiguity. For zero-gap requirements (auditing), use the PostgreSQL sequence directly with a connection pool — it's atomic and the throughput at 350 QPS is trivially within PostgreSQL's sequence generation capacity (~100K/sec).

**Follow-up:** "The in-memory range approach causes ID gaps on node failure. A security audit flags this as a potential information leak — why?"

**Pitfall:** Not recognizing that even with a Feistel shuffle, an auditor who knows the shuffle key can decode any short code back to a sequential ID and detect gaps, potentially revealing information about write rate and failure events. The mitigation is to use a cryptographically secure shuffle (SipHash) and treat the shuffle key as a secret, or switch to a random ID generation strategy entirely.

---

### Q4 (Senior) — "How does your Redis caching strategy hold up when a link gets 10 million clicks in 5 minutes?"

**Model answer:** At 10M clicks in 5 minutes, that's ~33,333 redirects/sec to a single short code — all routing to the same Redis node via consistent hashing (Day 6). A single Redis node handles ~100K simple GET ops/sec, so this is within limits but leaves no headroom for other keys on the same node. I apply the hot-key scatter pattern from Day 6: at link creation, detect links marked as "campaign" or likely viral and pre-populate 10 scatter copies (`dQw4w9W:0` through `dQw4w9W:9`) across 10 different Redis nodes. Read requests hash `short_code + random(10)` to pick a scatter copy. Each copy receives ~3,300 QPS — well within per-node capacity. For links that become viral unexpectedly (not pre-tagged), I monitor per-key request rate in Redis (`redis-cli --hotkeys`) and trigger scatter copy creation dynamically when a key exceeds 10K QPS. The 1–2 second window before scatter copies are ready is absorbed by the single hot node — it's within capacity, just without headroom.

**Follow-up:** "The scatter copies need to be consistent — if the link's destination is updated, all 10 copies must be updated. How do you handle this atomically?"

**Pitfall:** Using a Redis transaction (`MULTI/EXEC`) across keys on different nodes — Redis transactions are single-node only. The correct answer is to update all 10 copies in a Lua script loop or via a pipeline, accepting that there's a ~1ms window where some copies are updated and others aren't. For a read-heavy, eventually-consistent cache layer, this is acceptable.

---

### Q5 (Staff/Principal) — "Design the link analytics system so that click counts are accurate to within 1% at any point in time, even during a traffic spike of 10M clicks/minute, without the analytics writes becoming a bottleneck on the redirect path."

**Model answer:** The redirect path is latency-critical (p99 < 50ms) and must not block on analytics writes. I decouple them completely: the redirect handler publishes a fire-and-forget event to a Kafka topic (`acks=1`, no wait for broker ACK) — this adds ~200µs to the redirect path, not 5–15ms. Kafka absorbs the 10M clicks/minute write spike (Day 4's backpressure mechanism handles this naturally — Kafka at 32 partitions handles ~500K messages/sec sustained). A ClickHouse consumer group reads from Kafka and aggregates click counts in 10-second micro-batches, writing aggregated counts to a `click_counts_hourly` table in ClickHouse (columnar, append-only — ideal for this workload). A separate real-time counter consumer updates `urls.click_count` in PostgreSQL every 30 seconds via a `UPDATE urls SET click_count = click_count + ? WHERE short_code = ?` batch. For the "accurate within 1%" requirement: the 30-second batch window at 10M clicks/minute means at most 5M un-flushed clicks (50% of a 10-minute window). As a fraction of total clicks for a viral link (100M+ over its lifetime), this is well under 1%. For a 1-hour-old link with only 100K total clicks, the 30-second lag is 0.3% — within the 1% SLA. The key insight: "accurate within 1%" is a relative measure — it tightens automatically as total click volume grows.

**Follow-up:** "ClickHouse rejects a batch write due to a schema error. 10 minutes of click events are sitting in Kafka. How do you recover?"

**Pitfall:** Not knowing that Kafka retains messages for 7 days (Day 4's retention policy). Recovery is a consumer offset reset: fix the ClickHouse schema, reset the consumer group offset to 10 minutes ago (`kafka-consumer-groups.sh --reset-offsets --to-datetime`), and let the consumer replay. Zero data loss — this is exactly why the analytics pipeline uses Kafka rather than direct DB writes.

---

## Trade-offs to Articulate

1. **"I chose counter-based IDs with Feistel shuffle over hash-based IDs because counter-based generation has zero collision risk — at 100M URLs, MD5 truncated to 7 Base62 characters produces ~1,136 expected collisions requiring retry logic. The trade-off I'm accepting is that the shuffle key becomes a security secret — if it leaks, short codes become enumerable. I mitigate by rotating the key annually and storing it in a secrets manager, never in application config."**

2. **"I chose 302 (temporary redirect) over 301 (permanent redirect) as the default because analytics tracking — click counts, referrers, device breakdown — is a core product feature, and 301 permanently breaks analytics after the first browser visit. The trade-off I'm accepting is that the full redirect QPS hits my infrastructure (Redis) on every click rather than being absorbed by browser caches. At 99% Redis cache hit rate and ~0.5ms per cache read, this costs ~58 MB/sec egress — well within capacity."**

3. **"I chose a Bloom filter for long-URL dedup over a full DB lookup on every shorten request because at 350 peak write QPS, 99.9% of submissions are unique URLs — a DB lookup on every submission is wasteful. The Bloom filter returns a definitive 'new URL' answer in <1ms with zero false negatives. The trade-off I'm accepting is 0.1% false positive rate — 0.35 unnecessary DB reads/sec at peak — and the operational requirement to persist and restore the 180MB Bloom filter on Redis restarts."**

4. **"I chose to shard by `short_code` rather than `user_id` because the dominant access pattern — redirect lookup — takes only the short code as input. Sharding by `user_id` would require either a scatter-gather across all shards or a separate lookup table on every redirect. The trade-off I'm accepting is that user-centric queries (show all of user X's links) require a separate `user_url_index` table, adding a write to a second store on every URL creation."**

5. **"I chose fire-and-forget Kafka publish (`acks=1`) for analytics events on the redirect path rather than `acks=all` because adding 3–5ms of Kafka ACK latency to a 2ms redirect path is a 150–250% latency increase — a violation of the p99 < 50ms SLA at scale. The trade-off I'm accepting is losing analytics events for clicks that occur in the ~200ms window if the Kafka broker leader fails. At 10B clicks/day, that's at most ~23K lost events during a broker failover — 0.00023% event loss, acceptable for analytics."**

6. **"I chose PostgreSQL over Cassandra for the URL mapping store because the redirect path requires CP behavior — a 404 for a non-existent code must always be correct. Cassandra's AP model could serve a cached 'record not found' during a partition even if the record was written 100ms ago, causing a 404 on a valid link. The trade-off I'm accepting is that PostgreSQL requires explicit sharding management (Vitess or application-layer routing), whereas Cassandra handles this natively."**

---

## Failure Modes and Resilience Patterns

### 1. Counter Service Range Loss on Crash
- **Symptom:** After a counter service node crash and restart, IDs `[47,001–57,000]` are permanently skipped. Short codes in that range are never assigned.
- **Root cause:** The node had pre-allocated IDs 47,001–57,000 in memory. On crash, those IDs are lost. The node restarts and fetches a new range starting at 57,001.
- **Detection:** Monitor the gap between `MAX(raw_id)` in the DB and `nextval` of the PostgreSQL sequence. Gaps > 10,000 indicate a crash occurred. Not an operational emergency — gaps don't cause incorrect behavior, only namespace inefficiency.
- **Mitigation:** Reduce pre-allocation range from 10,000 to 1,000 IDs. On graceful shutdown, flush unused IDs back to a "reclaim pool." For zero-gap requirements, use PostgreSQL sequence directly (removes the pre-allocation optimization but guarantees contiguity).

### 2. Bloom Filter False Positive Storm
- **Symptom:** After a Redis restart without persistence, the Bloom filter is empty. All queries return MISS. For the next ~10 minutes, every re-submitted long URL creates a duplicate entry in the `user_url_index` table. UNIQUE constraint violations spike.
- **Root cause:** Bloom filter in Redis lost on restart. All queries are now definitive misses — no dedup check occurs.
- **Detection:** Alert on UNIQUE constraint violation rate in PostgreSQL exceeding 1/sec (baseline is ~0).
- **Mitigation:** Redis `SAVE` on shutdown persists the Bloom filter to an RDB snapshot. On startup, the Bloom filter is loaded from the snapshot in ~5 seconds (180MB file read). For deployments where this window is unacceptable, keep the Bloom filter as a secondary Redis key with `PERSIST` (no expiry) and enable AOF persistence on the Bloom filter Redis instance.

### 3. Viral Link Cache Miss Cascade
- **Symptom:** A link is tweeted by a celebrity. 500K redirects arrive in the first 60 seconds. The link was just created — not yet in Redis cache. All 500K requests miss and hit the DB shard simultaneously. The shard's connection pool exhausts; p99 redirect latency spikes to 2 seconds.
- **Root cause:** New link has no cache entry. High-traffic spike arrives before the cache is warm. The Bloom filter warming (step 8 in the write path) populated the cache, but the celebrity tweet arrived within the ~100ms window before the cache SET completed.
- **Detection:** Per-shard DB connection pool saturation (`cl_waiting > 0` in PgBouncer, Day 3). Redirect p99 latency alert.
- **Mitigation:** Cache the new link entry synchronously *before* returning the `shorten` response (not async). This guarantees cache is populated before the short URL is ever shared. For links in marketing campaigns (known high-traffic): pre-warm all 10 scatter copies at creation time. Add a circuit breaker on the DB shard — if connection pool is saturated, serve a `503 Retry-After: 1` rather than queuing indefinitely.

### 4. Analytics Kafka Lag During Spike
- **Symptom:** Click counts displayed to users are 30 minutes stale during a viral event. The "real-time" click counter on the dashboard shows 10K but the actual count is 500K.
- **Root cause:** ClickHouse consumer group lag. The consumer can process 1M events/sec, but during the viral spike, 10M events/sec are arriving. Kafka lag grows at 9M events/sec; at 7-day retention, the system can sustain this for ~7 days before messages start expiring unprocessed.
- **Detection:** `consumer_lag_by_time > 60s` alert on the `url.redirect` topic consumer group.
- **Mitigation:** Scale ClickHouse consumer instances horizontally (add pods — Kafka rebalances, Day 4). For the dashboard display: show a "live estimate" counter that reads from a Redis G-Counter (Day 7) updated by a lightweight counter consumer with `acks=1` — this counter is approximate but near-real-time. Show the precise count from ClickHouse with a "as of 5 minutes ago" label.

### 5. Short Code Collision (Hash-Based Generation)
- **Symptom:** 0.001% of shorten requests return an error: "short code already exists." Users retry and succeed (or don't, and submit a support ticket).
- **Root cause:** MD5 hash truncated to 7 chars collides with an existing short code. At 100M URLs, expected ~1,136 collisions. The application retries with a suffix, but at 3 retries it gives up and surfaces an error.
- **Detection:** Application-level metric: `shorten_collision_retry_count`. Alert when P95 retry count exceeds 1.
- **Mitigation:** Switch to counter-based ID generation (zero collision risk). If staying hash-based: increase retry count to 10 (still fast — each retry is a hash computation, not a DB round-trip). Add a final fallback to counter-based generation if all hash retries fail.

---

## How This Connects Forward

- **Day 9 (Rate Limiter):** The shorten endpoint is the primary rate-limiting target — a single user flooding the shortener at 10K requests/second would exhaust the counter service's pre-allocated range and fill the `user_url_index` table. Day 9's token bucket and Redis Lua atomic counter apply directly to protecting the `POST /shorten` endpoint.
- **Day 10 (Notification System):** The `url.created` Kafka event (step 9 of the write path) is consumed by notification workers to alert link owners of milestone click counts (e.g., "Your link hit 1,000 clicks"). Day 10's fan-out and priority queue design applies here.
- **Day 13 (Design a CDN):** The viral link problem at 10M redirects/minute points directly toward CDN-level redirect caching. Day 13 covers edge caching, origin shield, and TTL strategy — all of which apply to the 301/302 decision revisited at CDN scale.
- **Day 14 (Distributed ID Generation):** Today's counter service with pre-allocated ranges is a simplified Snowflake. Day 14 builds the full Snowflake algorithm with timestamp + worker ID + sequence, handling clock skew, worker ID assignment, and ID ordering properties — directly extending today's ID generation section.
- **Day 16 (Design a Web Crawler):** The URL shortener's long-URL fetch (fetching OG title at creation) is a simplified single-URL crawl. Day 16's crawler architecture is the scaled-out version of this pattern.
