# Day 9 — Design a Rate Limiter

## Day Summary

A rate limiter has one job: count how many times a client has done something in a time window, and block them if they've done it too many times. That sounds simple, but it hides several real problems. How do you count accurately when requests are spread across 50 app servers? How do you prevent a client from gaming the window boundaries to get double their quota? How do you make the limiter fast enough that it doesn't add noticeable latency to every single request in your system? And what happens when the counter store goes down — do you fail open (let everything through) or fail closed (block everything)? Day 9 is about picking the right algorithm for the right constraint, building the counter store correctly so it's both accurate and fast, and making the limiter resilient enough that it doesn't become a single point of failure for your entire API.

---

## Pre-read Checklist

- **Redis data structures and atomic operations** (Day 2) — Every algorithm here stores counters in Redis. You need to know `INCR`, `EXPIRE`, `ZADD`, `ZRANGEBYSCORE`, and why Lua scripts make multi-step operations atomic.
- **Stateless app servers** (Day 1) — Rate limiter state cannot live on individual app servers. This is the same reason session state moved to Redis in Day 1. The counter store must be external and shared.
- **L7 load balancing and API gateway** (Days 1, 5) — The rate limiter sits at the API gateway layer, before requests reach app servers. Know where in the request path it intercepts traffic.
- **At-least-once delivery and idempotency keys** (Days 4, 8) — Clients that get rate-limited will retry. Those retries must not count as new requests if they carry idempotency keys. The rate limiter and the idempotency layer need to cooperate.
- **CAP theorem — AP vs. CP** (Day 7) — A centralized Redis rate limiter is CP-leaning: if Redis is down, you can't count. A local per-server counter is AP-leaning: always available but approximate. Choosing between them is a CAP decision.

---

## The Problem, Stated Precisely

**Context:** You're adding rate limiting to the social platform's public API. Different endpoint categories have different limits. The rate limiter must work correctly across a fleet of 50 app servers.

### Functional Requirements
- Limit requests per user per time window (configurable per endpoint)
- Return `429 Too Many Requests` with a `Retry-After` header when the limit is exceeded
- Return rate limit headers on every response so clients can self-throttle
- Support multiple limit scopes: per-user, per-IP, per-API-key, global per-endpoint
- Limits are configurable without a code deploy

### Non-Functional Requirements

| Parameter | Target |
|---|---|
| Fleet size | 50 app servers |
| Request rate | 115,000 requests/sec (peak) |
| Rate limiter overhead | p99 < 5ms added latency per request |
| Accuracy | Exact for security-sensitive endpoints; ±5% acceptable for general API |
| Availability | 99.99% — limiter failure must not take down the API |
| Limit examples | `POST /shorten`: 10 req/min per user; `GET /redirect`: 1,000 req/min per IP; `POST /login`: 5 req/min per IP |

### Rate Limit Tiers

| Endpoint category | Limit | Window | Scope | Algorithm |
|---|---|---|---|---|
| Auth (`/login`, `/register`) | 5 requests | 1 minute | Per IP | Sliding window counter |
| Write API (`/shorten`, `/posts`) | 100 requests | 1 minute | Per user | Token bucket |
| Read API (`/redirect`, `/feed`) | 1,000 requests | 1 minute | Per IP | Fixed window |
| Admin API | 10 requests | 1 second | Per API key | Sliding window log |

---

## Capacity Estimation

### Counter Storage in Redis

```
Each rate limit check needs one or more Redis keys.

Fixed window counter — 1 key per user per window:
  Key: "rl:{user_id}:{endpoint}:{window_start}"
  Value: integer counter
  Size: ~60 bytes per key (key string + 8 byte integer + Redis overhead)
  Active users: 50M DAU / 86,400 sec × 60 sec window = ~34,722 active windows at once
  Storage: 34,722 × 60 bytes ≈ 2 MB — trivially small

Sliding window log — 1 sorted set per user:
  Key: "rl:log:{user_id}:{endpoint}"
  Value: sorted set of timestamps (one entry per request in the window)
  If limit is 100 req/min: up to 100 entries per set
  Size: 100 entries × 16 bytes each = 1,600 bytes per user
  At 50M active users: 50M × 1,600 bytes = 80 GB — too large for Redis in-memory
  → Only practical for low-volume, high-security endpoints (admin API, login)

Token bucket — 2 values per user:
  Key: "rl:bucket:{user_id}:{endpoint}"
  Fields: {tokens: float, last_refill: timestamp}
  Size: ~80 bytes per key
  Storage: similar to fixed window — a few MB for active users
```

### Redis Read/Write Load

```
Every API request = 1 rate limit check = 1 Redis round-trip
At 115,000 req/sec: 115,000 Redis ops/sec

Single Redis node throughput: ~100,000–200,000 simple ops/sec
→ Need at least 1 Redis node; 2 for headroom + HA

Redis latency: ~0.5ms per op in the same datacenter
With Lua script (multi-step atomic): ~1ms
Well within the 5ms overhead budget.
```

---

## Core Approaches

### 1. Fixed Window Counter

**What it does:** Divide time into fixed buckets (every 60 seconds, every minute on the clock). Count how many requests each user makes inside the current bucket. If the count exceeds the limit, reject.

**How it works in Redis:**
```python
def is_allowed_fixed_window(user_id: str, endpoint: str, limit: int, window_sec: int) -> bool:
    # Window key resets every `window_sec` seconds
    window_key = int(time.time() // window_sec)
    key = f"rl:{user_id}:{endpoint}:{window_key}"

    count = redis.incr(key)          # atomic increment; returns new value
    if count == 1:
        redis.expire(key, window_sec)  # set TTL only on first increment

    return count <= limit
```

**Why the `expire` only on count == 1:** If you call `EXPIRE` on every request, you accidentally reset the TTL on a key that's already near expiry — extending the window past its intended duration. Only set it once, on the first request that creates the key.

**The boundary burst problem — the one real weakness:**
```
Limit: 100 requests per 60-second window

t=0:59 (1 second before window resets):
  User sends 100 requests → all accepted (they're in window 1)

t=1:00 (window resets):
  User sends 100 more requests → all accepted (new window, counter at 0)

Result: 200 requests in 2 seconds. Double the intended limit.
```

**When to use it:** Read API endpoints where approximate limits are acceptable and the boundary burst is not a security issue. It's fast (1 Redis op) and memory-efficient.

**When not to use it:** Login rate limiting, payment endpoints, anything where double-limit bursts are dangerous.

---

### 2. Sliding Window Log

**What it does:** Keep a timestamped record of every request a user makes. When a new request comes in, count how many records exist in the last N seconds. If the count is at the limit, reject.

**How it works in Redis (sorted set with timestamps as scores):**
```python
def is_allowed_sliding_log(user_id: str, endpoint: str, limit: int, window_sec: int) -> bool:
    now = time.time()
    window_start = now - window_sec
    key = f"rl:log:{user_id}:{endpoint}"

    pipe = redis.pipeline()
    pipe.zremrangebyscore(key, 0, window_start)  # remove old entries
    pipe.zadd(key, {str(now): now})              # add current request
    pipe.zcard(key)                              # count entries in window
    pipe.expire(key, window_sec)
    results = pipe.execute()

    count = results[2]
    return count <= limit
```

**Why a sorted set?** Redis sorted sets are ordered by score. Using timestamp as the score lets you efficiently delete all entries older than `now - window_sec` with a single `ZREMRANGEBYSCORE` call. The count of remaining entries is the exact number of requests in the sliding window.

**The problem:** Memory. Each request in the window is stored as a separate entry. A user making 1,000 requests/minute with a 1-minute window stores 1,000 entries per key. At 50M active users this becomes 80GB — too expensive for a general-purpose limiter.

**When to use it:** Low-volume, high-security endpoints. The admin API allows only 10 requests/second — the sorted set per user has at most 10 entries at any time. The admin user base is tiny. Memory is not a concern.

**When not to use it:** Any endpoint with high limits (1,000+ req/min) or a large user base. The memory cost makes it impractical.

---

### 3. Sliding Window Counter (The Practical Choice)

**What it does:** Uses two fixed window counters — the current window and the previous window — and blends them based on how far into the current window you are. This approximates a true sliding window with O(1) memory.

**The math:**
```
Limit: 100 requests per 60-second window

current_window_count = 72    (requests so far in this window)
previous_window_count = 90   (requests in the last full window)
elapsed_fraction = 0.75      (we're 45 seconds into the 60-second window)

weighted_count = current_window_count + previous_window_count × (1 - elapsed_fraction)
               = 72 + 90 × 0.25
               = 72 + 22.5
               = 94.5

94.5 < 100 → allow the request

Why this works: the "overlap" with the previous window decreases linearly as you move
through the current window. At the start of a new window, the previous window contributes
fully. At the end, it contributes nothing. This smooths the boundary burst problem.
```

**Implementation:**
```python
def is_allowed_sliding_counter(user_id: str, endpoint: str, limit: int, window_sec: int) -> bool:
    now = time.time()
    current_window = int(now // window_sec)
    prev_window = current_window - 1
    elapsed_fraction = (now % window_sec) / window_sec

    curr_key = f"rl:{user_id}:{endpoint}:{current_window}"
    prev_key = f"rl:{user_id}:{endpoint}:{prev_window}"

    pipe = redis.pipeline()
    pipe.get(curr_key)
    pipe.get(prev_key)
    results = pipe.execute()

    curr_count = int(results[0] or 0)
    prev_count = int(results[1] or 0)

    weighted = curr_count + prev_count * (1 - elapsed_fraction)
    if weighted >= limit:
        return False

    # Atomically increment current window
    pipe = redis.pipeline()
    pipe.incr(curr_key)
    pipe.expire(curr_key, window_sec * 2)  # keep for 2 windows in case it's queried as prev
    pipe.execute()
    return True
```

**Accuracy:** The approximation error is at most (limit / window_sec) × 1 request, which is typically under 1% for reasonable limits. Cloudflare uses this exact algorithm for its distributed rate limiter.

**When to use it:** The default choice for most API endpoints. O(1) memory, near-exact accuracy, no boundary burst, 2 Redis GETs + 1 INCR per request.

---

### 4. Token Bucket

**What it does:** Each user has a bucket that holds up to `capacity` tokens. The bucket refills at `rate` tokens per second. Each request uses 1 token. If the bucket is empty, reject.

**This is the right model for "allow controlled bursting."** A user with a limit of 100 requests/minute might legitimately need to fire 20 requests at once (a bulk upload, a sync operation). Token bucket accommodates this — the bucket holds 100 tokens, the user burns 20 at once, then the bucket refills over the next 12 seconds.

**Implementation:**
```python
def is_allowed_token_bucket(user_id: str, endpoint: str,
                             capacity: int, refill_rate: float) -> bool:
    # refill_rate: tokens added per second (e.g., 100/60 ≈ 1.67 tokens/sec)
    now = time.time()
    key = f"rl:bucket:{user_id}:{endpoint}"

    # Lua script: atomic read-modify-write (no race condition)
    lua_script = """
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refill_rate = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])

    local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
    local tokens = tonumber(bucket[1]) or capacity
    local last_refill = tonumber(bucket[2]) or now

    -- Add tokens earned since last request
    local elapsed = now - last_refill
    tokens = math.min(capacity, tokens + elapsed * refill_rate)

    if tokens < 1 then
        return 0  -- rejected
    end

    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1  -- allowed
    """

    result = redis.eval(lua_script, 1, key, capacity, refill_rate, now)
    return result == 1
```

**Why Lua?** The read-compute-write sequence (read current tokens → calculate refill → write updated tokens) must be atomic. Between a Redis `GET` and `SET`, another server could read the same stale value and both could pass the limit check. A Lua script runs atomically on the Redis server — no other command executes between the read and write.

**When to use it:** Write APIs where some bursting is legitimate. `POST /shorten` with a limit of 100/min: a developer automating link creation for a marketing campaign legitimately needs to fire 20–30 at once.

**When not to use it:** Auth endpoints. You don't want a user to "save up" 5 login attempts and then fire them all at once against a single account. Fixed window or sliding window counter is better there — no burst allowance.

---

### 5. Leaky Bucket

**What it does:** Requests enter a queue. The queue drains at a fixed rate. If the queue is full, new requests are dropped.

**The key difference from token bucket:** Token bucket controls *input rate* (how many requests you accept). Leaky bucket controls *output rate* (how many requests get processed per second). Leaky bucket is for smoothing traffic to protect a downstream service, not for limiting a client.

```
Incoming requests → [QUEUE, max 100] → processed at 50/sec → downstream
If queue is full: DROP the incoming request
```

**When to use it:** Protecting a backend service that can only handle a fixed throughput — an external payment processor that accepts 50 requests/sec. You don't want bursts to overwhelm it even if each individual client is within their limit.

**When not to use it:** General API rate limiting. Clients get unpredictable behavior — their request was "accepted" but queued, introducing latency. For an API gateway, rejecting fast with a `429` is better UX than slow queuing.

---

### 6. Centralized Rate Limiting with Redis

**Why you need it:** If each of your 50 app servers tracks its own counter, a user can send 100 requests to each server and get 5,000 requests through — 50× their limit. State must be shared.

**The architecture:**

```
Client → L7 Load Balancer → App Server (any of 50)
                                   ↓
                           Rate Limiter Middleware
                                   ↓
                           Redis Cluster (shared counter store)
                                   ↓
                          Allow / Reject decision
                                   ↓
                           API Handler (if allowed)
```

**The Redis dependency problem:** Every request now requires a Redis round-trip before it can proceed. If Redis is slow (network blip), every API call is slow. If Redis is down, you have a choice:

- **Fail open:** Allow all requests through when Redis is unavailable. Risk: abuse during outages.
- **Fail closed:** Reject all requests when Redis is unavailable. Risk: your API is down whenever Redis is down.
- **Fail to local:** Fall back to per-server counters. Risk: limits become approximate (each server allows the full quota).

**The right answer for most systems:** Fail to local with an alert. During a Redis outage, rate limiting becomes approximate (50 servers × local limits), which is still better than either fully open or fully closed. Set a Redis circuit breaker — if Redis latency exceeds 3ms, skip the check for that request rather than blocking it.

**Two Redis nodes for HA:** Run Redis in a primary-replica setup (same as Day 3). If the primary fails, Sentinel promotes the replica in ~15 seconds. During those 15 seconds: fail to local.

---

### 7. Distributed / Local Rate Limiting

**What it does:** Each app server maintains its own counters in memory. No Redis involved.

**When it works:** When limits are high enough that even with 50 servers each allowing the full quota, abuse is still bounded. If the limit is 1,000 requests/minute and each server allows 1,000, a user can make 50,000 requests/minute — 50× the limit. That might be fine for a read API where the cost per request is low. It's not fine for a payment API where each request triggers a bank transaction.

**Local + gossip (approximate global):** Servers periodically share their counters with each other (gossip protocol, like Cassandra's anti-entropy). Each server knows roughly how many requests other servers have seen for each user. The count is approximate — gossip has a lag of 100ms–1s — but much more accurate than pure local.

**When to use pure local:** Soft limits on cheap read endpoints where 5–10× overage is acceptable. Never for security-sensitive limits.

---

### 8. Rate Limit Response Headers

Every response should tell the client where they stand so they can back off before getting rejected:

```http
HTTP/1.1 200 OK
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 34
X-RateLimit-Reset: 1711234620
X-RateLimit-Window: 60

HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1711234620
Retry-After: 23
Content-Type: application/json

{"error": "rate_limit_exceeded", "message": "Too many requests. Try again in 23 seconds."}
```

`Retry-After` is the number of seconds until their limit resets. A well-behaved client reads this and waits — instead of hammering your API with retries and making the problem worse.

---

## System Architecture Walkthrough

### Where the Limiter Lives

```
Client Request
     ↓
L7 Load Balancer (NGINX / AWS ALB)
     ↓
API Gateway (Kong / custom middleware)
  ↳ Rate Limiter middleware runs HERE, before any app logic
     ↓ (if allowed)
App Server Pod
     ↓
Database / Cache / Downstream Services
```

The rate limiter runs as middleware in the API gateway or as the first middleware in each app server's request pipeline. It intercepts the request, checks Redis, and either lets the request through or returns a 429 immediately — no app logic runs for rejected requests.

### Write Path (A Request Arrives)

1. Request hits the API gateway. Middleware extracts the rate limit key: `user_id` from the JWT token for authenticated endpoints; `ip_address` for unauthenticated endpoints.

2. Look up the rule for this endpoint: `{limit: 100, window: 60, algorithm: sliding_counter}`.

3. Run the Redis check (the Lua script or pipeline for the chosen algorithm). This is a single round-trip: ~0.5–1ms.

4. **If allowed:** Attach rate limit headers to the request context. Pass request to app handler. App handler attaches the same headers to the response.

5. **If rejected:** Return `429` immediately with `Retry-After` header. Log the rejection event for abuse monitoring.

### Read Path (Monitoring)

1. Every Redis counter write also emits a metric: `rate_limit.allowed{endpoint, user_tier}` and `rate_limit.rejected{endpoint, user_tier}`.

2. Alert on `rate_limit.rejected rate > 5% of total requests` for a given endpoint — signals either an attack or a legitimate client with bad retry logic.

3. Dashboard shows per-user rejection counts. Accounts rejected > 100 times/hour are flagged for abuse review.

### Failure Handling

- **Redis latency spike (> 3ms):** Circuit breaker trips. Requests fall through to the app handler without a rate limit check. Alert fires. Operator investigates within 5 minutes.
- **Redis node down:** Sentinel promotes replica (~15s). During those 15 seconds, each app server uses its in-memory fallback counter. Limits are approximate but enforced. After Redis recovers, in-memory counters are discarded and Redis counters resume.
- **App server crash mid-request:** The Redis counter was already incremented before the crash. The request never completed, but the count was charged. This is acceptable — the alternative (decrement on failure) requires a two-phase commit and is far more complex. At 5ms overhead, false-counted requests are at most 0.01% of traffic.

---

## Data Model

### Redis Key Schema

```
Fixed window counter:
  Key:   rl:fw:{scope}:{identifier}:{endpoint}:{window_epoch}
  Type:  String (integer)
  TTL:   window_duration × 2
  Ex:    rl:fw:user:user-uuid-123:shorten:28536921   → "72"

Sliding window log:
  Key:   rl:sl:{scope}:{identifier}:{endpoint}
  Type:  Sorted Set (score = timestamp, member = timestamp+random_suffix)
  TTL:   window_duration
  Ex:    rl:sl:ip:203.0.113.5:login   → {(1711234500.123, "1711234500.123-a3f"): score}

Token bucket:
  Key:   rl:tb:{scope}:{identifier}:{endpoint}
  Type:  Hash
  TTL:   3600 (1 hour; refreshed on each request)
  Ex:    rl:tb:user:user-uuid-123:shorten   → {tokens: "84.3", last_refill: "1711234567.891"}
```

### Rate Limit Rules Config (stored in Redis, updatable without deploy)

```json
{
  "POST /shorten": {
    "algorithm": "token_bucket",
    "scope": "user",
    "capacity": 100,
    "refill_rate": 1.67
  },
  "POST /login": {
    "algorithm": "sliding_counter",
    "scope": "ip",
    "limit": 5,
    "window_sec": 60
  },
  "GET /redirect": {
    "algorithm": "fixed_window",
    "scope": "ip",
    "limit": 1000,
    "window_sec": 60
  }
}
```

Stored as a Redis hash: `HGET rl:config "POST /login"`. App servers read this on startup and cache it locally with a 60-second TTL. Config changes propagate within 60 seconds without a deploy.

---

## Interview Questions with Model Answers

### Q1 (Mid) — "What's wrong with each app server tracking its own rate limit counter?"

**Model answer:** With 50 app servers each tracking their own counter, a user's requests get spread across all 50 servers by the load balancer. Each server sees only 1/50th of the user's actual traffic. A user allowed 100 requests/minute could send 100 to each server and push through 5,000 total — 50× their quota. The counter must be in a single shared store that all servers read and write. Redis is the standard answer: all 50 servers hit the same Redis instance, so the counter accurately reflects the user's total traffic across the whole fleet. The trade-off is that Redis becomes a required dependency on every API request — which is why you need Redis HA and a fallback strategy when Redis is slow or down.

**Follow-up:** "What if exact counts don't matter and you're okay with ±10% accuracy?"

**Pitfall:** Saying "just use a database" without acknowledging that at 115K QPS, a database for rate limit checks would be immediately overwhelmed. The answer is local counters with gossip synchronization, or accepting that local-only counters allow N× the limit where N is server count — and deciding whether that overage is acceptable for the specific endpoint.

---

### Q2 (Mid) — "Walk me through the boundary burst problem in fixed window counters and how sliding window fixes it."

**Model answer:** Fixed window counters reset on a schedule — say, every 60 seconds on the clock. A user allowed 100 requests/minute can send 100 in the last second of one window and 100 in the first second of the next window. Both windows see 100, so both allow them. But in that 2-second span the user sent 200 requests — double the limit. The sliding window counter fixes this by blending the current and previous window counts. When you're 45 seconds into a 60-second window, the previous window contributes 25% of its count to your running total. A user who maxed out the previous window starts the current window with 25% of their quota already "used" — they can only send 75 more before being limited. The approximation error is less than 1% for typical limits. It's not perfect, but it removes the exploitable boundary condition while keeping memory at O(1) — two counter keys per user instead of storing every timestamp.

**Follow-up:** "Is the sliding window counter's approximation ever not good enough?"

**Pitfall:** Saying the approximation is always fine. For login rate limiting (5 attempts/minute per IP), the boundary burst is a real security concern — an attacker getting 10 login attempts instead of 5 doubles their brute-force window. For auth endpoints, use the sliding window log (exact) or enforce a hard cooldown with a fixed window that's long enough that the boundary burst doesn't matter (e.g., 5 attempts per hour instead of per minute).

---

### Q3 (Senior) — "Your Redis rate limiter adds 1ms of latency to every API call. Your p99 API latency is already 45ms against a 50ms SLA. How do you fix this?"

**Model answer:** The 1ms Redis round-trip is already eating into the 5ms headroom on the SLA — any Redis latency spike pushes p99 over 50ms. I'd tackle this in layers. First, co-locate the Redis rate limiter in the same availability zone as the app servers to keep the RTT under 0.3ms. Second, pipeline the Redis call — for endpoints using fixed window counters (2 operations: INCR + EXPIRE on first request), send both in a single Redis pipeline rather than sequential calls. Third, move the rate limit check to the API gateway layer (NGINX with Redis module) rather than the app server — this eliminates a full app server hop for rejected requests and keeps accepted requests' Redis check off the critical path by doing it in parallel with connection setup. Fourth, if the endpoint is a read endpoint with a high limit (1,000 req/min), consider local counters with a Redis sync every 5 seconds — trade exact accuracy for removing the Redis dependency from the hot path entirely.

**Follow-up:** "What do you do when Redis itself becomes the bottleneck at 115K QPS?"

**Pitfall:** Proposing Redis Cluster for rate limiting without noting the complication: rate limit keys for one user might end up on different cluster nodes, requiring cross-slot operations (not allowed in Redis Cluster). The fix is key tagging: `{user_id}:endpoint:window` — using `{user_id}` as the hash tag ensures all keys for one user land on the same slot.

---

### Q4 (Senior) — "How do you rate limit at the IP level for an API that sits behind a load balancer?"

**Model answer:** The standard `REMOTE_ADDR` on an app server behind a load balancer is the LB's IP — all requests appear to come from the same address. I need the original client IP, which the L7 load balancer puts in the `X-Forwarded-For` header. The rate limiter reads `X-Forwarded-For` and takes the leftmost IP (the original client). The attack surface here is IP spoofing — a client can forge the `X-Forwarded-For` header to make it look like the request comes from a different IP. To prevent this, I configure the L7 load balancer to strip and rewrite the `X-Forwarded-For` header with the real client IP before it reaches the app — clients can't forge what the LB overwrites. For IPv6, I rate limit on the `/64` prefix (not the full address) because a single client can have millions of valid IPv6 addresses in a `/64` block and trivially rotate them to evade IP-based limits.

**Follow-up:** "A user is making legitimate requests from behind a corporate NAT — 500 employees sharing one IP. How do you avoid blocking them?"

**Pitfall:** Applying only IP-based limits. The correct design layers limits: IP-based limits are high (10,000 req/min) and act as abuse floor; user-based limits (authenticated requests) are lower (100 req/min per user). Corporate NAT users all authenticate individually, so they hit the per-user limit, not the per-IP limit.

---

### Q5 (Staff/Principal) — "Design a rate limiter that works across three geographic regions without a centralized Redis, but is still accurate to within 5%."

**Model answer:** A global centralized Redis means a cross-region round-trip on every request — 100–200ms of added latency, which is unacceptable. Instead I'd use a regional rate limiter with global coordination. Each region maintains its own Redis counter. The limit is split across regions based on traffic weight: if the US gets 60% of traffic, EU 30%, APAC 10%, the US region enforces a 60-request limit, EU 30, APAC 10 — totaling 100 globally. Regions publish their counter state to a Kafka topic (with cross-region replication via MirrorMaker) every 5 seconds. Each region receives the other regions' counts and adjusts its local remaining quota: `global_remaining = my_limit - my_count - sum(other_regions_counts)`. The 5-second sync window means a user can overshoot by at most (rate × 5s) = 5% at 100 req/min. For limits that are strictly security-sensitive (login), I'd route all auth requests globally to a single designated region's Redis, accepting the cross-region latency for those specific endpoints while keeping general API limits regional.

**Follow-up:** "Traffic weight between regions shifts — EU spikes to 70% of traffic but the EU region only has 30% of the quota. What breaks and how do you fix it?"

**Pitfall:** Not recognizing that static quota splits create starvation when traffic patterns shift. The fix is adaptive quota rebalancing: each region reports its current utilization rate every 5 seconds; a central quota manager (lightweight, not in the hot path) reallocates quotas proportionally to observed traffic. Regions use the updated quota on the next 5-second cycle.

---

## Trade-offs to Articulate

1. **"I chose the sliding window counter over the fixed window counter for auth endpoints because a fixed window lets a user double their login attempt quota by timing requests around window boundaries — in a brute-force attack, 10 attempts vs. 5 is the difference between cracking a 4-digit PIN in 1,000 tries or 500 tries. The trade-off I'm accepting is slightly higher Redis complexity: 2 GET operations and 1 INCR per request instead of 1 INCR."**

2. **"I chose the token bucket over the sliding window counter for write API endpoints because the write API has legitimate burst use cases — a developer syncing 50 links at once during a batch job. Token bucket allows a burst up to the bucket capacity while still enforcing the average rate. The trade-off I'm accepting is a more complex Redis data model (a hash with two fields and a Lua script) versus a simple counter."**

3. **"I chose centralized Redis over per-server local counters because with 50 app servers, local counters allow 50× the intended limit — for a security-sensitive endpoint like login, that's unacceptable. The trade-off I'm accepting is that Redis becomes a required dependency on every API request, adding ~1ms latency and requiring Redis HA with a failover strategy."**

4. **"I chose to fail open (fall back to local counters) when Redis is down, rather than fail closed (reject all requests), because a Redis outage that takes down the entire API is a worse outcome than temporarily approximate rate limiting. The trade-off I'm accepting is that during a Redis outage, each server enforces its own limit independently — a malicious user could get up to 50× their quota for the duration of the outage window."**

5. **"I chose to store rate limit rules in Redis (not application config files) so limits can be updated without a code deploy. Responding to an active abuse attack by editing a config file, waiting for CI/CD, and rolling out 50 servers takes 10–20 minutes. Updating a Redis key takes 1 second. The trade-off I'm accepting is that Redis rule config needs its own access control — a misconfigured rule that sets all limits to 0 takes down the API instantly."**

6. **"I chose to rate limit at the API gateway layer, before requests hit app servers, rather than inside each app server's handler. A rejected request that never reaches the app server saves the CPU cost of JWT verification, request parsing, and handler initialization — at 115K QPS with a 10% rejection rate, that's 11,500 requests/sec of compute saved. The trade-off I'm accepting is that the API gateway becomes more complex — it now carries business logic (rate limit rules) in addition to routing logic."**

---

## Failure Modes and Resilience Patterns

### 1. Redis Key Expiry Race Condition
- **Symptom:** Users occasionally get through more requests than their limit allows — specifically on the first request of a new window.
- **Root cause:** Two app servers handle the first request of a new window simultaneously. Both call `INCR` (which creates the key at value 1), both check if value == 1, both call `EXPIRE`. The INCR is atomic — only one creates the key. But the EXPIRE call from the second server can reset the TTL of an already-expired-and-recreated key, causing a window to last longer than intended.
- **Detection:** Monitor `rate_limit.allowed` count per user per window — if any window shows counts significantly above the limit, investigate.
- **Mitigation:** Use `SET key 0 NX EX window_sec` to create the key with TTL atomically if it doesn't exist, then `INCR`. The `NX` flag (set only if not exists) ensures the TTL is set exactly once, atomically.

### 2. Clock Skew Between App Servers Causing Window Misalignment
- **Symptom:** Users near the rate limit see inconsistent behavior — some requests allowed, some rejected — at window boundaries.
- **Root cause:** App servers compute `window_key = int(time.time() // window_sec)` using their local clock. If Server A's clock is 2 seconds ahead of Server B's, they compute different window keys for the same real-world moment. A request that Server A counts in window 100 gets counted by Server B in window 99 — two different Redis keys.
- **Detection:** Cross-server window key mismatch in logs. Users reporting inconsistent limits at the minute mark.
- **Mitigation:** Use NTP with sub-second synchronization (chrony instead of ntpd — achieves ~1ms accuracy). Or use Redis server time (`TIME` command) as the authoritative clock source for window computation — all servers agree on the same time regardless of local clock state.

### 3. Redis Hot Key on Popular Endpoints
- **Symptom:** One Redis node is at 90% CPU. Rate limit latency spikes to 10ms. Investigation shows a single rate limit key is receiving 50K ops/sec.
- **Root cause:** A global per-endpoint limit (not per-user) — e.g., "the entire `/redirect` endpoint allows 1M requests/min globally." All 115K requests/sec hit the same Redis key.
- **Detection:** `redis-cli --hotkeys`; per-node ops/sec dashboard shows one node at 3× cluster average.
- **Mitigation:** Split the global key into N shards: `rl:global:redirect:0` through `rl:global:redirect:9`. Each app server randomly picks a shard and increments it. To check the global count, sum all 10 shards. This is the same G-Counter pattern from Day 7 applied to rate limiting. Each shard handles ~11,500 ops/sec instead of 115,000.

### 4. Thundering Herd After Rate Limit Reset
- **Symptom:** Every 60 seconds, a spike of requests arrives simultaneously. Backend services see a sharp, periodic load spike. p99 latency spikes for 2–3 seconds every minute.
- **Root cause:** Many users hit their rate limit simultaneously and all retry at exactly the moment their window resets. The `X-RateLimit-Reset` timestamp tells all rejected clients the exact same reset time — they all retry at once.
- **Detection:** Load pattern shows sharp periodic spikes every `window_sec` seconds.
- **Mitigation:** Add jitter to the `Retry-After` value: instead of `reset_time - now`, return `reset_time - now + random(5)`. Clients retry spread over a 5-second window instead of all at once. Also: add jitter to window start times per-user by hashing `user_id` into a small offset — users don't all share the same window boundary.

### 5. Rule Config Change Causing Accidental Lockout
- **Symptom:** After updating a rate limit rule in Redis, all users are being rejected with 429 on the first request of every window.
- **Root cause:** A config change set `limit: 0` for an endpoint (typo — meant `limit: 1000`). All requests fail the check `count <= limit` since `count=1 > limit=0`.
- **Detection:** `rate_limit.rejected` rate jumps to 100% for the affected endpoint instantly.
- **Mitigation:** Validate all config changes before writing to Redis — a limit of 0 is invalid and should be rejected by the config update API. Add a config audit log. Make rule changes two-phase: write to a `rl:config:staging` key first, then promote to `rl:config` after a 30-second validation window where synthetic test requests are checked against the new config.

---

## How This Connects Forward

- **Day 10 (Notification System):** Notification delivery needs rate limiting at two levels: how many notifications a single event fan-out can generate per user per hour (don't send 500 notifications if someone likes all your posts), and how fast the notification dispatcher can call APNs/FCM (external APIs have their own rate limits). Day 10's priority queue design interacts directly with today's rate limit tiers.
- **Day 11 (Key-Value Store):** Building a KV store requires implementing exactly the operations the rate limiter relies on — atomic increment, TTL, conditional set (NX). Day 11's internals explain why `INCR` is atomic in Redis (single-threaded event loop) and how to build these primitives yourself.
- **Day 14 (Distributed ID Generation):** The Snowflake ID service from Day 8 must itself be rate-limit-aware — a runaway ID consumer could exhaust the 12-bit sequence space in a millisecond. Day 14 covers the sequence counter that is effectively a per-millisecond rate limit.
- **Day 17 (Design a Chat System):** WebSocket connections from a single user must be rate-limited — not just HTTP requests. The rate limiter needs to track message rate over a WebSocket connection, which doesn't map cleanly to per-request HTTP middleware. Day 17 covers connection-level rate limiting.
- **Day 20 (Payment System):** Payment endpoints are the highest-stakes rate limiting targets — not just for abuse prevention, but for fraud detection. A user making 50 payment attempts in 10 seconds is likely a stolen card attack. Day 20 builds on today's per-user rate limiting to add fraud pattern detection and automatic account suspension triggers.

---

## Diagrams to Draw

1. **Fixed Window Boundary Burst:** Draw a timeline with two 60-second windows side by side. Show a user sending 100 requests in the last 5 seconds of window 1, and 100 more in the first 5 seconds of window 2. Label the 10-second real span containing 200 requests. Below it, draw the same timeline with the sliding window counter — show the weighted count including the previous window's contribution, blocking the second burst.

2. **Algorithm Comparison Matrix:** Draw a 2×2 grid: axes are "Memory cost" (low/high) vs. "Accuracy" (approximate/exact). Place each algorithm: Fixed window (low memory, approximate — boundary burst), Sliding window log (high memory, exact), Sliding window counter (low memory, near-exact), Token bucket (low memory, approximate — burst-aware). Add a fifth axis annotation for "burst-friendly" on token bucket.

3. **Centralized Redis Architecture:** Draw 50 app servers, each with an arrow to a shared Redis Cluster (2 nodes). Show the request flow: Client → LB → App Server → Redis (rate limit check) → either 429 back to client, or forward to DB/cache. Add a dashed "fallback" path showing the local counter when Redis is unavailable.

4. **Token Bucket State Over Time:** Draw a horizontal timeline. Start: bucket has 100 tokens. At t=0, user fires 30 requests — bucket drops to 70. Show the bucket refilling at 1.67 tokens/sec (line sloping up). At t=20s, bucket is at ~103 tokens → capped at 100. At t=60s, user fires 100 requests — bucket empties. Show the next request rejected (bucket = 0). Show bucket refilling again.

5. **Redis Key Structure for All Three Algorithms:** Draw three Redis key layouts side by side. Fixed window: `rl:fw:user:abc:shorten:28536` → `"72"` (string). Sliding log: `rl:sl:ip:1.2.3.4:login` → sorted set `{timestamp: score, ...}` with a bracket showing the 60s window and old entries greyed out. Token bucket: `rl:tb:user:abc:shorten` → hash `{tokens: "84.3", last_refill: "1711234567"}`. Label TTL on each and annotate what operation hits each key type.
