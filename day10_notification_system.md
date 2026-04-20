# Day 10 — Design a Notification System

## Day Summary

A notification system looks like a simple fan-out problem — something happens, tell some people about it. But the real challenge is that "tell some people" can mean anything from 1 person (a 2FA code) to 50 million people (a celebrity posts something), across four delivery channels (push, email, SMS, in-app), each with its own external API rate limits, failure modes, and delivery guarantees. The system also has to be a good citizen: users who get 200 notifications in a minute will uninstall your app, so throttling and preference enforcement matter as much as delivery itself. The mental model shift from prior days is that you're no longer designing around your own infrastructure's limits — you're designing around *external* services (APNs, FCM, Twilio, SendGrid) that you don't control and that will reject you if you're not careful. Everything from the priority queue design to the retry logic is shaped by the constraints of those external APIs.

---

## Pre-read Checklist

- **Kafka consumer groups, at-least-once delivery, and DLQ** (Day 4) — The notification pipeline is built entirely on Kafka topics and consumer groups. At-least-once delivery is why the dedup step exists. DLQ handling is the retry strategy when APNs or FCM rejects a message. Be fluent in these.
- **Fan-out on write vs. fan-out on read, and the celebrity problem** (Days 3, 8) — The hybrid fan-out model (push for normal users, pull for celebrities) introduced for feeds applies directly to notifications. Day 10 resolves it with priority queues layered on top.
- **Redis dedup with NX + TTL** (Day 9) — The notification dedup pattern ("has this notification already been sent?") is the same Redis `SET NX EX` pattern from Day 9's rate limiter. The difference is the key is an idempotency key per notification event, not a counter.
- **Rate limiting** (Day 9) — External APIs (APNs, FCM, Twilio) impose their own rate limits on your service. You need to rate limit your own dispatchers so you don't get throttled or banned. Day 9's token bucket applies directly to the dispatcher layer.
- **Outbox pattern and async processing** (Days 3, 4) — The notification pipeline is the outbox pattern at scale. The source-of-truth event lives in the DB (or Kafka); dispatching is async. Day 10 adds priority tiers and channel routing on top of this base pattern.

---

## The Problem, Stated Precisely

**Context:** You're building the notification system for the social platform. It handles four channels and multiple event types with very different urgency levels.

### Functional Requirements
- Send push notifications (iOS via APNs, Android via FCM), emails (SendGrid), and SMS (Twilio)
- Triggered by product events: new follower, post liked, comment, mention, security alert, 2FA code, marketing campaign
- Respect user preferences: per-channel opt-out, do-not-disturb hours, per-type opt-out
- Throttle notifications per user: no more than 10 push notifications per hour for non-critical events
- Track delivery status per notification (sent, delivered, failed, bounced)
- Support retry on transient failure; give up after 3 attempts and route to DLQ
- No duplicate notifications — if the same event triggers two pipeline runs, the user gets one notification

### Non-Functional Requirements

| Parameter | Target |
|---|---|
| Total users | 1 billion |
| DAU | 50 million |
| Notification events per day | ~500 million |
| Peak event rate | ~20,000 events/sec |
| Fan-out peak (celebrity post) | up to 50 million notifications from 1 event |
| Critical notification latency (2FA, security) | p99 < 5 seconds end-to-end |
| Normal notification latency (likes, follows) | p99 < 30 seconds |
| Promotional notification latency (marketing) | within 4 hours of trigger |
| Delivery guarantee | At-least-once; idempotency prevents duplicates |
| Availability | 99.99% for critical; 99.9% for normal/promo |

---

## Capacity Estimation

### Events and Fan-out

```
500M notification events/day ÷ 86,400 sec = ~5,800 events/sec average
Peak: ~20,000 events/sec (3.4× average)

Fan-out: each event notifies N recipients.
  Average N: 10 recipients per event (most events are direct: like, comment, follow)
  Celebrity event N: up to 50M

Total notifications dispatched/day:
  Normal events: 490M events × 10 avg recipients = 4.9B notifications
  Celebrity events: 10M events × avg 500 followers = 5B notifications
  Total: ~10B notifications/day = ~115,000 dispatches/sec average

Channel split (estimated):
  Push (APNs + FCM): 70%  → 80,500/sec
  Email:             20%  → 23,000/sec
  SMS:                3%  → 3,450/sec (expensive; only for critical)
  In-app:             7%  → 8,050/sec (stored, not dispatched to external API)
```

### Storage

```
Notification record (per dispatched notification):
  notification_id: 16 bytes (UUID)
  user_id:         16 bytes
  event_type:      20 bytes
  channel:          8 bytes
  status:           8 bytes
  payload:        200 bytes (message body, title, metadata)
  created_at:       8 bytes
  Total:          ~276 bytes per notification

10B notifications/day × 276 bytes = ~2.76 TB/day
7-day hot storage: ~19 TB
Older data: archive to S3 (cold), keep only aggregate stats in DB

Preference store:
  1B users × (8 channels × 20 event types) preferences = 160B preference bits
  Stored as a compact bitmask per user: 20 bytes per user
  1B users × 20 bytes = 20 GB — fits in Redis or a single PostgreSQL table
```

### External API Rate Limits to Design Around

```
APNs:   No hard published limit; practical ceiling ~500K/sec with connection pooling
FCM:    600 requests/sec per connection; use multiple connections; max 4M/day on free tier
Twilio: ~100 SMS/sec on standard accounts; higher tiers available
SendGrid: 100 emails/sec on Pro tier; 1,500/sec on Premier
```

---

## Core Approaches

### 1. Decoupled Notification Service

**What it does:** The notification system is a completely separate service. Other services (post service, auth service, payment service) don't know anything about how notifications work — they just publish an event to a Kafka topic and move on. The notification service is the only thing that knows about APNs, FCM, or Twilio.

**Why this matters:** Without decoupling, every service that triggers a notification needs to know: which users to notify, what their channel preferences are, how to call APNs, how to retry, how to track delivery. That logic ends up copy-pasted everywhere. When APNs changes its API, you update 20 services. With decoupling, you update one.

**The contract between upstream services and the notification service:**

```json
// Upstream service publishes this to Kafka topic: notification.events
{
  "event_id":    "uuid-v4",         // idempotency key
  "event_type":  "post.liked",
  "actor_id":    "user-who-liked",
  "target_id":   "user-who-posted",
  "entity_id":   "post-uuid",
  "entity_type": "post",
  "timestamp":   1711234567890,
  "metadata": {
    "actor_name":  "alice",
    "post_preview": "Check out this..."
  }
}
```

The upstream service knows nothing about channels, preferences, or delivery. The notification service owns all of that.

---

### 2. Priority Queues

**What it does:** Not all notifications are equal. A 2FA code that arrives 60 seconds late makes your login flow unusable. A marketing email that arrives 2 hours late is fine. Priority queues ensure critical work is always processed first, and slow promotional jobs don't delay security alerts.

**Three-tier structure:**

```
CRITICAL tier — p99 delivery < 5 seconds
  Topics: notification.critical
  Events: 2FA codes, password reset links, security alerts, payment failures
  Consumer group: critical-dispatcher (4 instances, always running)
  Processing: immediate, no batching, no throttling
  Channel: SMS and push only (fastest delivery)

NORMAL tier — p99 delivery < 30 seconds
  Topics: notification.normal
  Events: new follower, post liked, comment, mention, new message
  Consumer group: normal-dispatcher (20 instances, auto-scales)
  Processing: small batches (up to 50 per Kafka poll), slight delay acceptable

PROMOTIONAL tier — delivery within 4 hours
  Topics: notification.promotional
  Events: weekly digest, campaign emails, feature announcements
  Consumer group: promo-dispatcher (4 instances)
  Processing: large batches (500 per poll), intentionally rate-limited
  Schedule: bulk jobs run 6am–10pm local time for each user's timezone
```

**Why separate Kafka topics instead of a priority field in one topic?**
Kafka delivers messages in partition order. If a critical 2FA notification is queued behind 10,000 promotional emails in the same topic, it waits. Separate topics with separate consumer groups means critical messages are never blocked by promotional ones. The consumer groups are completely independent.

---

### 3. Event-Driven Pipeline

**The full flow from event to delivery:**

```
Step 1: Event ingestion
  Post service publishes "post.liked" → Kafka topic: notification.events
  Rate: 20,000 events/sec → 8 Kafka partitions (partitioned by target_id)

Step 2: Notification fanout worker
  Reads from notification.events
  For each event:
    - Look up target user's device tokens, email, phone
    - Check user preferences (do they want this notification type on this channel?)
    - Check throttle limit (have they gotten too many notifications this hour?)
    - For each valid (user, channel) pair: publish to tier-appropriate topic

Step 3: Channel dispatchers
  Separate consumer groups per channel per tier:
    - critical-push-dispatcher   → APNs + FCM
    - critical-sms-dispatcher    → Twilio
    - normal-push-dispatcher     → APNs + FCM
    - normal-email-dispatcher    → SendGrid
    - promo-email-dispatcher     → SendGrid (separate sending domain)

Step 4: Delivery tracking
  After each dispatch attempt: write delivery status to notifications table
  On APNs/FCM callback: update status (delivered, device_not_registered, etc.)
```

**Why partition notification.events by `target_id`?**
All events targeting user X land in the same partition, processed by the same fanout worker instance. This means one worker instance holds user X's preference cache in memory — no cross-worker coordination needed for preference lookups. The preference cache is warm because the same worker always handles user X.

---

### 4. Fan-out on Write vs. Fan-out on Read

**The problem this solves:** When a celebrity with 10M followers posts something, do you create 10M notification records immediately (fan-out on write), or do you create 1 record and let each user's notification inbox query compute it (fan-out on read)?

**Fan-out on write (push model):**

```
Celebrity posts → fanout worker creates 10M notification records
  Write amplification: 1 event → 10M writes
  Read is cheap: "show my notifications" = simple SELECT WHERE user_id = me
  Problem: 10M writes takes time; last followers don't get notified for minutes
  Bigger problem: 50M followers × 20,000 peak events/sec = 1 trillion writes/sec
```

**Fan-out on read (pull model):**

```
Celebrity posts → 1 event record created
  "Show my notifications" = JOIN: my follows × recent events from those follows
  Write: O(1), no amplification
  Read: expensive — must query across all accounts the user follows
  Problem: if you follow 1,000 accounts, each notification fetch is 1,000 lookups
```

**Hybrid model (what you actually build):**

```
if follower_count < 100,000:
    fan_out_on_write()   # most users; fast delivery; bounded write amplification
else:
    fan_out_on_read()    # celebrities; store 1 record; merge at read time

Read path for notifications inbox:
  1. Fetch precomputed notifications for this user (fan-out-on-write records)
  2. For each celebrity the user follows: fetch their last 20 events
  3. Merge and sort by timestamp, deduplicate
  4. Return top 50
```

The threshold (100,000 followers) is tunable. The merge at read time adds ~20ms for users following 10 celebrities — acceptable for an inbox view, not acceptable for real-time push dispatch. For push notifications specifically, even celebrity events are dispatched via write fan-out — just throttled and spread over 60 seconds instead of being immediate.

---

### 5. Channel Dispatchers

**Each channel has different failure characteristics. The dispatcher must know them.**

**APNs (iOS push):**
```
Connection: HTTP/2 persistent connections; maintain a pool of 10 connections
Rate limit: no hard limit, but APNs will close connections that send too fast
Key error codes:
  400 BadDeviceToken     → token is invalid; delete from device_tokens table
  410 Unregistered       → user uninstalled app; delete token
  429 TooManyRequests    → back off exponentially; don't retry immediately
  500 InternalServerError → retry after 1 second (transient)

Never retry 400/410 — the device token is permanently invalid.
Always retry 500 — transient server error on Apple's side.
Retry 429 with exponential backoff starting at 5 seconds.
```

**FCM (Android push):**
```
API: HTTP/1.1 or HTTP/2 to fcm.googleapis.com
Batch: FCM accepts up to 500 tokens per request (use this — reduces API calls by 500×)
Key error codes:
  UNREGISTERED           → same as APNs 410; delete token
  INVALID_ARGUMENT       → malformed request; don't retry
  QUOTA_EXCEEDED         → slow down; token bucket rate limiter (Day 9) at dispatcher
  UNAVAILABLE            → retry with backoff
```

**Twilio (SMS):**
```
Most expensive channel: $0.0075 per SMS
Only use for: 2FA codes, password reset, critical security alerts
NEVER use for: marketing, social notifications
Rate limit: 100 messages/sec on standard accounts
Phone number pool: use multiple "from" numbers to distribute rate limits
Key consideration: international SMS rates vary wildly; geo-route to cheapest carrier
```

**SendGrid (Email):**
```
Transactional email (2FA, receipts): dedicated IP, high reputation, max 100/sec
Promotional email (campaigns): shared or dedicated IP, can warm up to 1,500/sec
Separate sending domains: use notifications@yourapp.com for transactional,
  news@yourapp.com for promotional — keeps IP reputation isolated
Bounce handling: SendGrid sends bounce webhooks; remove bounced addresses immediately
  Hard bounce (invalid address): remove permanently
  Soft bounce (inbox full): retry up to 3 times over 24 hours
```

---

### 6. Delivery Tracking and Retries

**The basic retry loop:**

```
Attempt 1: dispatch notification
  → Success: mark status = SENT, record sent_at timestamp
  → Failure (transient): schedule retry in 30 seconds
  → Failure (permanent, e.g. invalid token): mark status = FAILED_PERMANENT, stop

Attempt 2 (after 30s): dispatch again
  → Success: mark status = SENT
  → Failure: schedule retry in 5 minutes (exponential backoff)

Attempt 3 (after 5 min): dispatch again
  → Success: mark status = SENT
  → Failure: mark status = FAILED, publish to DLQ

DLQ consumer: logs the failure, alerts on-call if failure rate > 0.1%
```

**APNs and FCM have delivery callbacks** — they tell you if the push was actually delivered to the device (vs. just accepted by their server). The notification record should track both:
- `sent_at`: when you dispatched to APNs/FCM
- `delivered_at`: when APNs/FCM confirmed the device received it (from webhook callback)
- `opened_at`: when the user tapped the notification (from app SDK event)

This delivery funnel (sent → delivered → opened) is how you measure notification effectiveness and identify channel health problems.

---

### 7. User Preferences and Throttling

**Preference check — happens in the fanout worker, not the dispatcher:**

```python
def should_notify(user_id: str, event_type: str, channel: str) -> bool:
    prefs = get_user_prefs(user_id)  # cached in Redis, TTL=300s

    # Hard opt-outs
    if prefs.channel_disabled(channel):         return False
    if prefs.event_type_disabled(event_type):   return False
    if is_do_not_disturb(user_id):              return False  # check local time

    # Throttle check (Day 9 token bucket, per-user per-channel)
    if channel == 'push':
        if not rate_limiter.check(f"notif:{user_id}:push", limit=10, window=3600):
            return False  # user already got 10 push notifications this hour

    return True
```

**Do-not-disturb:** Store each user's timezone and DND hours (e.g., 10pm–8am local time). The fanout worker converts DND hours to UTC for comparison. For DND-blocked notifications: buffer them and deliver at 8am local time (store in a `scheduled_notifications` table with a `deliver_after` timestamp).

**Exception for critical tier:** 2FA codes and security alerts bypass all throttle checks and DND rules. A user who disabled all notifications should still get a password reset SMS. The `CRITICAL` tier skips the preference check entirely.

---

### 8. Deduplication

**The problem:** The fanout worker uses at-least-once delivery (Day 4). If the Kafka consumer crashes after publishing to the channel topic but before committing the offset, it will re-process the same event and potentially send a duplicate notification.

**The fix — Redis dedup key per notification:**

```python
def dispatch_with_dedup(notification_id: str, channel: str, payload: dict) -> bool:
    dedup_key = f"notif:sent:{notification_id}:{channel}"

    # SET key 1 NX EX 86400 — only sets if key doesn't exist
    is_new = redis.set(dedup_key, 1, nx=True, ex=86400)

    if not is_new:
        # Already sent. ACK the Kafka message without dispatching.
        return True  # success — idempotent skip

    # First time seeing this notification_id + channel combo
    success = dispatch_to_channel(channel, payload)
    if not success:
        # Delete the dedup key so retry is allowed
        redis.delete(dedup_key)
    return success
```

**Why 24-hour TTL on the dedup key?** Kafka retries happen within seconds to minutes. A 24-hour window is far beyond any realistic retry window, so duplicates are caught. After 24 hours the key expires and Redis memory is reclaimed.

**Why delete the dedup key on failure?** If dispatch failed (transient APNs error), you need the retry to actually retry — not skip it because the dedup key exists. Only keep the dedup key if dispatch succeeded.

---

## System Architecture Walkthrough

### Write Path (Event → Notification Dispatched)

1. **Event ingestion:** The post service publishes `post.liked` to Kafka topic `notification.events` (8 partitions, RF=3). The event contains who liked what — no notification logic, just the raw fact.

2. **Fanout worker** (12 instances, one per partition plus headroom):
   - Reads event from Kafka.
   - Looks up the target user's preferences from Redis (`user:prefs:{user_id}`, TTL=300s). On miss: read from PostgreSQL preferences table, populate Redis.
   - Looks up the target user's device tokens, email, phone from the `user_devices` table.
   - For each valid (user, channel) pair where `should_notify()` returns true: determines priority tier (this event is NORMAL), publishes a notification record to `notification.normal` Kafka topic.
   - Commits Kafka offset only after all channel-topic publishes succeed.

3. **Channel dispatcher** (e.g., normal-push-dispatcher, 8 instances):
   - Reads from `notification.normal`. Batch size: 50 notifications per poll.
   - Runs dedup check for each notification (Redis SET NX).
   - Groups by channel: sends iOS tokens to APNs, Android tokens to FCM.
   - FCM batches up to 500 tokens per HTTP request — 50 notifications → 1 FCM call.
   - On success: writes `status=SENT, sent_at=now()` to `notifications` table.
   - On failure: increments retry counter. If retry_count < 3: re-publishes to `notification.normal` with a delay header. If retry_count == 3: publishes to `notification.dlq`.
   - Commits Kafka offset after all dispatches and DB writes complete.

4. **APNs/FCM delivery callback:** APNs sends HTTP/2 push responses per-message. FCM sends batch responses. The dispatcher handles these inline (not via webhook) — if a token is `UNREGISTERED`, delete it from `user_devices` immediately.

5. **Delivery callback (optional, for email/SMS):** SendGrid sends webhook POSTs to `/webhooks/sendgrid` on delivery events (delivered, bounced, spam-reported). Twilio sends delivery webhooks similarly. A lightweight webhook handler writes delivery status updates to the `notifications` table.

### Read Path (User Checks Their Notification Inbox)

1. Client sends `GET /notifications` → app server.
2. App server queries `notifications` table: `SELECT * FROM notifications WHERE user_id = ? AND status != 'FAILED_PERMANENT' ORDER BY created_at DESC LIMIT 50`.
3. For users following celebrities (fan-out on read): merge in recent events from celebrity accounts. Deduplicate by `event_id`.
4. Return notification list with read/unread status.
5. Mark returned notifications as `read` in a background async write — don't block the response on this.

### Failure Handling

- **Fanout worker crash:** Kafka offset not committed → Kafka redelivers the event to another worker instance. The worker re-runs the fanout. Dedup keys in Redis prevent double-dispatch to external APIs.
- **APNs temporarily down:** Dispatcher catches the connection error. Retries with exponential backoff (30s, 5min). After 3 retries: DLQ. Alert fires if DLQ rate exceeds 0.1% of total dispatches.
- **Redis (preference cache) down:** Fanout worker falls back to direct PostgreSQL read for preferences. Latency increases from ~1ms to ~15ms per event. Throughput decreases but system continues. Alert fires immediately.
- **Promotional campaign fan-out:** A marketing team triggers a campaign to 50M users. This generates 50M email notifications. The promo-dispatcher processes at its rate-limited pace (~500 emails/sec on SendGrid Premier → ~28 hours for 50M). This is by design — promotional sends are scheduled and batched, never blocking critical or normal queues.

---

## Data Model

### `notifications` Table (PostgreSQL, sharded by `user_id`)

| Field | Type | Notes |
|---|---|---|
| notification_id | UUID (PK) | Idempotency key; generated by fanout worker |
| user_id | UUID | Shard key; index for inbox queries |
| event_id | UUID | FK to the source event; used for dedup at fanout layer |
| event_type | VARCHAR(50) | e.g. `post.liked`, `new_follower`, `2fa_code` |
| channel | VARCHAR(20) | `push_ios`, `push_android`, `email`, `sms`, `in_app` |
| priority | VARCHAR(20) | `critical`, `normal`, `promotional` |
| status | VARCHAR(30) | `pending`, `sent`, `delivered`, `failed`, `failed_permanent` |
| retry_count | SMALLINT | Default 0; max 3 |
| payload | JSONB | Message body, title, deep-link URL, metadata |
| created_at | TIMESTAMPTZ | When the notification was created by the fanout worker |
| sent_at | TIMESTAMPTZ | Nullable; when it was dispatched to APNs/FCM/etc. |
| delivered_at | TIMESTAMPTZ | Nullable; confirmed device delivery (from callback) |
| opened_at | TIMESTAMPTZ | Nullable; user tapped the notification |

**Access patterns:**
- Inbox query: `WHERE user_id = ? ORDER BY created_at DESC LIMIT 50` — index on `(user_id, created_at DESC)`.
- Status update: `WHERE notification_id = ?` — PK lookup.
- Analytics: aggregate by `(event_type, channel, status)` — offline, done in ClickHouse not PostgreSQL.

**Retention:** Keep 90 days of notifications in the hot table. Archive older records to S3. Users rarely look at notifications older than a week.

### `user_devices` Table (PostgreSQL, unsharded)

| Field | Type | Notes |
|---|---|---|
| device_id | UUID (PK) | |
| user_id | UUID | Index; join target for fanout worker |
| platform | VARCHAR(10) | `ios`, `android`, `web` |
| push_token | TEXT | APNs token or FCM registration token |
| token_valid | BOOLEAN | Set to false when APNs/FCM returns UNREGISTERED |
| app_version | VARCHAR(20) | For version-gated notifications |
| last_seen_at | TIMESTAMPTZ | For filtering inactive devices (> 90 days = skip push) |

**Why not shard this table?** It's small. 1B users × 1.5 devices avg = 1.5B rows × ~150 bytes = ~225 GB. Fits on a single large PostgreSQL instance with read replicas. The fanout worker reads it by `user_id` — a standard index lookup.

### `user_preferences` Table (PostgreSQL, unsharded + Redis cache)

| Field | Type | Notes |
|---|---|---|
| user_id | UUID (PK) | |
| push_enabled | BOOLEAN | Global push opt-in |
| email_enabled | BOOLEAN | |
| sms_enabled | BOOLEAN | |
| dnd_start_hour | SMALLINT | 0–23, in user's local time |
| dnd_end_hour | SMALLINT | |
| timezone | VARCHAR(50) | e.g. `America/New_York` |
| disabled_event_types | TEXT[] | e.g. `["post.liked", "new_follower"]` |

**Redis cache:** `HGETALL user:prefs:{user_id}` → all preference fields. TTL=300s. On preference update: write to PostgreSQL, then `DEL user:prefs:{user_id}` to invalidate. Next fanout worker request re-populates from PostgreSQL.

### Kafka Topic Layout

| Topic | Partitions | RF | Retention | Consumers |
|---|---|---|---|---|
| `notification.events` | 8 | 3 | 7 days | fanout-workers |
| `notification.critical` | 4 | 3 | 3 days | critical-push-dispatcher, critical-sms-dispatcher |
| `notification.normal` | 16 | 3 | 7 days | normal-push-dispatcher, normal-email-dispatcher |
| `notification.promotional` | 4 | 3 | 14 days | promo-email-dispatcher |
| `notification.dlq` | 4 | 3 | 30 days | dlq-monitor (alerts only) |

---

## Interview Questions with Model Answers

### Q1 (Mid) — "Why does the notification service need to be a separate service rather than logic inside each product service?"

**Model answer:** Three reasons. First, notification logic is cross-cutting — the same preference checks, channel routing, retry logic, and delivery tracking would need to exist in every service that triggers notifications (post service, auth service, payment service). That's massive duplication. Second, external API credentials for APNs, FCM, and Twilio are sensitive and should be held by one service, not scattered across 20. Third, scaling profiles are different — the post service scales with write QPS; the notification service scales with fan-out, which can be 1,000× larger than the triggering write. You want to scale them independently. The Kafka topic between them is the contract — upstream services publish raw events, the notification service owns everything else.

**Follow-up:** "What's the downside of this decoupling?"

**Pitfall:** Saying there's no downside. The real costs are: end-to-end latency increases (Kafka adds at least 100–500ms), debugging cross-service issues requires distributed tracing, and the notification service becomes a dependency that the whole platform relies on — its SLA must be very high.

---

### Q2 (Mid) — "A user gets 200 push notifications in 10 minutes because someone is spamming likes on all their posts. How do you handle this?"

**Model answer:** This is a throttling problem, handled in the fanout worker before dispatch — not at the dispatcher layer. I keep a per-user per-channel token bucket in Redis (the same pattern from Day 9): each user gets 10 push notification tokens per hour that refill at 1/360 tokens per second. When the like spam hits, the first 10 notifications go through, then the bucket is empty and the fanout worker skips generating push notifications for subsequent events. The notifications still get recorded in the `notifications` table (so the user sees them in the in-app inbox), but no push is sent. A digest notification — "You got 47 new likes while you were away" — is sent when the bucket refills. The key design decision is where throttling happens: in the fanout worker, not the dispatcher. By the time a notification reaches the dispatcher, it should already be one the user actually wants.

**Follow-up:** "Critical notifications (2FA, security alerts) must bypass throttling. How do you enforce that in code without special-casing every critical event type?"

**Pitfall:** Adding an `if event_type == '2fa'` check — this becomes a maintenance nightmare. The clean answer is priority tier: CRITICAL-tier notifications skip the throttle check entirely by routing through a separate code path that doesn't call the rate limiter.

---

### Q3 (Senior) — "A marketing team wants to send a push notification to all 50 million active users at 9am local time in each timezone. How do you design this?"

**Model answer:** Sending to 50M users "at 9am local time" means the sends are spread over 24 hours as timezones roll around — that's actually good, it prevents a single spike. I'd design it in three stages. First, a campaign job scheduler takes the campaign config (message, target segment, send time) and creates a work queue partitioned by timezone: at 8:55am UTC-5, enqueue all users in UTC-5. Second, the promo-email-dispatcher reads from this queue and dispatches through SendGrid at its rate limit (1,500/sec = 90K/min = 5.4M/hour). For 50M users, a global blast completes in ~9 hours of dispatch time, but since it's spread across timezones, the peak dispatch rate stays manageable. Third, dedup keys (Redis SET NX with 48h TTL) prevent double-send if the campaign job retries. The critical operational guard: campaign sends go through the PROMOTIONAL Kafka topic, which the promo-dispatcher reads independently. A 50M campaign never touches the CRITICAL or NORMAL dispatchers — the priority queue isolation from approach 2 protects real-time notifications from being delayed by marketing sends.

**Follow-up:** "SendGrid bounces 2% of your 50M emails. What do you do with those bounced addresses?"

**Pitfall:** Retrying bounced addresses. Hard bounces (invalid email) should be marked `email_valid = false` in `user_devices` immediately — retrying them damages your sender reputation. Soft bounces (inbox full) can be retried once after 24 hours.

---

### Q4 (Senior) — "APNs returns a `410 Unregistered` error for 5 million device tokens after an iOS app update. How does this play out in your system and what are the risks?"

**Model answer:** `410 Unregistered` means the device token is permanently invalid — the user uninstalled the app or reset their phone. The dispatcher receives this inline in the HTTP/2 response from APNs (not a webhook — APNs responds per-notification). For each `410` response, the dispatcher immediately marks the token as invalid: `UPDATE user_devices SET token_valid = false WHERE push_token = ?`. Future fanout workers skip devices where `token_valid = false`. The risk is volume: if 5M tokens go invalid simultaneously (e.g., right after an app update that forces re-registration), the dispatcher has to process 5M individual DB writes in a short window. I'd batch these: collect all invalid tokens during a dispatch batch, then run a single `UPDATE user_devices SET token_valid = false WHERE push_token = ANY(?)` with all invalid tokens at once — 500 tokens per SQL call instead of 500 individual calls, 1,000× fewer DB round-trips. The second risk is if the fanout worker's device token cache (Redis) is stale — it might keep trying to send to invalid tokens for up to the cache TTL (say 5 minutes). Acceptable, since each failed attempt is caught and the token invalidated.

**Follow-up:** "New device tokens are issued when users reinstall the app. How do you get the new token into your system?"

**Pitfall:** Not having a clear registration flow. The answer: the app SDK sends the new token to your service on every app launch (`POST /devices {push_token: "new_token", platform: "ios"}`). This upserts the device record — same user_id, new token. Old tokens are cleaned up when APNs returns 410 for them.

---

### Q5 (Staff/Principal) — "Your notification system works at 50M DAU. Design the changes needed to scale to 1B DAU with 10× the event volume, while keeping critical notification p99 latency under 5 seconds."

**Model answer:** The bottleneck analysis at 1B DAU: fanout workers need to process 200K events/sec (10× current 20K), and the dispatcher fleet needs to handle ~1.15M push dispatches/sec. Three architectural changes. First, fanout worker scaling: increase `notification.events` to 64 partitions (up from 8) and run 64 fanout worker instances. The preference cache in Redis must be tiered — users who haven't been active in 30 days move to a cold-read path (PostgreSQL directly, not Redis), reducing Redis memory from 50M active users to 10M truly active users. Second, dispatcher sharding by region: run separate dispatcher fleets per geographic region (US, EU, APAC). Each region dispatches to APNs/FCM using region-local HTTP/2 connection pools — this keeps APNs round-trips at ~20ms (same-region) rather than 150ms (cross-region). Regional dispatchers read from regional Kafka clusters (MirrorMaker replication from the central cluster). Third, the critical path isolation must be reinforced: the CRITICAL dispatcher gets dedicated Kafka brokers, not just dedicated topics on shared brokers. At 10× volume, noisy-neighbor effects on shared brokers could add 50–100ms of latency to the critical queue — which could push p99 critical delivery past the 5-second SLA. Dedicated brokers eliminate this risk at the cost of ~$500/month additional infrastructure.

**Follow-up:** "At 1B users, the `user_devices` table is 225 GB. The fanout worker does a `WHERE user_id = ?` lookup for every notification event. At 200K events/sec, that's 200K index lookups/sec. How do you make this not fall over?"

**Pitfall:** Just "adding read replicas." At 200K QPS, even 10 read replicas at 20K QPS each are at their ceiling. The correct answer: move `user_devices` to a write-through Redis cache keyed by `user_id` with the device token list as the value. Cache hit rate at 50M active users out of 1B total: the hot user set is small — 95% of events target users who were active in the last 7 days, who are already warm in Redis. Cold cache miss goes to PostgreSQL. At 95% hit rate, PostgreSQL only handles 10K lookups/sec — well within replica capacity.

---

## Trade-offs to Articulate

1. **"I chose separate Kafka topics per priority tier over a single topic with a priority field because Kafka delivers messages in partition order — a backlog of promotional messages in a shared topic would delay critical 2FA notifications sitting behind them. Separate topics with separate consumer groups mean critical messages are never blocked. The trade-off I'm accepting is more Kafka topics to manage and more consumer group configurations to maintain."**

2. **"I chose to do preference and throttle checks in the fanout worker (before Kafka) rather than in the dispatcher (after Kafka) because filtering early means fewer messages written to downstream topics. At 10B notifications/day, if 30% are filtered by preferences, catching them at the fanout saves 3B writes to channel-specific Kafka topics and 3B dispatcher invocations. The trade-off is that the fanout worker is more complex — it must know about preferences, throttling, and priority tiers."**

3. **"I chose fan-out-on-write for users with fewer than 100K followers and fan-out-on-read for celebrities because write amplification from celebrity posts is unbounded — a single post from a 50M-follower account would create 50M notification records. The trade-off I'm accepting is complexity at read time: the notification inbox must merge two data sources (precomputed records and on-the-fly celebrity queries) and deduplicate them, adding ~20ms to inbox load time."**

4. **"I chose to delete the Redis dedup key on dispatch failure rather than keeping it, because a dedup key that persists through a failure would prevent the retry from ever dispatching — the retry would see the key, treat it as already sent, and skip the actual API call. The trade-off I'm accepting is a narrow race condition: if the dispatch succeeds but the process crashes before the DB status write, the dedup key is gone and a retry could double-dispatch. I bound this risk by writing dispatch status to the notifications table before declaring success."**

5. **"I chose separate sending domains for transactional vs. promotional email because IP and domain reputation are shared within a domain — if promotional emails generate high spam rates, the domain's reputation drops and transactional emails (2FA, receipts) start landing in spam. Separation keeps a bad promotional campaign from breaking login flows. The trade-off is managing two sets of DNS records, DKIM keys, and SendGrid sender configurations."**

6. **"I chose APNs HTTP/2 persistent connections over per-request connections because each APNs connection setup takes ~200ms (TLS handshake). At 80K push dispatches/sec, per-request connections would add 200ms to every notification and overwhelm APNs with connection overhead. A pool of 10 persistent HTTP/2 connections multiplexes thousands of concurrent requests. The trade-off is connection lifecycle management — detecting stale connections, reconnecting after server-side closes, and balancing load across the pool."**

---

## Failure Modes and Resilience Patterns

### 1. APNs Connection Pool Exhaustion
- **Symptom:** Push notification dispatch latency climbs from 20ms to 5 seconds. iOS push delivery rate drops to near zero. No errors from APNs — just queuing.
- **Root cause:** All 10 APNs connections are occupied with in-flight requests that are waiting for a slow APNs response. New dispatch requests queue at the connection pool level. A spike in traffic (a celebrity posts) created more concurrent APNs requests than the pool can handle.
- **Detection:** Connection pool queue depth > 0 for more than 5 seconds; push dispatch p99 latency alert.
- **Mitigation:** Scale connection pool from 10 to 50 connections during spikes (APNs supports many concurrent connections). Implement a timeout on APNs requests (3 seconds): if no response in 3s, close the connection and retry on a new one. The root fix: use the FCM batch API (500 tokens/request) to reduce the number of concurrent APNs/FCM connections needed by 500×.

### 2. Preference Cache Stampede After Redis Restart
- **Symptom:** After Redis restarts, fanout worker throughput drops from 20K events/sec to ~200 events/sec. PostgreSQL CPU spikes to 100%. Notification delivery lags by minutes.
- **Root cause:** All preference cache entries expire simultaneously (Redis restarted cold). Every fanout worker event triggers a PostgreSQL preference read. At 20K events/sec with a 15ms DB read, the PostgreSQL connection pool exhausts.
- **Detection:** Redis `keyspace_hits / (keyspace_hits + keyspace_misses)` drops to near 0; PostgreSQL connection pool saturation alert.
- **Mitigation:** Use Redis persistence (AOF + RDB snapshot) so the cache survives restarts. On planned restarts, pre-warm the cache from PostgreSQL before taking traffic. Add a circuit breaker: if PostgreSQL is saturated, serve notifications without preference checks for 60 seconds rather than dropping all events.

### 3. Promotional Campaign Blocking Normal Queue
- **Symptom:** Normal-tier notifications (likes, comments) are delayed by 5–10 minutes during a large promotional send.
- **Root cause:** A marketing team triggered a 50M campaign without going through the promotional workflow — they published directly to `notification.normal`. The 50M records flood the normal consumer group.
- **Detection:** `notification.normal` consumer lag spikes from 0 to 50M in seconds; normal notification delivery p99 alert.
- **Mitigation:** The marketing campaign API must route to `notification.promotional` — enforce this at the API layer, not by convention. Restrict which services can publish to `notification.normal` using Kafka ACLs: only the fanout worker service account has produce permissions on normal and critical topics. Marketing campaign tooling only has produce permissions on promotional topics.

### 4. FCM Token Churn After App Update
- **Symptom:** Android push delivery rate drops from 95% to 40% after an app update. FCM returns `UNREGISTERED` for 60% of dispatches.
- **Root cause:** The new app version invalidates all old FCM registration tokens. Users must open the app to receive a new token. Until they do, all push attempts fail.
- **Detection:** `UNREGISTERED` FCM error rate > 5% of dispatches; push delivery rate alert.
- **Mitigation:** Track `app_version` in `user_devices`. After an app update, the fanout worker can skip push dispatch for devices on the old version (where token churn is expected) and instead route to in-app notification only — the in-app notification is visible when the user next opens the updated app. This reduces FCM failure noise and saves external API calls.

### 5. DLQ Silent Growth
- **Symptom:** 0.05% of notifications silently never arrive. Users report missing critical security alerts. Investigation finds millions of records in the DLQ with no one monitoring it.
- **Root cause:** A transient SendGrid API change broke the email dispatcher's authentication header format. All email notifications failed 3 times and moved to DLQ. The DLQ was not monitored.
- **Detection:** `notification.dlq` message count > 0 must be a P1 alert, not a P3. Add a DLQ consumer that emits a metric for every message received — alert when DLQ ingest rate exceeds 0 for more than 60 seconds.
- **Mitigation:** Treat DLQ entries as incidents. Build a replay tool that re-publishes DLQ messages to their original topic after the root cause is fixed. For critical notifications specifically: if retry count = 3 and channel = `sms` or `push`, escalate immediately to on-call rather than just writing to DLQ.

---

## How This Connects Forward

- **Day 11 (Key-Value Store):** The preference cache design — reading from PostgreSQL on miss, writing to Redis, invalidating on update — is the canonical write-through cache pattern. Day 11 builds the storage engine that powers both the preference store and the dedup key store from scratch.
- **Day 13 (Design a CDN):** Notification payloads often contain image URLs (profile pictures, post thumbnails). Delivering those images is a CDN problem — the same image is fetched by potentially millions of notification tap-throughs simultaneously. Day 13's cache hit ratio and origin shield design apply directly.
- **Day 15 (Design a Search System):** The `notifications` table at 90-day retention across 1B users is petabytes of data. Searching it ("show all notifications I got in March about post X") requires a search index, not a SQL `LIKE` query. Day 15's Elasticsearch indexing applies here.
- **Day 17 (Design a Chat System):** Chat messages trigger notifications, but they have a tighter latency requirement (sub-second for message delivery). Day 17 covers the WebSocket push path that replaces or augments the APNs/FCM path for users who are actively online.
- **Day 20 (Payment System):** Payment events (charge succeeded, charge failed, refund issued) are the highest-priority notification triggers in an e-commerce system. Day 20's event model maps directly onto today's CRITICAL tier — with the additional requirement that financial notification delivery must be tracked for regulatory compliance, not just operational health.
