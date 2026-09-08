# Architecture

## Overview

webhook-reliability delivers events from a producer to a tenant's webhook
endpoint with at-least-once delivery, idempotent ingestion, exponential
backoff on retry, per-tenant rate limiting, and a durable attempt history
that an AI ops assistant can later read from to investigate failures and
recommend safe replays.

![img.png](img.png)



## Components

### Producer
External system that owns the events. 

### Ingestion API
Public HTTP Endpoint (`POST /events`). Responsible for:
- authenticating the tenant
- validating the payload
- enforcing request-level rate limiting 
- client-side retry of the POST doesn't create a duplicate
  event
- generating the platform's own event identifier (`webhook-id`, a
  UUIDv7)
- writing the event, its idempotency record, and its outbox row to
  Postgres in a single transaction


### PostgreSQL — events, idempotency, outbox, deliveries, retry schedule

**Why consolidated:** the outbox publisher and the retry scheduler are
structurally the same kind of component, both poll a table on a
schedule and publish to Kafka. And it lets this table act both as an Idempotency store and 
the delivery-attempt audit trail the AI ops assistant reads from.


### Outbox publisher
Polls the outbox table and publishes to Kafka, partitioned by tenant ID.

**Why this exists:**  the classic dual-write problem, solved by never
attempting the dual write.

### Kafka
Internal event buffer.
Topic partitioned by tenant ID, which gives per-tenant ordered delivery. 

### Delivery Worker
Consumes from Kafka. Calls the tenant endpoint over HTTPS using the
`webhook-id` header established at ingestion, unchanged across every
retry of that event.

### Tenant Endpoint
External, tenant-owned HTTP receiver. 

### Retry Scheduler
Polls Postgres for rows past their `next_retry_at`, re-publishes them to
Kafka so the delivery worker picks them up again.

## Key decisions

- Ingestion idempotency uses the event ID the source already sends.
- **`webhook-id`** (platform-generated UUIDv7) is a separate concern from
  the key above — it's what the *tenant* uses for their own idempotent
  processing of deliveries, assigned once at ingestion and reused
  unchanged across every retry. UUIDv7 is time-ordered, which lets the
  outbox publisher and retry scheduler query "oldest due" efficiently
  without a separate timestamp index.
- Outbox publisher concurrency: SELECT ... FOR UPDATE SKIP LOCKED, not a manual 
  locked_by column. Multiple publisher instances can run at once — each grabs a
  batch of unpublished rows and locks them; other instances skip those rows instead 
  of waiting, so no two instances publish the same row. If an instance crashes 
  mid-batch, Postgres releases its locks automatically and another instance picks the 
  rows back up.
- **One consolidated Postgres store** instead of separate physical
  stores for idempotency, outbox, deliveries, and retry schedule.
- **Kafka topic partitioned by tenant ID** for ordered per-tenant
  delivery.
- Add tenant-level backpressure: track a rolling failure count per tenant, and once it
  crosses a threshold, stretch that tenant's backoff further instead of retrying on 
  the normal schedule, resetting the counter on the next successful delivery.
- Add jitter, batch limits, and tenant-level backpressure to the retry scheduler: cap how many due rows it
  pulls per poll, randomize each row's retry delay slightly so a burst of failures 
  doesn't all wake up at once, and track a rolling failure count per tenant so a 
  consistently-failing endpoint gets a longer backoff instead of retrying on the 
  normal schedule, resetting the moment a delivery succeeds.


See `docs/decisions/` for the reasoning behind individual choices once
those are written up as ADRs.