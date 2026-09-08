# ADR 0001: Kafka Consumer Commit Strategy

## Status
Accepted

## Context
The Delivery Worker consumes an event from Kafka, then calls the tenant
endpoint. When it commits the Kafka offset relative to that call
determines the delivery guarantee:
- Commit before the call: if the worker crashes after committing but
  before calling, the event is lost — Kafka won't redeliver it.
- Commit after the call, before recording the outcome: same risk if the
  crash happens in that gap.
- Auto-commit (Kafka's default): commits on a timer, independent of
  whether the call even happened yet.

## Decision
Disable auto-commit (`enable.auto.commit=false`). Commit the offset
manually, only after the delivery outcome (success or failure) has been
durably written to Postgres.

Order per message:
1. Call the tenant endpoint.
2. Write the outcome to Postgres (delivered, or failure + next_retry_at).
3. Commit the Kafka offset.

## Consequences
If the worker crashes between step 2 and step 3, the offset was never
committed, so Kafka redelivers the message. The worker will call the
tenant endpoint again — a duplicate delivery attempt. This is accepted,
not eliminated: the platform's idempotency model (the `webhook-id` given
to tenants) is what makes duplicates safe to receive, not the commit
strategy.

The guarantee this buys: no event is ever silently dropped. The
tradeoff it accepts: an occasional duplicate call to the tenant
endpoint, in that one narrow crash window.