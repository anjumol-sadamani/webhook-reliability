# ADR 0002: Retry Backoff Schedule

## Status
Accepted

## Context
architecture.md named "exponential backoff" without concrete numbers,
an attempt cap, or a maximum age before dead-lettering.

## Decision
Delay doubles per attempt, capped at 1 hour, jittered ±20% to avoid
retries synchronizing into bursts:

30s → 1m → 2m → 4m → 8m → 16m → 32m → 1h → 1h → 1h ...

Maximum 15 attempts. After the 15th failed attempt (roughly 24 hours
after first failure, given the cap), the row is marked `dead_lettered`
instead of scheduled for another retry.

Additionally, on each scheduler poll:
- **Batch limit**: pull a capped number of due rows per poll, not every
  overdue row at once, so a long outage doesn't produce a burst large
  enough to overwhelm Kafka when the tenant recovers.
- **Tenant-level backpressure**: track a rolling failure count per
  tenant. Once it crosses a threshold, stretch that tenant's backoff
  further than the standard schedule, resetting the counter on the next
  successful delivery.

## Consequences
A tenant outage produces a predictable, bounded retry window (~24
hours) rather than retrying indefinitely. Jitter and the batch limit
prevent a recovering endpoint from being hit with every accumulated
retry at once. Tenant-level backpressure means one consistently-failing
tenant doesn't consume disproportionate scheduler and Kafka capacity at
the expense of healthy tenants.