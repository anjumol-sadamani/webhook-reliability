# ADR 0004: Single consolidated database

## Status
Accepted

## Context
Could separate databases for ingestion (events, idempotency) and delivery (outbox, retries).

## Decision
Use **one PostgreSQL database** for all concerns.

## Rationale
- **Avoids dual-write**: Single transaction for event + outbox row
- **Shared polling pattern**: Outbox publisher and retry scheduler both poll → publish to Kafka
- **Dual-purpose table**: Events table serves as both idempotency store and delivery audit trail for AI ops assistant

## Consequences
- All tables in one schema
- Simpler deployment and backup
- Single connection pool to tune