# ADR 0003: JdbcClient over JPA

## Status
Accepted

## Context
Need a data access strategy for high-throughput webhook processing.

## Decision
Use **JdbcTemplate** instead of JPA/Hibernate.

## Rationale
- **Faster**: No entity state tracking, no proxy objects, no dirty checking


## Consequences
- Plain POJOs (no annotations)
- Manual RowMapper implementations
- Explicit SQL for all queries