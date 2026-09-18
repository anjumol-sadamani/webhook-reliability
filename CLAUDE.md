# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Start dependencies (PostgreSQL, Kafka)
docker-compose up -d

# Build
./mvnw compile

# Run application
./mvnw spring-boot:run

# Run all tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=ClassName

# Run a single test method
./mvnw test -Dtest=ClassName#methodName
```

## Architecture Overview

Webhook reliability system delivering events from producers to tenant webhook endpoints with at-least-once delivery, idempotent ingestion, and exponential backoff retry.

### Event Flow

1. **Ingestion API** receives events at `POST /api/v1/events/{sourceId}`
2. Extracts idempotency key from body (JSONPath) or header based on source config
3. Deduplicates by `(source_id, idempotency_key)` unique constraint
4. Stores event in PostgreSQL with `published_at = NULL`
5. **Outbox Publisher** polls unpublished events, publishes to Kafka (partitioned by source ID)
6. **Delivery Worker** (not yet implemented) consumes from Kafka, calls tenant endpoint
7. **Retry Scheduler** (not yet implemented) re-queues failed deliveries with backoff

### Module Structure

- `common/` - Shared entities (`Event`, `Source`, `Delivery`) and JdbcClient repositories
- `ingestion/` - REST controllers and services for event/source ingestion
- `outbox/` - `OutboxPublisher` scheduled job polling events table

### Key Patterns

- **Outbox pattern**: Events stored in DB first, separate publisher avoids dual-write problem
- **Concurrency**: `SELECT ... FOR UPDATE SKIP LOCKED` allows multiple publisher instances
- **Data access**: JdbcClient (not JPA) for performance - see ADR-0003
- **Idempotency**: Source-provided event ID (from body/header) + platform-generated UUID (`webhook-id`)

### Database Tables

- `sources` - Webhook source config (name, eventIdSource, eventIdPath, destinationUrl)
- `events` - Ingested events with idempotency key and JSONB body
- `deliveries` - Delivery attempts with retry scheduling (next_retry_at, attempt_count, status)

### Configuration

Key properties in `application.properties`:
- `outbox.poll-interval-ms` - Publisher polling interval (default: 1000ms)
- `outbox.batch-size` - Events per batch (default: 100)
- `outbox.topic` - Kafka topic name

## ADRs

See `docs/decisions/` for architecture decisions:
- **0001**: Manual Kafka offset commit after writing delivery outcome to DB
- **0002**: Exponential backoff 30s→1h, max 15 attempts
- **0003**: JdbcClient over JPA for performance
- **0004**: Single consolidated PostgreSQL database for all concerns

## Tech Stack

- Java 21, Spring Boot 4.1.1
- PostgreSQL 16 with Flyway migrations
- Apache Kafka 3.7.0
- Testcontainers for integration tests

## Git
- Only commit staged files. Show the commit message before committing.