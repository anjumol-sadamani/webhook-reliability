# ADR 0005: Kafka Error Handling Strategy

## Status
Accepted

## Context
The Delivery Worker consumes messages from Kafka and processes them by
calling tenant endpoints and writing outcomes to Postgres. When processing
fails, we must decide how to handle the error without violating ADR-0001's
guarantee that "no event is ever silently dropped."

Spring Kafka's default error handler retries failed records a few times,
then "recovers" by logging the error and committing the offset. This means
after retries are exhausted, the message is lost without any record.

Errors fall into two categories:
- **Transient errors**: Infrastructure failures like DB unavailable,
  connection timeouts, lock contention. These will resolve on their own.
- **Non-transient errors**: Bad data, logic errors, configuration problems.
  Retrying won't help; the message needs human investigation.

## Decision
Configure a custom `DefaultErrorHandler` with exponential backoff that
differentiates between transient and non-transient errors:

### Transient Errors (retry indefinitely)
- `TransientDataAccessException`, `RecoverableDataAccessException`
- `ConnectException`, `SocketTimeoutException`
- Any `DataAccessException` with connection/timeout in the message

When retries are exhausted for a transient error, the recoverer throws
an exception, causing Kafka to seek back and restart the retry cycle.
This continues until the infrastructure recovers.

### Non-Transient Errors (dead-letter after retries)
- `JsonProcessingException` (malformed message)
- `IllegalArgumentException` (bad data)
- Any other exception not classified as transient

After 10 retry attempts with exponential backoff (1s → 60s), the message
is written to the `dead_letters` table and the offset is committed.

### Backoff Parameters
- Initial interval: 1 second
- Multiplier: 2.0
- Maximum interval: 60 seconds
- Maximum attempts: 10 (for non-transient errors)

## Consequences

### Positive
- **No message loss**: Transient errors retry until infrastructure recovers.
- **ADR-0001 compliance**: Every message either succeeds, retries, or lands
  in dead_letters. Nothing is silently dropped.
- **Single dead-letter store**: The existing `dead_letters` table remains
  the only place to look for failed messages (no Kafka DLT).

### Negative
- **Partition blocking**: A transient error blocks processing of the entire
  partition until resolved. This is acceptable because:
  1. Transient errors are rare (infrastructure usually works)
  2. Ordering within a partition must be preserved anyway
  3. The alternative (DLT) would strand good events during DB outages
- **Backoff memory**: If many records fail, the error handler holds state
  for each. In practice, failures cluster around one root cause.

### Trade-offs Accepted
- We accept partition blocking during transient failures in exchange for
  zero message loss.
- We accept that a truly infinite retry for transient errors could block
  a partition forever if infrastructure never recovers. Monitoring and
  alerts should catch this.
