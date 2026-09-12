package com.webhook_reliability.common.entity;

import java.time.Instant;
import java.util.UUID;

public record Event(
    UUID id,
    UUID sourceId,
    String idempotencyKey,
    String body,         // JSONB stored as String
    Instant publishedAt, // null until published to Kafka
    Instant createdAt
) {
    public static Event create(UUID sourceId, String idempotencyKey, String body) {
        return new Event(null, sourceId, idempotencyKey, body, null, null);
    }
}
