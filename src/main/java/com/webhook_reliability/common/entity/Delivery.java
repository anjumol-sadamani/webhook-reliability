package com.webhook_reliability.common.entity;

import java.time.Instant;
import java.util.UUID;

public record Delivery(
    UUID id,
    UUID eventId,
    Instant nextRetryAt,
    int attemptCount,
    String status,       // "pending", "delivered", "dead_lettered"
    Integer lastStatusCode,
    String lastError,
    Instant createdAt,
    Instant updatedAt
) {
    public static Delivery create(UUID eventId) {
        return new Delivery(null, eventId, Instant.now(), 0, "pending", null, null, null, null);
    }
}