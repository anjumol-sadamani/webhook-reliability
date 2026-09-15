package com.webhook_reliability.common.entity;

import java.time.Instant;
import java.util.UUID;

public record Source(
    UUID id,
    String name,
    KeySource eventIdSource, // BODY or HEADER
    String eventIdPath,      // JSONPath if BODY (e.g., "$.id"), header name if HEADER (e.g., "X-Idempotency-Key")
    String destinationUrl,
    Instant createdAt
) {
    public static Source create(String name, KeySource eventIdSource, String eventIdPath, String destinationUrl) {
        return new Source(null, name, eventIdSource, eventIdPath, destinationUrl, null);
    }
}