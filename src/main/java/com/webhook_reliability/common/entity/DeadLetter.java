package com.webhook_reliability.common.entity;

import java.time.Instant;
import java.util.UUID;

public record DeadLetter(
    UUID id,
    UUID eventId,           // NULL if deserialization failed
    String reason,          // DESERIALIZATION, MAX_RETRIES_EXCEEDED, SOURCE_NOT_FOUND
    String topic,
    Integer partitionNum,
    Long offsetNum,
    String rawPayload,      // original message for deserialization failures
    Integer lastStatusCode,
    String lastError,
    int attemptCount,
    Instant createdAt
) {
    public static DeadLetter forDeserialization(
            String topic, int partition, long offset, String rawPayload, String error) {
        return new DeadLetter(
            null, null, "DESERIALIZATION",
            topic, partition, offset,
            rawPayload, null, error, 0, null
        );
    }

    public static DeadLetter forMaxRetries(
            UUID eventId, int attemptCount, Integer statusCode, String error) {
        return new DeadLetter(
            null, eventId, "MAX_RETRIES_EXCEEDED",
            null, null, null,
            null, statusCode, error, attemptCount, null
        );
    }

    public static DeadLetter forSourceNotFound(
            UUID eventId, String topic, int partition, long offset) {
        return new DeadLetter(
            null, eventId, "SOURCE_NOT_FOUND",
            topic, partition, offset,
            null, null, "Source not found", 0, null
        );
    }
}