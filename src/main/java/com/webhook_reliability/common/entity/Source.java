package com.webhook_reliability.common.entity;

import java.time.Instant;
import java.util.UUID;

public record Source(
    UUID id,
    String name,
    String eventIdLocation,  // "body" or "header"
    String eventIdPath,      // JSON path when location is "body"
    String eventIdHeader,    // header name when location is "header"
    String destinationUrl,
    Instant createdAt
) {
    public static Source create(String name, String eventIdLocation,
                                 String eventIdPath, String eventIdHeader,
                                 String destinationUrl) {
        return new Source(null, name, eventIdLocation, eventIdPath,
                          eventIdHeader, destinationUrl, null);
    }
}
