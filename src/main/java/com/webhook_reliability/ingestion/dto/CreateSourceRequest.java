package com.webhook_reliability.ingestion.dto;

import com.webhook_reliability.common.entity.KeySource;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateSourceRequest(
    @NotBlank String name,
    @NotNull KeySource eventIdSource,  // BODY or HEADER
    @NotBlank String eventIdPath,      // JSONPath if BODY (e.g., "$.id"), header name if HEADER (e.g., "X-Idempotency-Key")
    @NotBlank String destinationUrl
) {}