package com.webhook_reliability.ingestion.dto;

import java.util.UUID;

public record CreateSourceResponse(
    UUID sourceId,
    String webhookUrl
) {}