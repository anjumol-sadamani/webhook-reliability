package com.webhook_reliability.delivery;

import java.util.UUID;

/**
 * Kafka message envelope for webhook delivery.
 * Published by OutboxPublisher, consumed by DeliveryWorker.
 */
public record DeliveryMessage(
    UUID eventId,
    UUID sourceId,
    String body
) {
}
