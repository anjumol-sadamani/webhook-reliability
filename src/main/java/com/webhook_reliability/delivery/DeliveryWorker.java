package com.webhook_reliability.delivery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhook_reliability.common.entity.Delivery;
import com.webhook_reliability.common.entity.Source;
import com.webhook_reliability.common.repository.DeliveryRepository;
import com.webhook_reliability.common.repository.SourceRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * Kafka consumer that delivers webhooks to tenant endpoints.
 *
 * Consumes messages from the webhook-events topic, calls the tenant's
 * destination URL, and updates the delivery status in the database.
 * Uses manual offset commit per ADR-0001.
 */
@Service
public class DeliveryWorker {

    private static final Logger log = LoggerFactory.getLogger(DeliveryWorker.class);

    private final ObjectMapper objectMapper;
    private final SourceRepository sourceRepository;
    private final DeliveryRepository deliveryRepository;
    private final WebhookClient webhookClient;

    public DeliveryWorker(
            ObjectMapper objectMapper,
            SourceRepository sourceRepository,
            DeliveryRepository deliveryRepository,
            WebhookClient webhookClient) {
        this.objectMapper = objectMapper;
        this.sourceRepository = sourceRepository;
        this.deliveryRepository = deliveryRepository;
        this.webhookClient = webhookClient;
    }

    @KafkaListener(topics = "${outbox.topic}", groupId = "${delivery.consumer-group}")
    public void handleMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        DeliveryMessage message;
        try {
            message = objectMapper.readValue(record.value(), DeliveryMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize message, skipping: {}", record.value(), e);
            ack.acknowledge();
            return;
        }

        try {
            processDelivery(message);
        } catch (Exception e) {
            log.error("Failed to process delivery for event {}: {}", message.eventId(), e.getMessage(), e);
            // Don't acknowledge - let Kafka redeliver
            // Note: This could cause infinite loops for poison messages.
            // In production, consider a dead-letter queue after N redeliveries.
            throw e;
        }

        ack.acknowledge();
    }

    private void processDelivery(DeliveryMessage message) {
        UUID eventId = message.eventId();
        UUID sourceId = message.sourceId();

        // Look up source to get destination URL
        Optional<Source> sourceOpt = sourceRepository.findById(sourceId);
        if (sourceOpt.isEmpty()) {
            log.error("Source {} not found for event {}, cannot deliver", sourceId, eventId);
            return;
        }
        Source source = sourceOpt.get();

        // Find or create delivery record
        Delivery delivery = deliveryRepository.findByEventId(eventId)
            .orElseGet(() -> deliveryRepository.save(Delivery.create(eventId)));

        // Check if already delivered (duplicate message)
        if ("delivered".equals(delivery.status())) {
            log.debug("Event {} already delivered, skipping", eventId);
            return;
        }

        // Check if dead-lettered (shouldn't happen via Kafka, but defensive)
        if ("dead_lettered".equals(delivery.status())) {
            log.warn("Event {} is dead-lettered, skipping", eventId);
            return;
        }

        // Attempt delivery
        DeliveryResult result = webhookClient.send(
            source.destinationUrl(),
            eventId,  // webhook-id header for tenant idempotency
            message.body()
        );

        // Update delivery status based on result
        if (result.success()) {
            deliveryRepository.markDelivered(delivery.id(), result.statusCode());
            log.info("Successfully delivered event {} to {}", eventId, source.destinationUrl());
        } else {
            handleFailure(delivery, result);
        }
    }

    private void handleFailure(Delivery delivery, DeliveryResult result) {
        int nextAttempt = delivery.attemptCount() + 1;

        if (nextAttempt >= BackoffCalculator.MAX_ATTEMPTS) {
            deliveryRepository.markDeadLettered(delivery.id(), result.statusCode(), result.error());
            log.warn("Event {} dead-lettered after {} attempts", delivery.eventId(), nextAttempt);
        } else {
            var nextRetryAt = BackoffCalculator.nextRetryAt(nextAttempt);
            deliveryRepository.scheduleRetry(delivery.id(), nextRetryAt, result.statusCode(), result.error());
            log.info("Event {} scheduled for retry {} at {}", delivery.eventId(), nextAttempt, nextRetryAt);
        }
    }
}