package com.webhook_reliability.delivery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhook_reliability.common.entity.Delivery;
import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.repository.DeliveryRepository;
import com.webhook_reliability.common.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Scheduled job that re-publishes failed deliveries to Kafka for retry.
 *
 * Polls the deliveries table for entries where status='pending' and
 * next_retry_at has passed, then re-publishes them to Kafka for the
 * DeliveryWorker to attempt again.
 *
 * Uses SELECT ... FOR UPDATE SKIP LOCKED for safe concurrent execution
 * across multiple application instances.
 */
@Service
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final DeliveryRepository deliveryRepository;
    private final EventRepository eventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${outbox.topic}")
    private String topic;

    @Value("${retry.batch-size}")
    private int batchSize;

    public RetryScheduler(
            DeliveryRepository deliveryRepository,
            EventRepository eventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.deliveryRepository = deliveryRepository;
        this.eventRepository = eventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${retry.poll-interval-ms}")
    public void retryFailedDeliveries() {
        List<Delivery> dueDeliveries = deliveryRepository.findDue(batchSize);

        for (Delivery delivery : dueDeliveries) {
            try {
                republishDelivery(delivery);
            } catch (Exception e) {
                log.warn("Failed to republish delivery {} for event {}, will retry on next poll",
                    delivery.id(), delivery.eventId(), e);
            }
        }

        if (!dueDeliveries.isEmpty()) {
            log.info("Re-published {} deliveries for retry", dueDeliveries.size());
        }
    }

    private void republishDelivery(Delivery delivery) throws Exception {
        Optional<Event> eventOpt = eventRepository.findById(delivery.eventId());
        if (eventOpt.isEmpty()) {
            log.error("Event {} not found for delivery {}, marking as dead-lettered",
                delivery.eventId(), delivery.id());
            deliveryRepository.markDeadLettered(delivery.id(), 0, "Event not found");
            return;
        }

        Event event = eventOpt.get();
        String messageValue = serializeMessage(event);

        kafkaTemplate.send(topic, event.sourceId().toString(), messageValue)
            .get();  // Wait for ack

        log.debug("Re-published event {} for retry attempt {}", event.id(), delivery.attemptCount() + 1);
    }

    private String serializeMessage(Event event) throws JsonProcessingException {
        DeliveryMessage message = new DeliveryMessage(
            event.id(),
            event.sourceId(),
            event.body()
        );
        return objectMapper.writeValueAsString(message);
    }
}
