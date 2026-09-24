package com.webhook_reliability.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.repository.EventRepository;
import com.webhook_reliability.delivery.DeliveryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final EventRepository eventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${outbox.topic}")
    private String topic;

    @Value("${outbox.batch-size}")
    private int batchSize;

    @Value("${outbox.enabled:true}")
    private boolean enabled;

    public OutboxPublisher(
            EventRepository eventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.eventRepository = eventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms}")
    public void scheduledPublish() {
        if (!enabled) {
            return;
        }
        publishPendingEvents();
    }

    public void publishPendingEvents() {
        List<Event> events = eventRepository.findUnpublished(batchSize);

        for (Event event : events) {
            try {
                String messageValue = serializeMessage(event);
                kafkaTemplate.send(topic, event.sourceId().toString(), messageValue)
                    .get();  // Wait for ack
                eventRepository.markPublished(event.id());
                log.debug("Published event {} to topic {}", event.id(), topic);
            } catch (Exception e) {
                log.warn("Failed to publish event {}, will retry on next poll", event.id(), e);
                // Skip to next event, this one will be retried on next poll
            }
        }

        if (!events.isEmpty()) {
            log.info("Published {} events to Kafka", events.size());
        }
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