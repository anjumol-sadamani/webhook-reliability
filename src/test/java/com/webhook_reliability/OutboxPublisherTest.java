package com.webhook_reliability;

import com.fasterxml.jackson.databind.JsonNode;
import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.repository.EventRepository;
import com.webhook_reliability.outbox.OutboxPublisher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OutboxPublisherTest extends AbstractIntegrationTest {

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private EventRepository eventRepository;

    @Test
    void publishPendingEventsSendsToKafkaAndMarksPublished() throws Exception {
        UUID sourceId = createSource();

        String eventBody = """
            {
                "id": "outbox-test-event",
                "type": "test.outbox",
                "data": "test"
            }
            """;

        restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        // Verify event is initially unpublished
        Optional<Event> beforePublish = eventRepository.findBySourceIdAndIdempotencyKey(sourceId, "outbox-test-event");
        assertThat(beforePublish).isPresent();
        assertThat(beforePublish.get().publishedAt()).isNull();

        // Create a Kafka consumer and subscribe before publishing
        Consumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(List.of("webhook-events"));
        // Initial poll to trigger partition assignment
        consumer.poll(Duration.ofMillis(100));

        // Trigger the outbox publisher
        outboxPublisher.publishPendingEvents();

        // Verify event is now marked as published in DB
        Optional<Event> afterPublish = eventRepository.findBySourceIdAndIdempotencyKey(sourceId, "outbox-test-event");
        assertThat(afterPublish).isPresent();
        assertThat(afterPublish.get().publishedAt()).isNotNull();

        // Use Awaitility to poll Kafka until the message arrives
        AtomicReference<JsonNode> foundMessage = new AtomicReference<>();

        await().atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    if (record.value().contains("outbox-test-event")) {
                        foundMessage.set(objectMapper.readTree(record.value()));
                        break;
                    }
                }
                assertThat(foundMessage.get()).isNotNull();
            });

        // Verify message structure
        JsonNode message = foundMessage.get();
        assertThat(message.has("eventId")).isTrue();
        assertThat(message.has("sourceId")).isTrue();
        assertThat(message.has("body")).isTrue();

        consumer.close();
    }
}
