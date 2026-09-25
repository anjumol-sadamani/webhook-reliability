package com.webhook_reliability;

import com.fasterxml.jackson.databind.JsonNode;
import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.repository.EventRepository;
import com.webhook_reliability.outbox.OutboxPublisher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.ArrayList;
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

    @Test
    void publishRespectsConfiguredBatchSize() throws Exception {
        UUID sourceId = createSource();

        // Create 5 events
        for (int i = 1; i <= 5; i++) {
            String eventBody = String.format("""
                {
                    "id": "batch-test-event-%d",
                    "type": "test.batch"
                }
                """, i);

            restClient.post()
                .uri("/api/v1/events/" + sourceId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(eventBody)
                .retrieve()
                .toEntity(Void.class);
        }

        // Set batch size to 2
        int originalBatchSize = (int) ReflectionTestUtils.getField(outboxPublisher, "batchSize");
        ReflectionTestUtils.setField(outboxPublisher, "batchSize", 2);

        try {
            // First publish call - should only publish 2
            outboxPublisher.publishPendingEvents();

            Integer publishedCount = jdbc.sql("SELECT COUNT(*) FROM events WHERE source_id = :sourceId AND published_at IS NOT NULL")
                .param("sourceId", sourceId)
                .query(Integer.class)
                .single();
            assertThat(publishedCount).isEqualTo(2);

            // Second publish call - should publish 2 more
            outboxPublisher.publishPendingEvents();

            publishedCount = jdbc.sql("SELECT COUNT(*) FROM events WHERE source_id = :sourceId AND published_at IS NOT NULL")
                .param("sourceId", sourceId)
                .query(Integer.class)
                .single();
            assertThat(publishedCount).isEqualTo(4);

        } finally {
            // Restore original batch size
            ReflectionTestUtils.setField(outboxPublisher, "batchSize", originalBatchSize);
        }
    }

    @Test
    void alreadyPublishedEventsAreNotRepublished() throws Exception {
        UUID sourceId = createSource();

        String eventBody = """
            {
                "id": "already-published-event",
                "type": "test.republish"
            }
            """;

        restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        // Manually mark as published using now() SQL function
        jdbc.sql("UPDATE events SET published_at = now() WHERE source_id = :sourceId AND idempotency_key = :key")
            .param("sourceId", sourceId)
            .param("key", "already-published-event")
            .update();

        // Set up consumer before publish
        Consumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(List.of("webhook-events"));
        consumer.poll(Duration.ofMillis(100));

        // Call publish - should not send anything for this event
        outboxPublisher.publishPendingEvents();

        // Poll for a short time and verify no message for this event
        List<String> receivedMessages = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        for (var record : records) {
            receivedMessages.add(record.value());
        }

        boolean containsOurEvent = receivedMessages.stream()
            .anyMatch(msg -> msg.contains("already-published-event"));
        assertThat(containsOurEvent).isFalse();

        consumer.close();
    }

    @Test
    void kafkaMessageKeyIsSourceId() throws Exception {
        UUID sourceId = createSource();

        String eventBody = """
            {
                "id": "key-test-event",
                "type": "test.key"
            }
            """;

        restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        Consumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(List.of("webhook-events"));
        consumer.poll(Duration.ofMillis(100));

        outboxPublisher.publishPendingEvents();

        AtomicReference<ConsumerRecord<String, String>> foundRecord = new AtomicReference<>();

        await().atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    if (record.value().contains("key-test-event")) {
                        foundRecord.set(record);
                        break;
                    }
                }
                assertThat(foundRecord.get()).isNotNull();
            });

        // Verify the Kafka message key is the sourceId
        assertThat(foundRecord.get().key()).isEqualTo(sourceId.toString());

        consumer.close();
    }
}
