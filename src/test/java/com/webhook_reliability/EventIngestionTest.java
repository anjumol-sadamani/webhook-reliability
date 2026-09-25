package com.webhook_reliability;

import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.repository.EventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventIngestionTest extends AbstractIntegrationTest {

    @Autowired
    private EventRepository eventRepository;

    @Test
    void postEventsSavesEvent() throws Exception {
        UUID sourceId = createSource();

        String eventBody = """
            {
                "id": "event-123",
                "type": "order.created",
                "data": {"orderId": 456}
            }
            """;

        ResponseEntity<Void> response = restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Verify event was persisted in DB
        Optional<Event> event = eventRepository.findBySourceIdAndIdempotencyKey(sourceId, "event-123");
        assertThat(event).isPresent();
        assertThat(event.get().body()).contains("order.created");
    }

    @Test
    void postingSameEventTwiceDoesNotCreateDuplicate() throws Exception {
        UUID sourceId = createSource();

        String eventBody = """
            {
                "id": "duplicate-event-001",
                "type": "payment.received",
                "amount": 100
            }
            """;

        // Post the same event twice
        ResponseEntity<Void> response1 = restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        ResponseEntity<Void> response2 = restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Verify only one event exists in DB
        Integer count = jdbc.sql("SELECT COUNT(*) FROM events WHERE source_id = :sourceId AND idempotency_key = :key")
            .param("sourceId", sourceId)
            .param("key", "duplicate-event-001")
            .query(Integer.class)
            .single();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void postEventToUnknownSourceReturns404() {
        UUID unknownSourceId = UUID.randomUUID();

        String eventBody = """
            {
                "id": "event-xyz",
                "type": "test"
            }
            """;

        assertThatThrownBy(() -> restClient.post()
                .uri("/api/v1/events/" + unknownSourceId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(eventBody)
                .retrieve()
                .toEntity(Void.class))
            .isInstanceOf(HttpClientErrorException.NotFound.class);
    }

    @Test
    void postEventWithMissingIdempotencyKeyPathReturns400() throws Exception {
        UUID sourceId = createSource(); // Source expects $.id

        String eventBody = """
            {
                "type": "missing-id-field",
                "data": "test"
            }
            """;

        assertThatThrownBy(() -> restClient.post()
                .uri("/api/v1/events/" + sourceId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(eventBody)
                .retrieve()
                .toEntity(Void.class))
            .isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    @Test
    void postMalformedJsonReturns400() throws Exception {
        UUID sourceId = createSource();

        String malformedBody = "{not valid json";

        assertThatThrownBy(() -> restClient.post()
                .uri("/api/v1/events/" + sourceId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(malformedBody)
                .retrieve()
                .toEntity(Void.class))
            .isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    @Test
    void concurrentPostsOfSameEventCreateOnlyOne() throws Exception {
        UUID sourceId = createSource();
        int threadCount = 10;

        String eventBody = """
            {
                "id": "concurrent-event-001",
                "type": "concurrent.test"
            }
            """;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    restClient.post()
                        .uri("/api/v1/events/" + sourceId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(eventBody)
                        .retrieve()
                        .toEntity(Void.class);
                } catch (Exception e) {
                    // Some threads may get errors due to race, that's expected
                }
            }));
        }

        startLatch.countDown(); // Release all threads at once

        for (Future<?> future : futures) {
            future.get(); // Wait for all to complete
        }
        executor.shutdown();

        // Verify only one event exists in DB
        Integer count = jdbc.sql("SELECT COUNT(*) FROM events WHERE source_id = :sourceId AND idempotency_key = :key")
            .param("sourceId", sourceId)
            .param("key", "concurrent-event-001")
            .query(Integer.class)
            .single();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void postEventWithHeaderModeExtractsKey() throws Exception {
        // Create source with HEADER mode
        String sourceRequest = """
            {
                "name": "header-mode-source",
                "eventIdSource": "HEADER",
                "eventIdPath": "X-Idempotency-Key",
                "destinationUrl": "https://example.com/webhook"
            }
            """;

        ResponseEntity<String> sourceResponse = restClient.post()
            .uri("/api/v1/sources")
            .contentType(MediaType.APPLICATION_JSON)
            .body(sourceRequest)
            .retrieve()
            .toEntity(String.class);

        UUID sourceId = UUID.fromString(objectMapper.readTree(sourceResponse.getBody()).get("sourceId").asText());

        String eventBody = """
            {
                "type": "header-mode-event",
                "data": "test"
            }
            """;

        ResponseEntity<Void> response = restClient.post()
            .uri("/api/v1/events/" + sourceId)
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", "header-key-value-123")
            .body(eventBody)
            .retrieve()
            .toEntity(Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Verify event was saved with the header value as idempotency key
        Optional<Event> event = eventRepository.findBySourceIdAndIdempotencyKey(sourceId, "header-key-value-123");
        assertThat(event).isPresent();
    }
}
