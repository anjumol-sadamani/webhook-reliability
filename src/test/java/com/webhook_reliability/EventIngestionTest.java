package com.webhook_reliability;

import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.repository.EventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
}
