package com.webhook_reliability.unit;

import com.jayway.jsonpath.PathNotFoundException;
import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.entity.KeySource;
import com.webhook_reliability.common.entity.Source;
import com.webhook_reliability.common.repository.EventRepository;
import com.webhook_reliability.common.repository.SourceRepository;
import com.webhook_reliability.ingestion.service.IngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyKeyExtractionTest {

    @Mock
    private EventRepository eventRepository;

    @Mock
    private SourceRepository sourceRepository;

    private IngestionService ingestionService;

    @BeforeEach
    void setUp() {
        ingestionService = new IngestionService(eventRepository, sourceRepository);
    }

    @Test
    void extractsKeyFromBodyUsingJsonPath() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.BODY, "$.id");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));
        when(eventRepository.findBySourceIdAndIdempotencyKey(any(), any())).thenReturn(Optional.empty());
        when(eventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        String body = """
            {"id": "abc-123", "type": "order.created"}
            """;

        ingestionService.ingest(sourceId, body, new HttpHeaders());

        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventRepository).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().idempotencyKey()).isEqualTo("abc-123");
    }

    @Test
    void extractsKeyFromNestedJsonPath() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.BODY, "$.data.eventId");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));
        when(eventRepository.findBySourceIdAndIdempotencyKey(any(), any())).thenReturn(Optional.empty());
        when(eventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        String body = """
            {"data": {"eventId": "nested-xyz"}, "type": "test"}
            """;

        ingestionService.ingest(sourceId, body, new HttpHeaders());

        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventRepository).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().idempotencyKey()).isEqualTo("nested-xyz");
    }

    @Test
    void extractsKeyFromHeader() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.HEADER, "X-Idempotency-Key");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));
        when(eventRepository.findBySourceIdAndIdempotencyKey(any(), any())).thenReturn(Optional.empty());
        when(eventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Idempotency-Key", "header-value-456");

        ingestionService.ingest(sourceId, "{}", headers);

        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventRepository).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().idempotencyKey()).isEqualTo("header-value-456");
    }

    @Test
    void throwsWhenHeaderMissing() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.HEADER, "X-Idempotency-Key");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));

        assertThatThrownBy(() -> ingestionService.ingest(sourceId, "{}", new HttpHeaders()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required header: X-Idempotency-Key");
    }

    @Test
    void throwsWhenHeaderBlank() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.HEADER, "X-Idempotency-Key");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));

        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Idempotency-Key", "   ");

        assertThatThrownBy(() -> ingestionService.ingest(sourceId, "{}", headers))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required header: X-Idempotency-Key");
    }

    @Test
    void throwsWhenJsonPathNotFound() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.BODY, "$.missing");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));

        String body = """
            {"id": "abc", "type": "test"}
            """;

        assertThatThrownBy(() -> ingestionService.ingest(sourceId, body, new HttpHeaders()))
            .isInstanceOf(PathNotFoundException.class);
    }

    @Test
    void throwsWhenJsonMalformed() {
        UUID sourceId = UUID.randomUUID();
        Source source = createSource(sourceId, KeySource.BODY, "$.id");
        when(sourceRepository.findById(sourceId)).thenReturn(Optional.of(source));

        String body = "{invalid json";

        assertThatThrownBy(() -> ingestionService.ingest(sourceId, body, new HttpHeaders()))
            .isInstanceOf(Exception.class); // JsonPath throws various exceptions for malformed JSON
    }

    private Source createSource(UUID id, KeySource keySource, String eventIdPath) {
        return new Source(id, "test-source", keySource, eventIdPath, "https://example.com/webhook", null);
    }
}
