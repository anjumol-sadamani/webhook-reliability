package com.webhook_reliability.ingestion.service;

import com.jayway.jsonpath.JsonPath;
import com.webhook_reliability.common.entity.Event;
import com.webhook_reliability.common.entity.Source;
import com.webhook_reliability.common.repository.EventRepository;
import com.webhook_reliability.common.repository.SourceRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class IngestionService {

    private final EventRepository eventRepository;
    private final SourceRepository sourceRepository;

    public IngestionService(EventRepository eventRepository, SourceRepository sourceRepository) {
        this.eventRepository = eventRepository;
        this.sourceRepository = sourceRepository;
    }

    @Transactional
    public Event ingest(UUID sourceId, String rawBody) {
        Source source = sourceRepository.findById(sourceId)
            .orElseThrow(() -> new IllegalArgumentException("Source not found: " + sourceId));

        String idempotencyKey = extractIdempotencyKey(rawBody, source.eventIdPath());

        // Idempotency check
        Optional<Event> existing = eventRepository.findBySourceIdAndIdempotencyKey(sourceId, idempotencyKey);
        if (existing.isPresent()) {
            return existing.get();
        }

        // Save new event
        Event event = Event.create(sourceId, idempotencyKey, rawBody);
        return eventRepository.save(event);
    }

    private String extractIdempotencyKey(String rawBody, String eventIdPath) {
        Object value = JsonPath.read(rawBody, eventIdPath);
        if (value == null) {
            throw new IllegalArgumentException("Could not extract idempotency key using path: " + eventIdPath);
        }
        return value.toString();
    }
}