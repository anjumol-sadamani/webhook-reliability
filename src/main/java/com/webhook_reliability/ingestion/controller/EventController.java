package com.webhook_reliability.ingestion.controller;

import com.webhook_reliability.ingestion.service.IngestionService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final IngestionService ingestionService;

    public EventController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/{sourceId}")
    public ResponseEntity<Void> ingest(
            @PathVariable UUID sourceId,
            @RequestHeader HttpHeaders headers,
            @RequestBody String rawBody) {
        ingestionService.ingest(sourceId, rawBody, headers);
        return ResponseEntity.accepted().build();
    }
}