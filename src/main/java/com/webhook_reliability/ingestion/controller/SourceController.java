package com.webhook_reliability.ingestion.controller;

import com.webhook_reliability.common.entity.Source;
import com.webhook_reliability.ingestion.dto.CreateSourceRequest;
import com.webhook_reliability.ingestion.dto.CreateSourceResponse;
import com.webhook_reliability.ingestion.service.SourceService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/api/v1/sources")
public class SourceController {

    private final SourceService sourceService;

    public SourceController(SourceService sourceService) {
        this.sourceService = sourceService;
    }

    @PostMapping
    public ResponseEntity<CreateSourceResponse> create(
            @Valid @RequestBody CreateSourceRequest request,
            HttpServletRequest httpRequest) {
        Source source = sourceService.create(request);

        String baseUrl = getBaseUrl(httpRequest);
        String webhookUrl = baseUrl + "/api/v1/events/" + source.id();

        CreateSourceResponse response = new CreateSourceResponse(source.id(), webhookUrl);
        return ResponseEntity
            .created(URI.create("/api/v1/sources/" + source.id()))
            .body(response);
    }

    private String getBaseUrl(HttpServletRequest request) {
        String scheme = request.getScheme();
        String host = request.getServerName();
        int port = request.getServerPort();

        if ((scheme.equals("http") && port == 80) || (scheme.equals("https") && port == 443)) {
            return scheme + "://" + host;
        }
        return scheme + "://" + host + ":" + port;
    }
}