package com.webhook_reliability;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceApiTest extends AbstractIntegrationTest {

    @Test
    void postSourcesCreatesSource() throws Exception {
        String requestBody = """
            {
                "name": "my-webhook",
                "eventIdSource": "BODY",
                "eventIdPath": "$.id",
                "destinationUrl": "https://example.com/webhook"
            }
            """;

        ResponseEntity<String> response = restClient.post()
            .uri("/api/v1/sources")
            .contentType(MediaType.APPLICATION_JSON)
            .body(requestBody)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();

        JsonNode json = objectMapper.readTree(response.getBody());
        assertThat(json.has("sourceId")).isTrue();
        assertThat(json.has("webhookUrl")).isTrue();

        UUID sourceId = UUID.fromString(json.get("sourceId").asText());

        // Verify source was persisted in DB
        Integer count = jdbc.sql("SELECT COUNT(*) FROM sources WHERE id = :id")
            .param("id", sourceId)
            .query(Integer.class)
            .single();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void postSourcesWithMissingNameReturns400() {
        String requestBody = """
            {
                "eventIdSource": "BODY",
                "eventIdPath": "$.id",
                "destinationUrl": "https://example.com/webhook"
            }
            """;

        assertThatThrownBy(() -> restClient.post()
                .uri("/api/v1/sources")
                .contentType(MediaType.APPLICATION_JSON)
                .body(requestBody)
                .retrieve()
                .toEntity(String.class))
            .isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    @Test
    void postSourcesWithMissingEventIdSourceReturns400() {
        String requestBody = """
            {
                "name": "my-webhook",
                "eventIdPath": "$.id",
                "destinationUrl": "https://example.com/webhook"
            }
            """;

        assertThatThrownBy(() -> restClient.post()
                .uri("/api/v1/sources")
                .contentType(MediaType.APPLICATION_JSON)
                .body(requestBody)
                .retrieve()
                .toEntity(String.class))
            .isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    @Test
    void postSourcesWithInvalidEventIdSourceReturns400() {
        String requestBody = """
            {
                "name": "my-webhook",
                "eventIdSource": "INVALID_VALUE",
                "eventIdPath": "$.id",
                "destinationUrl": "https://example.com/webhook"
            }
            """;

        assertThatThrownBy(() -> restClient.post()
                .uri("/api/v1/sources")
                .contentType(MediaType.APPLICATION_JSON)
                .body(requestBody)
                .retrieve()
                .toEntity(String.class))
            .isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    @Test
    void postSourcesWithHeaderModeCreatesSource() throws Exception {
        String requestBody = """
            {
                "name": "header-mode-source",
                "eventIdSource": "HEADER",
                "eventIdPath": "X-Idempotency-Key",
                "destinationUrl": "https://example.com/webhook"
            }
            """;

        ResponseEntity<String> response = restClient.post()
            .uri("/api/v1/sources")
            .contentType(MediaType.APPLICATION_JSON)
            .body(requestBody)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        JsonNode json = objectMapper.readTree(response.getBody());
        UUID sourceId = UUID.fromString(json.get("sourceId").asText());

        // Verify source was persisted with HEADER mode
        String eventIdSource = jdbc.sql("SELECT event_id_source FROM sources WHERE id = :id")
            .param("id", sourceId)
            .query(String.class)
            .single();
        assertThat(eventIdSource).isEqualTo("HEADER");
    }
}
