package com.webhook_reliability;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
}
