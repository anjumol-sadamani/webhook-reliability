package com.webhook_reliability.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.UUID;

/**
 * HTTP client for delivering webhooks to tenant endpoints.
 */
@Component
public class WebhookClient {

    private static final Logger log = LoggerFactory.getLogger(WebhookClient.class);

    private final RestClient restClient;

    public WebhookClient(
            RestClient.Builder restClientBuilder,
            @Value("${delivery.http-timeout-ms:30000}") int timeoutMs) {
        this.restClient = restClientBuilder
            .defaultHeaders(headers -> {
                headers.setContentType(MediaType.APPLICATION_JSON);
            })
            .build();
    }

    /**
     * Deliver a webhook to the tenant's endpoint.
     *
     * @param url the tenant's webhook URL
     * @param webhookId unique ID for idempotency (the event's UUID)
     * @param body the JSON payload to deliver
     * @return the result of the delivery attempt
     */
    public DeliveryResult send(String url, UUID webhookId, String body) {
        try {
            var response = restClient.post()
                .uri(url)
                .header("webhook-id", webhookId.toString())
                .body(body)
                .retrieve()
                .toBodilessEntity();

            int statusCode = response.getStatusCode().value();
            log.debug("Webhook delivered to {} with status {}", url, statusCode);
            return DeliveryResult.success(statusCode);

        } catch (org.springframework.web.client.HttpClientErrorException e) {
            // 4xx - client error, don't retry (except maybe 429)
            int statusCode = e.getStatusCode().value();
            String error = e.getResponseBodyAsString();
            log.warn("Webhook delivery failed (client error) to {}: {} - {}", url, statusCode, error);
            return DeliveryResult.failure(statusCode, error);

        } catch (org.springframework.web.client.HttpServerErrorException e) {
            // 5xx - server error, should retry
            int statusCode = e.getStatusCode().value();
            String error = e.getResponseBodyAsString();
            log.warn("Webhook delivery failed (server error) to {}: {} - {}", url, statusCode, error);
            return DeliveryResult.failure(statusCode, error);

        } catch (org.springframework.web.client.ResourceAccessException e) {
            // Connection/timeout errors
            String error = e.getMessage();
            log.warn("Webhook delivery failed (connection error) to {}: {}", url, error);
            return DeliveryResult.failure(0, error);

        } catch (Exception e) {
            // Unexpected errors
            String error = e.getMessage();
            log.error("Webhook delivery failed (unexpected error) to {}: {}", url, error, e);
            return DeliveryResult.failure(0, error);
        }
    }
}