package com.webhook_reliability.unit;

import com.webhook_reliability.delivery.DeliveryResult;
import com.webhook_reliability.delivery.WebhookClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class WebhookClientTest {

    private WebhookClient webhookClient;
    private MockRestServiceServer mockServer;

    private static final String WEBHOOK_URL = "https://tenant.example.com/webhook";
    private static final UUID WEBHOOK_ID = UUID.randomUUID();
    private static final String BODY = "{\"type\":\"order.created\",\"data\":{}}";

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        // Bind mock server BEFORE creating WebhookClient (which calls builder.build())
        mockServer = MockRestServiceServer.bindTo(builder).build();
        webhookClient = new WebhookClient(builder, 30000);
    }

    @ParameterizedTest
    @ValueSource(ints = {200, 201, 202, 204})
    void returnsSuccessFor2xxResponses(int statusCode) {
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andRespond(withStatus(HttpStatus.valueOf(statusCode)));

        DeliveryResult result = webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        assertThat(result.success()).isTrue();
        assertThat(result.statusCode()).isEqualTo(statusCode);
        assertThat(result.error()).isNull();
        mockServer.verify();
    }

    @Test
    void setsWebhookIdHeader() {
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andExpect(header("webhook-id", WEBHOOK_ID.toString()))
            .andRespond(withStatus(HttpStatus.OK));

        webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        mockServer.verify();
    }

    @Test
    void setsContentTypeToJson() {
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andExpect(header("Content-Type", MediaType.APPLICATION_JSON_VALUE))
            .andRespond(withStatus(HttpStatus.OK));

        webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        mockServer.verify();
    }

    @Test
    void sendsBodyAsProvided() {
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andExpect(content().string(BODY))
            .andRespond(withStatus(HttpStatus.OK));

        webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        mockServer.verify();
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 422, 429})
    void returnsFailureFor4xxClientErrors(int statusCode) {
        String errorBody = "Bad request";
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andRespond(withStatus(HttpStatus.valueOf(statusCode)).body(errorBody));

        DeliveryResult result = webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        assertThat(result.success()).isFalse();
        assertThat(result.statusCode()).isEqualTo(statusCode);
        assertThat(result.error()).isEqualTo(errorBody);
        mockServer.verify();
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 502, 503, 504})
    void returnsFailureFor5xxServerErrors(int statusCode) {
        String errorBody = "Internal server error";
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andRespond(withStatus(HttpStatus.valueOf(statusCode)).body(errorBody));

        DeliveryResult result = webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        assertThat(result.success()).isFalse();
        assertThat(result.statusCode()).isEqualTo(statusCode);
        assertThat(result.error()).isEqualTo(errorBody);
        mockServer.verify();
    }

    @Test
    void returnsFailureWithZeroStatusCodeForConnectionError() {
        // Create a client pointing to an invalid URL that will fail to connect
        RestClient.Builder builder = RestClient.builder();
        WebhookClient clientForConnectionTest = new WebhookClient(builder, 100);

        // Using an unroutable IP to trigger connection timeout
        String unreachableUrl = "http://10.255.255.1:12345/webhook";

        DeliveryResult result = clientForConnectionTest.send(unreachableUrl, WEBHOOK_ID, BODY);

        assertThat(result.success()).isFalse();
        assertThat(result.statusCode()).isEqualTo(0);
        assertThat(result.error()).isNotNull();
    }

    @Test
    void truncatesLongErrorMessages() {
        // Error messages longer than 1024 chars should be truncated by DeliveryResult
        String longError = "x".repeat(2000);
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR).body(longError));

        DeliveryResult result = webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        assertThat(result.success()).isFalse();
        assertThat(result.error()).hasSize(1024);
        mockServer.verify();
    }

    @Test
    void handlesEmptyErrorBody() {
        mockServer.expect(requestTo(WEBHOOK_URL))
            .andExpect(method(HttpMethod.POST))
            .andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR).body(""));

        DeliveryResult result = webhookClient.send(WEBHOOK_URL, WEBHOOK_ID, BODY);

        assertThat(result.success()).isFalse();
        assertThat(result.statusCode()).isEqualTo(500);
        assertThat(result.error()).isEmpty();
        mockServer.verify();
    }
}
