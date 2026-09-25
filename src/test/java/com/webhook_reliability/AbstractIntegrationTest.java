package com.webhook_reliability;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.UUID;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    // Singleton containers - started once and shared across all test classes
    static final PostgreSQLContainer<?> postgres;
    static final KafkaContainer kafka;

    static {
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
            .withInitScript("init-uuidv7.sql");
        postgres.start();

        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
        kafka.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        // Required because init script creates uuidv7() function in public schema
        registry.add("spring.flyway.baseline-on-migrate", () -> "true");
        registry.add("spring.flyway.baseline-version", () -> "0");
        // Disable scheduled outbox publisher to prevent race conditions in tests
        registry.add("outbox.enabled", () -> "false");
        // Start from earliest offset so tests don't miss messages
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected JdbcClient jdbc;

    @Autowired
    protected ObjectMapper objectMapper;

    protected RestClient restClient;

    @BeforeEach
    void setUpBase() {
        restClient = RestClient.builder()
            .baseUrl("http://localhost:" + port)
            .build();

        jdbc.sql("DELETE FROM dead_letters").update();
        jdbc.sql("DELETE FROM deliveries").update();
        jdbc.sql("DELETE FROM events").update();
        jdbc.sql("DELETE FROM sources").update();
    }

    protected UUID createSource() throws Exception {
        String requestBody = """
            {
                "name": "test-source",
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

        JsonNode json = objectMapper.readTree(response.getBody());
        return UUID.fromString(json.get("sourceId").asText());
    }

    protected Consumer<String, String> createKafkaConsumer() {
        Map<String, Object> props = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        return new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
    }
}
