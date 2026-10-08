package com.webhook_reliability.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhook_reliability.common.entity.DeadLetter;
import com.webhook_reliability.common.entity.Delivery;
import com.webhook_reliability.common.entity.KeySource;
import com.webhook_reliability.common.entity.Source;
import com.webhook_reliability.common.repository.DeadLetterRepository;
import com.webhook_reliability.common.repository.DeliveryRepository;
import com.webhook_reliability.common.repository.SourceRepository;
import com.webhook_reliability.delivery.BackoffCalculator;
import com.webhook_reliability.delivery.DeliveryResult;
import com.webhook_reliability.delivery.DeliveryWorker;
import com.webhook_reliability.delivery.WebhookClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeliveryWorkerHandleFailureTest {

    @Mock
    private SourceRepository sourceRepository;

    @Mock
    private DeliveryRepository deliveryRepository;

    @Mock
    private DeadLetterRepository deadLetterRepository;

    @Mock
    private WebhookClient webhookClient;

    @Mock
    private TransactionTemplate transactionTemplate;

    @Mock
    private Acknowledgment ack;

    private ObjectMapper objectMapper;
    private DeliveryWorker deliveryWorker;

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final UUID SOURCE_ID = UUID.randomUUID();
    private static final UUID DELIVERY_ID = UUID.randomUUID();
    private static final String DESTINATION_URL = "https://example.com/webhook";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        deliveryWorker = new DeliveryWorker(
            objectMapper,
            sourceRepository,
            deliveryRepository,
            deadLetterRepository,
            webhookClient,
            transactionTemplate
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 5, 10, 13})
    void schedulesRetryWhenBelowMaxAttempts(int currentAttemptCount) {
        setupSuccessfulMessageParsing(currentAttemptCount);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(500, "Server error"));

        deliveryWorker.handleMessage(createRecord(), ack);

        ArgumentCaptor<Instant> retryTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(deliveryRepository).scheduleRetry(
            eq(DELIVERY_ID),
            retryTimeCaptor.capture(),
            eq(500),
            eq("Server error")
        );

        // Verify retry is scheduled in the future
        assertThat(retryTimeCaptor.getValue()).isAfter(Instant.now());

        // Should not dead-letter or mark failed
        verify(deadLetterRepository, never()).save(any());
        verify(deliveryRepository, never()).markFailed(any());
    }

    @Test
    void deadLettersWhenReachingMaxAttempts() {
        int currentAttemptCount = BackoffCalculator.MAX_ATTEMPTS - 1; // 14
        setupSuccessfulMessageParsing(currentAttemptCount);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(503, "Service unavailable"));

        // Mock transaction template to execute the callback
        doAnswer(invocation -> {
            Consumer<Object> callback = invocation.getArgument(0);
            callback.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        deliveryWorker.handleMessage(createRecord(), ack);

        // Verify dead letter saved with correct data
        ArgumentCaptor<DeadLetter> deadLetterCaptor = ArgumentCaptor.forClass(DeadLetter.class);
        verify(deadLetterRepository).save(deadLetterCaptor.capture());

        DeadLetter deadLetter = deadLetterCaptor.getValue();
        assertThat(deadLetter.eventId()).isEqualTo(EVENT_ID);
        assertThat(deadLetter.reason()).isEqualTo("MAX_RETRIES_EXCEEDED");
        assertThat(deadLetter.attemptCount()).isEqualTo(BackoffCalculator.MAX_ATTEMPTS);
        assertThat(deadLetter.lastStatusCode()).isEqualTo(503);
        assertThat(deadLetter.lastError()).isEqualTo("Service unavailable");

        // Verify delivery marked as failed
        verify(deliveryRepository).markFailed(DELIVERY_ID);

        // Should not schedule retry
        verify(deliveryRepository, never()).scheduleRetry(any(), any(), anyInt(), anyString());
    }

    @Test
    void deadLettersWhenBeyondMaxAttempts() {
        int currentAttemptCount = BackoffCalculator.MAX_ATTEMPTS; // 15
        setupSuccessfulMessageParsing(currentAttemptCount);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(500, "Error"));

        doAnswer(invocation -> {
            Consumer<Object> callback = invocation.getArgument(0);
            callback.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        deliveryWorker.handleMessage(createRecord(), ack);

        verify(deadLetterRepository).save(any(DeadLetter.class));
        verify(deliveryRepository).markFailed(DELIVERY_ID);
        verify(deliveryRepository, never()).scheduleRetry(any(), any(), anyInt(), anyString());
    }

    @Test
    void transactionWrapsDeadLetterAndMarkFailed() {
        int currentAttemptCount = BackoffCalculator.MAX_ATTEMPTS - 1;
        setupSuccessfulMessageParsing(currentAttemptCount);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(500, "Error"));

        // Don't execute the callback - just verify it was called
        deliveryWorker.handleMessage(createRecord(), ack);

        // Verify transaction template was used
        verify(transactionTemplate).executeWithoutResult(any());
    }

    @Test
    void passesCorrectStatusCodeAndErrorToScheduleRetry() {
        setupSuccessfulMessageParsing(0);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(429, "Rate limited"));

        deliveryWorker.handleMessage(createRecord(), ack);

        verify(deliveryRepository).scheduleRetry(
            eq(DELIVERY_ID),
            any(Instant.class),
            eq(429),
            eq("Rate limited")
        );
    }

    @Test
    void acknowledgesMessageAfterSchedulingRetry() {
        setupSuccessfulMessageParsing(0);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(500, "Error"));

        deliveryWorker.handleMessage(createRecord(), ack);

        verify(ack).acknowledge();
    }

    @Test
    void acknowledgesMessageAfterDeadLettering() {
        setupSuccessfulMessageParsing(BackoffCalculator.MAX_ATTEMPTS - 1);
        when(webhookClient.send(anyString(), any(), anyString()))
            .thenReturn(DeliveryResult.failure(500, "Error"));

        doAnswer(invocation -> {
            Consumer<Object> callback = invocation.getArgument(0);
            callback.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        deliveryWorker.handleMessage(createRecord(), ack);

        verify(ack).acknowledge();
    }

    private void setupSuccessfulMessageParsing(int attemptCount) {
        Source source = new Source(SOURCE_ID, "test-source", KeySource.BODY, "$.id", DESTINATION_URL, null);
        when(sourceRepository.findById(SOURCE_ID)).thenReturn(Optional.of(source));

        Delivery delivery = new Delivery(
            DELIVERY_ID, EVENT_ID, Instant.now(), attemptCount,
            "pending", null, null, Instant.now(), Instant.now()
        );
        when(deliveryRepository.findByEventId(EVENT_ID)).thenReturn(Optional.of(delivery));
    }

    private ConsumerRecord<String, String> createRecord() {
        String messageJson = String.format(
            "{\"eventId\":\"%s\",\"sourceId\":\"%s\",\"body\":\"{}\"}",
            EVENT_ID, SOURCE_ID
        );
        return new ConsumerRecord<>("webhook-events", 0, 0L, SOURCE_ID.toString(), messageJson);
    }
}