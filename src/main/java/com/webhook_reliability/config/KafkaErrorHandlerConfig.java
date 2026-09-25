package com.webhook_reliability.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhook_reliability.common.entity.DeadLetter;
import com.webhook_reliability.common.repository.DeadLetterRepository;
import com.webhook_reliability.delivery.DeliveryMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.UUID;

/**
 * Configures Kafka error handling strategy per ADR-0005.
 *
 * Transient errors (DB unavailable, connection failures): retry indefinitely
 * with exponential backoff until the infrastructure recovers.
 *
 * Non-transient errors (bad data, logic errors): retry a few times, then
 * write to dead_letters table and commit offset.
 */
@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);

    private static final long INITIAL_INTERVAL_MS = 1000;
    private static final double MULTIPLIER = 2.0;
    private static final long MAX_INTERVAL_MS = 60000;
    private static final int MAX_ATTEMPTS = 10;

    private final DeadLetterRepository deadLetterRepository;
    private final ObjectMapper objectMapper;

    public KafkaErrorHandlerConfig(DeadLetterRepository deadLetterRepository, ObjectMapper objectMapper) {
        this.deadLetterRepository = deadLetterRepository;
        this.objectMapper = objectMapper;
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(INITIAL_INTERVAL_MS, MULTIPLIER);
        backOff.setMaxInterval(MAX_INTERVAL_MS);
        backOff.setMaxElapsedTime(MAX_INTERVAL_MS * MAX_ATTEMPTS); // Rough limit before recovery

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            this::recoverFailedRecord,
            backOff
        );

        // Mark exceptions that should never be retried (immediate dead-letter)
        errorHandler.addNotRetryableExceptions(
            JsonProcessingException.class
        );

        return errorHandler;
    }

    /**
     * Called when retries are exhausted. For transient errors, throws to
     * trigger infinite retry. For non-transient errors, writes to dead_letters.
     */
    @SuppressWarnings("unchecked")
    private void recoverFailedRecord(ConsumerRecord<?, ?> record, Exception exception) {
        if (isTransientError(exception)) {
            log.warn("Transient error processing record at {}:{}:{}, will retry indefinitely: {}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());
            // Throwing from recoverer causes seek-back, enabling infinite retry
            throw new RuntimeException("Transient error, retrying", exception);
        }

        // Non-transient error: write to dead_letters and allow commit
        log.error("Non-transient error processing record at {}:{}:{}, sending to dead_letters: {}",
            record.topic(), record.partition(), record.offset(), exception.getMessage(), exception);

        UUID eventId = extractEventId((ConsumerRecord<String, String>) record);
        deadLetterRepository.save(DeadLetter.forInfrastructureError(
            eventId,
            record.topic(),
            record.partition(),
            record.offset(),
            (String) record.value(),
            exception.getClass().getSimpleName() + ": " + exception.getMessage()
        ));
    }

    private UUID extractEventId(ConsumerRecord<String, String> record) {
        try {
            DeliveryMessage message = objectMapper.readValue(record.value(), DeliveryMessage.class);
            return message.eventId();
        } catch (Exception e) {
            // Can't parse message, eventId will be null in dead_letter
            return null;
        }
    }

    private boolean isTransientError(Exception exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof TransientDataAccessException ||
                cause instanceof RecoverableDataAccessException ||
                cause instanceof ConnectException ||
                cause instanceof SocketTimeoutException) {
                return true;
            }
            // DataAccessException is broad; check if it's infrastructure-related
            if (cause instanceof DataAccessException) {
                String message = cause.getMessage();
                if (message != null && (
                    message.contains("connection") ||
                    message.contains("Connection") ||
                    message.contains("timeout") ||
                    message.contains("Timeout"))) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
