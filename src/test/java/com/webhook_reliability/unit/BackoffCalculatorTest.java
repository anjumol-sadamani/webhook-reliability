package com.webhook_reliability.unit;

import com.webhook_reliability.delivery.BackoffCalculator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffCalculatorTest {

    @Test
    void maxAttemptsIs15() {
        assertThat(BackoffCalculator.MAX_ATTEMPTS).isEqualTo(15);
    }

    @ParameterizedTest
    @CsvSource({
        "0, 30",      // attempt 0 -> 30s base
        "1, 60",      // attempt 1 -> 1m base
        "2, 120",     // attempt 2 -> 2m base
        "3, 240",     // attempt 3 -> 4m base
        "4, 480",     // attempt 4 -> 8m base
        "5, 960",     // attempt 5 -> 16m base
        "6, 1920",    // attempt 6 -> 32m base
        "7, 3600",    // attempt 7 -> 1h base (capped)
    })
    void nextRetryAtReturnsDelayWithinJitterRange(int attemptCount, long baseDelaySeconds) {
        Instant before = Instant.now();
        Instant nextRetry = BackoffCalculator.nextRetryAt(attemptCount);
        Instant after = Instant.now();

        Duration minDelay = Duration.ofMillis((long) (baseDelaySeconds * 1000 * 0.8));
        Duration maxDelay = Duration.ofMillis((long) (baseDelaySeconds * 1000 * 1.2));

        // nextRetry should be between (before + minDelay) and (after + maxDelay)
        assertThat(nextRetry).isAfterOrEqualTo(before.plus(minDelay));
        assertThat(nextRetry).isBeforeOrEqualTo(after.plus(maxDelay));
    }

    @Test
    void delaysCappedAtOneHourForHighAttempts() {
        long oneHourSeconds = 3600;

        for (int attempt = 8; attempt <= 15; attempt++) {
            Instant before = Instant.now();
            Instant nextRetry = BackoffCalculator.nextRetryAt(attempt);
            Instant after = Instant.now();

            Duration minDelay = Duration.ofMillis((long) (oneHourSeconds * 1000 * 0.8));
            Duration maxDelay = Duration.ofMillis((long) (oneHourSeconds * 1000 * 1.2));

            assertThat(nextRetry)
                .as("Attempt %d should have 1h base delay", attempt)
                .isAfterOrEqualTo(before.plus(minDelay))
                .isBeforeOrEqualTo(after.plus(maxDelay));
        }
    }

    @Test
    void veryHighAttemptCountDoesNotOverflow() {
        // Attempt counts beyond MAX_ATTEMPTS should still work (capped at 1h)
        Instant before = Instant.now();
        Instant nextRetry = BackoffCalculator.nextRetryAt(100);
        Instant after = Instant.now();

        long oneHourSeconds = 3600;
        Duration minDelay = Duration.ofMillis((long) (oneHourSeconds * 1000 * 0.8));
        Duration maxDelay = Duration.ofMillis((long) (oneHourSeconds * 1000 * 1.2));

        assertThat(nextRetry).isAfterOrEqualTo(before.plus(minDelay));
        assertThat(nextRetry).isBeforeOrEqualTo(after.plus(maxDelay));
    }

    @Test
    void jitterProducesVariation() {
        // Run multiple times and verify we get different results (statistical test)
        int sampleSize = 20;
        Instant[] results = new Instant[sampleSize];

        for (int i = 0; i < sampleSize; i++) {
            results[i] = BackoffCalculator.nextRetryAt(0);
        }

        // Count unique values - with jitter we should have more than 1
        long uniqueCount = java.util.Arrays.stream(results)
            .distinct()
            .count();

        assertThat(uniqueCount)
            .as("Jitter should produce variation across %d samples", sampleSize)
            .isGreaterThan(1);
    }
}