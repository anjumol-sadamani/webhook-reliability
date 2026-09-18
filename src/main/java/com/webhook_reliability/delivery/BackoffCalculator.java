package com.webhook_reliability.delivery;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Calculates retry backoff delays per ADR-0002.
 *
 * Schedule: 30s → 1m → 2m → 4m → 8m → 16m → 32m → 1h (capped)
 * Jitter: ±20%
 * Max attempts: 15
 */
public final class BackoffCalculator {

    private static final Duration[] DELAYS = {
        Duration.ofSeconds(30),   // attempt 1
        Duration.ofMinutes(1),    // attempt 2
        Duration.ofMinutes(2),    // attempt 3
        Duration.ofMinutes(4),    // attempt 4
        Duration.ofMinutes(8),    // attempt 5
        Duration.ofMinutes(16),   // attempt 6
        Duration.ofMinutes(32),   // attempt 7
        Duration.ofHours(1),      // attempts 8-15 (capped)
    };

    public static final int MAX_ATTEMPTS = 15;

    private BackoffCalculator() {
    }

    /**
     * Calculate the next retry time based on the current attempt count.
     *
     * @param attemptCount the number of attempts already made (0-based before first attempt)
     * @return the instant when the next retry should occur
     */
    public static Instant nextRetryAt(int attemptCount) {
        int index = Math.min(attemptCount, DELAYS.length - 1);
        Duration baseDelay = DELAYS[index];
        Duration jitteredDelay = applyJitter(baseDelay);
        return Instant.now().plus(jitteredDelay);
    }

    /**
     * Apply ±20% jitter to avoid synchronized retry bursts.
     */
    private static Duration applyJitter(Duration base) {
        double jitterFactor = 0.8 + (ThreadLocalRandom.current().nextDouble() * 0.4); // 0.8 to 1.2
        long jitteredMillis = (long) (base.toMillis() * jitterFactor);
        return Duration.ofMillis(jitteredMillis);
    }
}