package com.webhook_reliability.delivery;

/**
 * Result of a webhook delivery attempt.
 */
public record DeliveryResult(
    boolean success,
    int statusCode,
    String error
) {
    public static DeliveryResult success(int statusCode) {
        return new DeliveryResult(true, statusCode, null);
    }

    public static DeliveryResult failure(int statusCode, String error) {
        return new DeliveryResult(false, statusCode, truncateError(error));
    }

    private static String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 1024 ? error.substring(0, 1024) : error;
    }
}