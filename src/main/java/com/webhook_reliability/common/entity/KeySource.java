package com.webhook_reliability.common.entity;

public enum KeySource {
    BODY,   // Extract idempotency key from JSON body using JSONPath
    HEADER  // Extract idempotency key from HTTP header
}