-- Dead letters table for all permanent failures
-- Single source of truth for "what failed and why"

CREATE TABLE dead_letters (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    event_id        UUID REFERENCES events(id),  -- NULL if deserialization failed
    reason          TEXT NOT NULL CHECK (reason IN ('DESERIALIZATION', 'MAX_RETRIES_EXCEEDED', 'SOURCE_NOT_FOUND')),

    -- Kafka coordinates (for replay/debugging)
    topic           TEXT,
    partition_num   INT,
    offset_num      BIGINT,

    -- Payload/error details
    raw_payload     TEXT,                        -- original message (for deserialization failures)
    last_status_code INT,                        -- last HTTP status (for max retries)
    last_error      TEXT,
    attempt_count   INT NOT NULL DEFAULT 0,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dead_letters_event_id ON dead_letters (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX idx_dead_letters_reason ON dead_letters (reason, created_at);

-- Rename 'dead_lettered' status to 'failed' in deliveries
ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'delivered', 'failed'));

-- Update any existing dead_lettered rows to failed
UPDATE deliveries SET status = 'failed' WHERE status = 'dead_lettered';