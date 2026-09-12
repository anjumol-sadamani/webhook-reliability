CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE sources (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL,
    event_id_location  TEXT NOT NULL CHECK (event_id_location IN ('body', 'header')),
    event_id_path      TEXT,
    event_id_header    TEXT,
    destination_url    TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_id_rule_matches_location CHECK (
        (event_id_location = 'body'   AND event_id_path   IS NOT NULL AND event_id_header IS NULL)
     OR (event_id_location = 'header' AND event_id_header IS NOT NULL AND event_id_path   IS NULL)
    )
);

CREATE TABLE events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id        UUID NOT NULL REFERENCES sources(id),
    idempotency_key  TEXT NOT NULL,
    body             JSONB NOT NULL,
    published_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, idempotency_key)
);


CREATE INDEX idx_events_unpublished ON events (created_at) WHERE published_at IS NULL;

CREATE TABLE deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt_count   INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'dead_lettered')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);


CREATE INDEX idx_deliveries_due ON deliveries (next_retry_at) WHERE status = 'pending';