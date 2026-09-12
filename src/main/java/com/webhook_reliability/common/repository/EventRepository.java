package com.webhook_reliability.common.repository;

import com.webhook_reliability.common.entity.Event;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class EventRepository {

    private final JdbcClient jdbc;

    private static final RowMapper<Event> ROW_MAPPER = (rs, rowNum) -> {
        Timestamp publishedAt = rs.getTimestamp("published_at");
        return new Event(
            rs.getObject("id", UUID.class),
            rs.getObject("source_id", UUID.class),
            rs.getString("idempotency_key"),
            rs.getString("body"),
            publishedAt != null ? publishedAt.toInstant() : null,
            rs.getTimestamp("created_at").toInstant()
        );
    };

    public EventRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Event save(Event event) {
        return jdbc.sql("""
            INSERT INTO events (source_id, idempotency_key, body)
            VALUES (:sourceId, :idempotencyKey, :body::jsonb)
            RETURNING *
            """)
            .param("sourceId", event.sourceId())
            .param("idempotencyKey", event.idempotencyKey())
            .param("body", event.body())
            .query(ROW_MAPPER)
            .single();
    }

    public Optional<Event> findById(UUID id) {
        return jdbc.sql("SELECT * FROM events WHERE id = :id")
            .param("id", id)
            .query(ROW_MAPPER)
            .optional();
    }

    public Optional<Event> findBySourceIdAndIdempotencyKey(UUID sourceId, String idempotencyKey) {
        return jdbc.sql("SELECT * FROM events WHERE source_id = :sourceId AND idempotency_key = :idempotencyKey")
            .param("sourceId", sourceId)
            .param("idempotencyKey", idempotencyKey)
            .query(ROW_MAPPER)
            .optional();
    }

    public List<Event> findUnpublished(int limit) {
        return jdbc.sql("""
            SELECT * FROM events
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """)
            .param("limit", limit)
            .query(ROW_MAPPER)
            .list();
    }

    public void markPublished(UUID id) {
        jdbc.sql("UPDATE events SET published_at = now() WHERE id = :id")
            .param("id", id)
            .update();
    }
}
