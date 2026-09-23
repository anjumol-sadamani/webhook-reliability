package com.webhook_reliability.common.repository;

import com.webhook_reliability.common.entity.Delivery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class DeliveryRepository {

    private final JdbcClient jdbc;

    private static final RowMapper<Delivery> ROW_MAPPER = (rs, rowNum) -> new Delivery(
        rs.getObject("id", UUID.class),
        rs.getObject("event_id", UUID.class),
        rs.getTimestamp("next_retry_at").toInstant(),
        rs.getInt("attempt_count"),
        rs.getString("status"),
        rs.getObject("last_status_code", Integer.class),
        rs.getString("last_error"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("updated_at").toInstant()
    );

    public DeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Delivery save(Delivery delivery) {
        return jdbc.sql("""
            INSERT INTO deliveries (event_id)
            VALUES (:eventId)
            RETURNING *
            """)
            .param("eventId", delivery.eventId())
            .query(ROW_MAPPER)
            .single();
    }

    public Optional<Delivery> findByEventId(UUID eventId) {
        return jdbc.sql("SELECT * FROM deliveries WHERE event_id = :eventId")
            .param("eventId", eventId)
            .query(ROW_MAPPER)
            .optional();
    }

    public List<Delivery> findDue(int limit) {
        return jdbc.sql("""
            SELECT * FROM deliveries
            WHERE status = 'pending' AND next_retry_at <= now()
            ORDER BY next_retry_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """)
            .param("limit", limit)
            .query(ROW_MAPPER)
            .list();
    }

    public void markDelivered(UUID id, int statusCode) {
        jdbc.sql("""
            UPDATE deliveries
            SET status = 'delivered',
                last_status_code = :statusCode,
                last_error = NULL,
                updated_at = now()
            WHERE id = :id
            """)
            .param("id", id)
            .param("statusCode", statusCode)
            .update();
    }

    public void scheduleRetry(UUID id, Instant nextRetryAt, int statusCode, String error) {
        jdbc.sql("""
            UPDATE deliveries
            SET next_retry_at = :nextRetryAt,
                attempt_count = attempt_count + 1,
                last_status_code = :statusCode,
                last_error = :error,
                updated_at = now()
            WHERE id = :id
            """)
            .param("id", id)
            .param("nextRetryAt", Timestamp.from(nextRetryAt))
            .param("statusCode", statusCode)
            .param("error", error)
            .update();
    }

    public void markFailed(UUID id) {
        jdbc.sql("""
            UPDATE deliveries
            SET status = 'failed',
                updated_at = now()
            WHERE id = :id
            """)
            .param("id", id)
            .update();
    }
}