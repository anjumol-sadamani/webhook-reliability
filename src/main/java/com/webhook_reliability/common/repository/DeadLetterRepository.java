package com.webhook_reliability.common.repository;

import com.webhook_reliability.common.entity.DeadLetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class DeadLetterRepository {

    private final JdbcClient jdbc;

    private static final RowMapper<DeadLetter> ROW_MAPPER = (rs, rowNum) -> new DeadLetter(
        rs.getObject("id", UUID.class),
        rs.getObject("event_id", UUID.class),
        rs.getString("reason"),
        rs.getString("topic"),
        rs.getObject("partition_num", Integer.class),
        rs.getObject("offset_num", Long.class),
        rs.getString("raw_payload"),
        rs.getObject("last_status_code", Integer.class),
        rs.getString("last_error"),
        rs.getInt("attempt_count"),
        rs.getTimestamp("created_at").toInstant()
    );

    public DeadLetterRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public DeadLetter save(DeadLetter deadLetter) {
        return jdbc.sql("""
            INSERT INTO dead_letters (
                event_id, reason, topic, partition_num, offset_num,
                raw_payload, last_status_code, last_error, attempt_count
            ) VALUES (
                :eventId, :reason, :topic, :partitionNum, :offsetNum,
                :rawPayload, :lastStatusCode, :lastError, :attemptCount
            )
            RETURNING *
            """)
            .param("eventId", deadLetter.eventId())
            .param("reason", deadLetter.reason())
            .param("topic", deadLetter.topic())
            .param("partitionNum", deadLetter.partitionNum())
            .param("offsetNum", deadLetter.offsetNum())
            .param("rawPayload", deadLetter.rawPayload())
            .param("lastStatusCode", deadLetter.lastStatusCode())
            .param("lastError", deadLetter.lastError())
            .param("attemptCount", deadLetter.attemptCount())
            .query(ROW_MAPPER)
            .single();
    }

    public List<DeadLetter> findByReason(String reason, int limit) {
        return jdbc.sql("""
            SELECT * FROM dead_letters
            WHERE reason = :reason
            ORDER BY created_at DESC
            LIMIT :limit
            """)
            .param("reason", reason)
            .param("limit", limit)
            .query(ROW_MAPPER)
            .list();
    }

    public List<DeadLetter> findRecent(int limit) {
        return jdbc.sql("""
            SELECT * FROM dead_letters
            ORDER BY created_at DESC
            LIMIT :limit
            """)
            .param("limit", limit)
            .query(ROW_MAPPER)
            .list();
    }
}