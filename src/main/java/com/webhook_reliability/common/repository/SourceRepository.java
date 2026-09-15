package com.webhook_reliability.common.repository;

import com.webhook_reliability.common.entity.KeySource;
import com.webhook_reliability.common.entity.Source;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public class SourceRepository {

    private final JdbcClient jdbc;

    private static final RowMapper<Source> ROW_MAPPER = (rs, rowNum) -> new Source(
        rs.getObject("id", UUID.class),
        rs.getString("name"),
        KeySource.valueOf(rs.getString("event_id_source")),
        rs.getString("event_id_path"),
        rs.getString("destination_url"),
        rs.getTimestamp("created_at").toInstant()
    );

    public SourceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Source save(Source source) {
        return jdbc.sql("""
            INSERT INTO sources (name, event_id_source, event_id_path, destination_url)
            VALUES (:name, :eventIdSource, :eventIdPath, :destinationUrl)
            RETURNING *
            """)
            .param("name", source.name())
            .param("eventIdSource", source.eventIdSource().name())
            .param("eventIdPath", source.eventIdPath())
            .param("destinationUrl", source.destinationUrl())
            .query(ROW_MAPPER)
            .single();
    }

    public Optional<Source> findById(UUID id) {
        return jdbc.sql("SELECT * FROM sources WHERE id = :id")
            .param("id", id)
            .query(ROW_MAPPER)
            .optional();
    }
}