package com.webhook_reliability.common.repository;

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
        rs.getString("event_id_location"),
        rs.getString("event_id_path"),
        rs.getString("event_id_header"),
        rs.getString("destination_url"),
        rs.getTimestamp("created_at").toInstant()
    );

    public SourceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Source save(Source source) {
        return jdbc.sql("""
            INSERT INTO sources (name, event_id_location, event_id_path, event_id_header, destination_url)
            VALUES (:name, :eventIdLocation, :eventIdPath, :eventIdHeader, :destinationUrl)
            RETURNING *
            """)
            .param("name", source.name())
            .param("eventIdLocation", source.eventIdLocation())
            .param("eventIdPath", source.eventIdPath())
            .param("eventIdHeader", source.eventIdHeader())
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
