package com.webhook_reliability.ingestion.service;

import com.webhook_reliability.common.entity.Source;
import com.webhook_reliability.common.repository.SourceRepository;
import com.webhook_reliability.ingestion.dto.CreateSourceRequest;
import org.springframework.stereotype.Service;

@Service
public class SourceService {

    private final SourceRepository sourceRepository;

    public SourceService(SourceRepository sourceRepository) {
        this.sourceRepository = sourceRepository;
    }

    public Source create(CreateSourceRequest request) {
        Source source = Source.create(
            request.name(),
            request.eventIdSource(),
            request.eventIdPath(),
            request.destinationUrl()
        );
        return sourceRepository.save(source);
    }
}