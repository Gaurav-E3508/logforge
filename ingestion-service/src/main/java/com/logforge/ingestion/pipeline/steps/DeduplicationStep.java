package com.logforge.ingestion.pipeline.steps;

import com.logforge.common.model.LogEvent;
import com.logforge.ingestion.dedup.BloomFilter;
import com.logforge.ingestion.pipeline.PipelineStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 1: Drop duplicate events using the Bloom Filter.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeduplicationStep implements PipelineStep {

    private final BloomFilter bloomFilter;

    @Override
    public LogEvent process(LogEvent event) {
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            log.warn("Event missing eventId — skipping deduplication check");
            return event;
        }

        if (bloomFilter.mightContain(event.getEventId())) {
            log.debug("Duplicate event detected, dropping: {}", event.getEventId());
            return null; // DROP — null signals pipeline to discard this event
        }

        bloomFilter.put(event.getEventId());
        return event;
    }

    @Override public String stepName() { return "DEDUPLICATION"; }
}