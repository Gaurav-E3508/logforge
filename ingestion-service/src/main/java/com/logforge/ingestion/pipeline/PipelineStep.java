package com.logforge.ingestion.pipeline;

import com.logforge.common.model.LogEvent;

/**
 * Contract for every step in the ingestion pipeline.
 * Each step receives a LogEvent, does its work, and either
 * returns the (possibly modified) event or null to drop it.
 */
public interface PipelineStep {
    /**
     * @return the processed event, or null to drop it from the pipeline
     */
    LogEvent process(LogEvent event);

    String stepName();
}