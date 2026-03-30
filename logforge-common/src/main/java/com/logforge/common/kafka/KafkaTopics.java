package com.logforge.common.kafka;

/**
 * Central registry of all Kafka topic names used in LogForge.
 *
 * Think of Kafka topics like different conveyor belt lanes:
 * - RAW_LOGS: everything comes in here first (unprocessed)
 * - PROCESSED_LOGS: cleaned, enriched, validated events
 * - ALERTS: fired alert notifications going to Slack/email
 * - METRICS: aggregated window metrics from Stream Processor
 * - DEAD_LETTER: events that failed processing (for investigation)
 */
public final class KafkaTopics {

    private KafkaTopics() {} // prevent instantiation

    /** All raw logs from every service arrive here first */
    public static final String RAW_LOGS = "logforge.raw-logs";

    /** After ingestion pipeline — deduplicated, enriched, classified */
    public static final String PROCESSED_LOGS = "logforge.processed-logs";

    /** Alert notifications — consumed by Alert Engine for fanout */
    public static final String ALERTS = "logforge.alerts";

    /** Aggregated metrics from Stream Processor sliding windows */
    public static final String METRICS = "logforge.metrics";

    /**
     * Events that failed validation or processing.
     * Never silently drop a bad event — park it here for investigation.
     */
    public static final String DEAD_LETTER = "logforge.dead-letter";

    // Topic configuration constants
    public static final int RAW_LOGS_PARTITIONS = 12;     // matches ingestion service thread count
    public static final int PROCESSED_LOGS_PARTITIONS = 6;
    public static final int DEFAULT_REPLICATION_FACTOR = 1; // local dev — use 3 in production
}