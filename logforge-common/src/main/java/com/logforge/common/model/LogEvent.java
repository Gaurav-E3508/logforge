package com.logforge.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * The core data structure of LogForge.
 * Every log event in the system is represented as this object —
 * from capture in the SDK, through Kafka, ingestion, storage, to search.
 */
@Data                          // Lombok: generates getters, setters, equals, hashCode, toString
@Builder                       // Lombok: enables LogEvent.builder().level("ERROR").build()
@NoArgsConstructor             // Lombok: generates empty constructor (required by Jackson)
@AllArgsConstructor            // Lombok: generates constructor with all fields
@JsonInclude(JsonInclude.Include.NON_NULL)  // Don't serialize null fields to JSON
public class LogEvent {

    // --- Identity ---

    /** Unique ID for this log event — used by Bloom Filter for deduplication */
    private String eventId;

    /** When this event occurred — epoch milliseconds */
    private long timestamp;

    /** When LogForge received this event — for lag tracking */
    private long ingestedAt;

    // --- Classification ---

    /** Log level: TRACE, DEBUG, INFO, WARN, ERROR, FATAL */
    private LogLevel level;

    /** Which microservice produced this log */
    private String serviceName;

    /** Which server/pod/container produced this log */
    private String hostName;

    /** PRODUCTION, STAGING, DEVELOPMENT */
    private Environment environment;

    /** Assigned by the Ingestion Service classifier */
    private String category;

    // --- Content ---

    /** The actual log message — this is what gets indexed for full-text search */
    private String message;

    /** Full stack trace — only present for ERROR/FATAL levels */
    private String stackTrace;

    // --- Distributed Tracing ---

    /**
     * Trace ID — ties together all logs from a single user request
     * across multiple services (e.g., one payment flow touches
     * payment-service, fraud-service, notification-service).
     * All three share the same traceId.
     */
    private String traceId;

    /** Span ID — identifies this specific operation within a trace */
    private String spanId;

    // --- Network / Geo ---

    /** IP address of the service that sent this log */
    private String sourceIp;

    /** Country code resolved from IP by Geo-IP enrichment (e.g., "IN", "US") */
    private String country;

    /** City resolved from IP */
    private String city;

    // --- Custom Data ---

    /**
     * Arbitrary key-value pairs the developer wants to attach.
     * Example: {"userId": "u_991", "orderId": "ord_4421", "amount": "1500"}
     * This is what makes LogForge flexible — any service can attach
     * its own context without changing the schema.
     */
    private Map<String, String> tags;

    // --- Processing Metadata (set by Ingestion Service) ---

    /** Which Kafka partition this event came from */
    private int kafkaPartition;

    /** Kafka offset — for replay and exactly-once tracking */
    private long kafkaOffset;

    /** Which storage tier this event lives in: HOT, WARM, COLD */
    private StorageTier storageTier;


    // -------------------------
    // Enums defined inside the class for cohesion
    // -------------------------

    public enum LogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR, FATAL
    }

    public enum Environment {
        DEVELOPMENT, STAGING, PRODUCTION
    }

    public enum StorageTier {
        HOT,    // Redis — last 1 hour
        WARM,   // MongoDB — last 30 days
        COLD    // Segment files — up to 1 year
    }
}