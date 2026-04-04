package com.logforge.ingestion.pipeline.steps;

import com.logforge.common.model.LogEvent;
import com.logforge.ingestion.pipeline.PipelineStep;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Step 3: Enrich the event with country/city from the source IP.
 *
 * In production you'd use MaxMind GeoLite2 database for real lookups.
 * For now we use a simple prefix-based mock that covers local dev scenarios.
 *
 * The real implementation would:
 * 1. Load the GeoLite2-City.mmdb file on startup into a Trie structure
 * 2. For each IP, traverse the Trie to find the matching subnet
 * 3. Return country + city metadata
 *
 * We'll upgrade this to a real Trie-based lookup in Step 5 (Index Engine).
 */
@Slf4j
@Component
public class GeoIpEnrichmentStep implements PipelineStep {

    // Mock IP → country mapping for local dev
    // Real implementation: MaxMind GeoLite2 database
    private static final Map<String, String[]> GEO_MOCK = new HashMap<>();

    static {
        GEO_MOCK.put("10.",       new String[]{"IN", "Mumbai"});
        GEO_MOCK.put("192.168.",  new String[]{"IN", "Pune"});
        GEO_MOCK.put("172.",      new String[]{"US", "New York"});
        GEO_MOCK.put("52.",       new String[]{"US", "Virginia"});
        GEO_MOCK.put("35.",       new String[]{"US", "Oregon"});
        GEO_MOCK.put("13.",       new String[]{"SG", "Singapore"});
    }

    @Override
    public LogEvent process(LogEvent event) {
        // Skip if no IP, or already has country info, or it's localhost
        if (event.getSourceIp() == null
                || event.getCountry() != null
                || event.getSourceIp().equals("127.0.0.1")
                || event.getSourceIp().equals("0:0:0:0:0:0:0:1")) {
            return event;
        }

        String[] geo = lookupGeo(event.getSourceIp());

        // Rebuild with geo info — LogEvent is immutable via @Builder
        return LogEvent.builder()
                .eventId(event.getEventId())
                .timestamp(event.getTimestamp())
                .ingestedAt(event.getIngestedAt())
                .level(event.getLevel())
                .serviceName(event.getServiceName())
                .hostName(event.getHostName())
                .environment(event.getEnvironment())
                .category(event.getCategory())
                .message(event.getMessage())
                .stackTrace(event.getStackTrace())
                .traceId(event.getTraceId())
                .spanId(event.getSpanId())
                .sourceIp(event.getSourceIp())
                .country(geo[0])
                .city(geo[1])
                .tags(event.getTags())
                .kafkaPartition(event.getKafkaPartition())
                .kafkaOffset(event.getKafkaOffset())
                .storageTier(event.getStorageTier())
                .build();
    }

    private String[] lookupGeo(String ip) {
        for (Map.Entry<String, String[]> entry : GEO_MOCK.entrySet()) {
            if (ip.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return new String[]{"UNKNOWN", "UNKNOWN"};
    }

    @Override public String stepName() { return "GEO_IP_ENRICHMENT"; }
}