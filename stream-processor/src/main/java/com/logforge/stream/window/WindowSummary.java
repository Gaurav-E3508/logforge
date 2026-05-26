package com.logforge.stream.window;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Snapshot of aggregated metrics for one service over the window period.
 * Published to the METRICS Kafka topic every minute.
 */
@Data
@AllArgsConstructor
public class WindowSummary {
    private String serviceName;
    private long   timestamp;
    private long   windowSizeMinutes;
    private long   totalEvents;
    private long   totalErrors;
    private long   totalWarns;
    private long   totalFatal;
    private long   errorRatePercent;  // 0-100
    private int    activeBuckets;
}