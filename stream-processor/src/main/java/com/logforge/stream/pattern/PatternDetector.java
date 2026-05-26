package com.logforge.stream.pattern;

import com.logforge.stream.window.WindowBucket;
import com.logforge.stream.window.WindowSummary;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Detects patterns in window data that indicate problems.
 *
 * Patterns detected:
 * 1. ERROR_SPIKE      — sudden jump in error count
 * 2. HIGH_ERROR_RATE  — error% exceeds threshold
 * 3. FATAL_DETECTED   — any FATAL event (always alert-worthy)
 * 4. VOLUME_SPIKE     — total log volume suddenly much higher than normal
 *    (could indicate a loop or runaway process)
 *
 * REAL LIFE ANALOGY:
 * The traffic operator has rules:
 * - "Alert me if accidents exceed 10 in one minute" (ERROR_SPIKE)
 * - "Alert me if >50% of events are accidents" (HIGH_ERROR_RATE)
 * - "Alert me IMMEDIATELY for any fatality" (FATAL_DETECTED)
 */
@Slf4j
@Component
public class PatternDetector {

    @Value("${logforge.stream.error-spike-threshold:10}")
    private long errorSpikeThreshold;

    @Value("${logforge.stream.error-rate-threshold-percent:50}")
    private long errorRateThreshold;

    /**
     * Analyse a window summary and return any detected patterns.
     */
    public List<DetectedPattern> detect(WindowSummary summary,
                                        Collection<WindowBucket> buckets) {
        List<DetectedPattern> patterns = new ArrayList<>();

        // Pattern 1: High overall error rate
        if (summary.getErrorRatePercent() >= errorRateThreshold
                && summary.getTotalEvents() > 5) {
            patterns.add(new DetectedPattern(
                    PatternType.HIGH_ERROR_RATE,
                    summary.getServiceName(),
                    String.format("Error rate is %d%% over last %d minutes " +
                                    "(threshold: %d%%)",
                            summary.getErrorRatePercent(),
                            summary.getWindowSizeMinutes(),
                            errorRateThreshold),
                    summary.getErrorRatePercent()
            ));
        }

        // Pattern 2: Error spike in a single bucket (one bad minute)
        for (WindowBucket bucket : buckets) {
            long bucketErrors = bucket.getErrorCount().get();
            if (bucketErrors >= errorSpikeThreshold) {
                patterns.add(new DetectedPattern(
                        PatternType.ERROR_SPIKE,
                        summary.getServiceName(),
                        String.format("%d errors in a single minute " +
                                        "(threshold: %d)",
                                bucketErrors, errorSpikeThreshold),
                        bucketErrors
                ));
                break; // one spike alert per window is enough
            }
        }

        // Pattern 3: Any FATAL event — always serious
        if (summary.getTotalFatal() > 0) {
            patterns.add(new DetectedPattern(
                    PatternType.FATAL_DETECTED,
                    summary.getServiceName(),
                    String.format("%d FATAL events detected in last %d minutes",
                            summary.getTotalFatal(),
                            summary.getWindowSizeMinutes()),
                    summary.getTotalFatal()
            ));
        }

        // Pattern 4: Volume spike — more than 10x the normal rate
        // (simple heuristic: if one bucket has >10x the average of others)
        if (buckets.size() > 2) {
            double avgVolume = buckets.stream()
                    .mapToLong(b -> b.getTotalCount().get())
                    .average().orElse(0);
            for (WindowBucket bucket : buckets) {
                if (bucket.getTotalCount().get() > avgVolume * 10
                        && avgVolume > 5) {
                    patterns.add(new DetectedPattern(
                            PatternType.VOLUME_SPIKE,
                            summary.getServiceName(),
                            String.format("Volume spike: %d events in one minute" +
                                            " vs avg %.1f/min",
                                    bucket.getTotalCount().get(), avgVolume),
                            bucket.getTotalCount().get()
                    ));
                    break;
                }
            }
        }

        if (!patterns.isEmpty()) {
            log.warn("Patterns detected for service '{}': {}",
                    summary.getServiceName(), patterns.size());
        }

        return patterns;
    }

    public enum PatternType {
        ERROR_SPIKE, HIGH_ERROR_RATE, FATAL_DETECTED, VOLUME_SPIKE
    }

    public record DetectedPattern(
            PatternType type,
            String      serviceName,
            String      description,
            long        value
    ) {}
}