package com.logforge.stream.window;

import com.logforge.common.model.LogEvent;
import com.logforge.stream.pattern.PatternDetector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages all sliding windows — one per service.
 *
 * Structure: ConcurrentHashMap<serviceName, SlidingWindow>
 *
 * On every log event:
 *   1. Find (or create) the window for that service
 *   2. Record the event into the appropriate bucket
 *
 * Every minute (scheduled):
 *   1. Evict expired buckets from all windows
 *   2. Summarize each window
 *   3. Run pattern detection
 *   4. Publish metrics to Kafka
 */
@Slf4j
@Component
public class WindowManager {

    // serviceName → SlidingWindow
    private final ConcurrentHashMap<String, SlidingWindow> windows
            = new ConcurrentHashMap<>();

    private final PatternDetector patternDetector;
    private final int windowSizeMinutes;

    public WindowManager(PatternDetector patternDetector,
                         @Value("${logforge.stream.window-size-minutes:5}")
                         int windowSizeMinutes) {
        this.patternDetector  = patternDetector;
        this.windowSizeMinutes = windowSizeMinutes;
    }

    /**
     * Record a log event — called by the Kafka consumer for every event.
     */
    public void record(LogEvent event) {
        String service = event.getServiceName();
        if (service == null || service.isBlank()) return;

        windows.computeIfAbsent(service,
                        s -> new SlidingWindow(s, windowSizeMinutes))
                .record(event);
    }

    /**
     * Runs every minute — evicts old buckets, summarizes, detects patterns.
     */
    @Scheduled(fixedDelayString =
            "${logforge.stream.eviction-interval-ms:60000}")
    public void evictAndAggregate() {
        if (windows.isEmpty()) return;

        log.debug("Running window eviction + aggregation for {} services",
                windows.size());

        windows.forEach((service, window) -> {
            // 1. Evict expired buckets
            window.evictExpiredBuckets();

            // 2. Summarize the window
            WindowSummary summary = window.summarize();

            // 3. Detect patterns
            List<PatternDetector.DetectedPattern> patterns =
                    patternDetector.detect(summary, window.getBuckets());

            if (!patterns.isEmpty()) {
                patterns.forEach(p ->
                        log.warn("[PATTERN DETECTED] type={} service={} value={} — {}",
                                p.type(), p.serviceName(), p.value(), p.description())
                );
            }

            log.debug("Window summary — service={} events={} errors={} errorRate={}%",
                    service, summary.getTotalEvents(),
                    summary.getTotalErrors(), summary.getErrorRatePercent());
        });
    }

    /**
     * Get a snapshot of all current window summaries.
     * Used by the REST endpoint.
     */
    public List<WindowSummary> getAllSummaries() {
        return windows.values().stream()
                .map(SlidingWindow::summarize)
                .sorted(Comparator.comparingLong(
                        WindowSummary::getTotalErrors).reversed())
                .collect(Collectors.toList());
    }

    public WindowSummary getSummary(String serviceName) {
        SlidingWindow window = windows.get(serviceName);
        return window != null ? window.summarize() : null;
    }

    public int activeServiceCount() { return windows.size(); }
}