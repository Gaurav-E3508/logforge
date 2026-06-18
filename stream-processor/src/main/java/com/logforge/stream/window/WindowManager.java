package com.logforge.stream.window;

import com.logforge.common.model.LogEvent;
import com.logforge.common.zookeeper.LeaderElectionService;
import com.logforge.stream.pattern.PatternDetector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class WindowManager {

    private final ConcurrentHashMap<String, SlidingWindow> windows
            = new ConcurrentHashMap<>();

    private final PatternDetector       patternDetector;
    private final LeaderElectionService leaderElection;
    private final int windowSizeMinutes;

    public WindowManager(PatternDetector patternDetector,
                         LeaderElectionService leaderElection,
                         @Value("${logforge.stream.window-size-minutes:5}")
                         int windowSizeMinutes) {
        this.patternDetector  = patternDetector;
        this.leaderElection   = leaderElection;
        this.windowSizeMinutes = windowSizeMinutes;
    }

    public void record(LogEvent event) {
        String service = event.getServiceName();
        if (service == null || service.isBlank()) return;

        windows.computeIfAbsent(service,
                        s -> new SlidingWindow(s, windowSizeMinutes))
                .record(event);
    }

    /**
     * Runs every minute — but ONLY the elected leader performs
     * aggregation and pattern detection. This prevents duplicate
     * alerts/logs if multiple stream-processor instances run for HA.
     */
    @Scheduled(fixedDelayString =
            "${logforge.stream.eviction-interval-ms:60000}")
    public void evictAndAggregate() {
        if (windows.isEmpty()) return;

        if (!leaderElection.isLeader()) {
            log.debug("Not leader — skipping aggregation cycle (instanceId: {})",
                    leaderElection.getInstanceId());
            return;
        }

        log.debug("Running window eviction + aggregation (LEADER, instanceId: {})",
                leaderElection.getInstanceId());

        windows.forEach((service, window) -> {
            window.evictExpiredBuckets();
            WindowSummary summary = window.summarize();

            List<PatternDetector.DetectedPattern> patterns =
                    patternDetector.detect(summary, window.getBuckets());

            if (!patterns.isEmpty()) {
                patterns.forEach(p ->
                        log.warn("[PATTERN DETECTED] type={} service={} value={} — {}",
                                p.type(), p.serviceName(), p.value(), p.description())
                );
            }
        });
    }

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