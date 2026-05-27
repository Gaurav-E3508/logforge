package com.logforge.stream.controller;

import com.logforge.stream.window.WindowManager;
import com.logforge.stream.window.WindowSummary;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API for real-time window metrics.
 *
 * Examples:
 *   GET /api/metrics              — all services
 *   GET /api/metrics/payment-service — one service
 *   GET /api/metrics/stats        — system-wide stats
 */
@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final WindowManager windowManager;

    @GetMapping
    public ResponseEntity<List<WindowSummary>> getAllMetrics() {
        return ResponseEntity.ok(windowManager.getAllSummaries());
    }

    @GetMapping("/{serviceName}")
    public ResponseEntity<?> getServiceMetrics(
            @PathVariable String serviceName) {

        WindowSummary summary = windowManager.getSummary(serviceName);
        if (summary == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(summary);
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        List<WindowSummary> all = windowManager.getAllSummaries();

        long totalErrors = all.stream()
                .mapToLong(WindowSummary::getTotalErrors).sum();
        long totalEvents = all.stream()
                .mapToLong(WindowSummary::getTotalEvents).sum();

        return ResponseEntity.ok(Map.of(
                "activeServices",  windowManager.activeServiceCount(),
                "totalEventsInWindow", totalEvents,
                "totalErrorsInWindow", totalErrors,
                "topErrorService", all.isEmpty() ? "none"
                        : all.get(0).getServiceName()
        ));
    }
}