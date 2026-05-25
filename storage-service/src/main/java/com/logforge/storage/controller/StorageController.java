package com.logforge.storage.controller;

import com.logforge.common.model.LogEvent;
import com.logforge.storage.service.UnifiedStorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * REST API for querying storage across all three tiers.
 *
 * Examples:
 *   GET /api/storage/logs/payment-service?from=1711600000000&to=1711650000000
 *   GET /api/storage/logs/recent?limit=50
 *   GET /api/storage/logs/{eventId}?service=payment-service
 */
@RestController
@RequestMapping("/api/storage")
@RequiredArgsConstructor
public class StorageController {

    private final UnifiedStorageService storageService;

    @GetMapping("/logs/{serviceName}")
    public ResponseEntity<Map<String, Object>> getByServiceAndRange(
            @PathVariable String serviceName,
            @RequestParam(defaultValue = "0") long from,
            @RequestParam(defaultValue = "0") long to) {

        long toTime   = to   == 0 ? System.currentTimeMillis() : to;
        long fromTime = from == 0 ? toTime - 3_600_000 : from; // default: last hour

        List<LogEvent> events =
                storageService.findByServiceAndTimeRange(serviceName, fromTime, toTime);

        return ResponseEntity.ok(Map.of(
                "service",   serviceName,
                "from",      fromTime,
                "to",        toTime,
                "count",     events.size(),
                "events",    events
        ));
    }

    @GetMapping("/logs/recent")
    public ResponseEntity<List<LogEvent>> getRecent(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(storageService.findRecent(limit));
    }

    @GetMapping("/logs/event/{eventId}")
    public ResponseEntity<?> getById(
            @PathVariable String eventId,
            @RequestParam String service) {

        return storageService.findById(eventId, service)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}