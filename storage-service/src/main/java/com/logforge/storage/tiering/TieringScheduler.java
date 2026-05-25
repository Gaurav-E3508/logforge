package com.logforge.storage.tiering;

import com.logforge.common.model.LogEvent;
import com.logforge.storage.cold.ColdStorageManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Runs on a schedule to move data between storage tiers.
 *
 * TIERING RULES:
 * Hot  (Redis)   → expires automatically via TTL (1 hour) — no action needed
 * Warm (MongoDB) → events older than 30 days move to Cold
 * Cold (disk)    → segments older than 1 year are deleted
 *
 * REAL LIFE ANALOGY:
 * A hospital records clerk who comes in every night:
 * 1. Takes patient files older than 30 days from the ward cabinet
 * 2. Boxes them up and moves them to the basement archive
 * 3. Shreds archive boxes older than 1 year (legal retention limit)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TieringScheduler {

    private final MongoTemplate       mongoTemplate;
    private final ColdStorageManager  coldStorageManager;

    private static final long WARM_TTL_MS  = 30L * 24 * 60 * 60 * 1000; // 30 days
    private static final int  BATCH_SIZE   = 1000; // events per archive batch

    /**
     * Runs every hour (configurable via logforge.storage.tiering-interval-ms).
     * Moves events older than 30 days from MongoDB to cold segment files.
     */
    @Scheduled(fixedDelayString =
            "${logforge.storage.tiering-interval-ms:3600000}")
    public void tierWarmToCold() {
        log.info("Starting warm → cold tiering job");
        long cutoffTime = System.currentTimeMillis() - WARM_TTL_MS;

        // Find all events in MongoDB older than 30 days
        Query query = new Query(
                Criteria.where("timestamp").lt(cutoffTime)
        ).limit(BATCH_SIZE);

        List<LogEvent> oldEvents = mongoTemplate.find(query, LogEvent.class, "logs");

        if (oldEvents.isEmpty()) {
            log.info("Tiering job complete — no events to move to cold storage");
            return;
        }

        // Group by service name — one segment file per service per batch
        Map<String, List<LogEvent>> byService = oldEvents.stream()
                .collect(Collectors.groupingBy(e ->
                        e.getServiceName() != null
                                ? e.getServiceName() : "unknown"));

        // Archive each service's events to cold storage
        byService.forEach((service, events) -> {
            coldStorageManager.archive(service, events);
            log.info("Moved {} events for service '{}' from warm → cold",
                    events.size(), service);
        });

        // Remove archived events from MongoDB
        Query deleteQuery = new Query(
                Criteria.where("timestamp").lt(cutoffTime)
        ).limit(BATCH_SIZE);
        mongoTemplate.remove(deleteQuery, LogEvent.class, "logs");

        log.info("Tiering job complete — archived {} events, " +
                        "cold segments total: {}",
                oldEvents.size(), coldStorageManager.getTotalSegmentCount());
    }

    /**
     * Runs once a day — cleans up cold segments older than 1 year.
     */
    @Scheduled(cron = "0 0 2 * * *") // 2 AM every day
    public void deleteExpiredColdSegments() {
        log.info("Starting cold storage cleanup job");
        coldStorageManager.deleteExpiredSegments();
    }
}