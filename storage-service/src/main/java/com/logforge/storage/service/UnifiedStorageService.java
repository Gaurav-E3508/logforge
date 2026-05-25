package com.logforge.storage.service;

import com.logforge.common.model.LogEvent;
import com.logforge.storage.cold.ColdStorageManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Single entry point to read from ALL three storage tiers.
 *
 * This implements the K-way merge pattern:
 * Query all three tiers → merge results → sort by timestamp → return.
 *
 * REAL LIFE ANALOGY:
 * The hospital records clerk who can fetch a patient's full history
 * by checking: the nurse's desk (hot) + ward cabinet (warm) +
 * basement archive (cold) — then returning everything in date order.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UnifiedStorageService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final MongoTemplate                 mongoTemplate;
    private final ColdStorageManager            coldStorageManager;

    /**
     * Fetch a single log event by ID — checks hot tier first, then warm.
     */
    public Optional<LogEvent> findById(String eventId, String serviceName) {
        // 1. Check Redis (hot) first — fastest
        String redisKey = "log:" + serviceName + ":" + eventId;
        Object cached = redisTemplate.opsForValue().get(redisKey);
        if (cached instanceof LogEvent event) {
            log.debug("Cache HIT (hot) for eventId: {}", eventId);
            return Optional.of(event);
        }

        // 2. Check MongoDB (warm)
        Query query = new Query(Criteria.where("eventId").is(eventId));
        LogEvent warmResult = mongoTemplate.findOne(query, LogEvent.class, "logs");
        if (warmResult != null) {
            log.debug("Cache HIT (warm) for eventId: {}", eventId);
            return Optional.of(warmResult);
        }

        log.debug("Cache MISS for eventId: {}", eventId);
        return Optional.empty();
    }

    /**
     * Search all three tiers for events in a time range for a given service.
     * Results are merged and sorted by timestamp descending.
     */
    public List<LogEvent> findByServiceAndTimeRange(String serviceName,
                                                    long fromTime,
                                                    long toTime) {
        List<LogEvent> allResults = new ArrayList<>();

        // 1. Warm tier — MongoDB
        try {
            Query query = new Query(
                    Criteria.where("serviceName").is(serviceName)
                            .and("timestamp").gte(fromTime).lte(toTime)
            ).limit(1000);
            allResults.addAll(mongoTemplate.find(query, LogEvent.class, "logs"));
        } catch (Exception e) {
            log.error("Error querying warm tier: {}", e.getMessage());
        }

        // 2. Cold tier — segment files
        try {
            allResults.addAll(
                    coldStorageManager.search(serviceName, fromTime, toTime));
        } catch (Exception e) {
            log.error("Error querying cold tier: {}", e.getMessage());
        }

        // K-way merge — sort all results by timestamp descending
        allResults.sort((a, b) -> Long.compare(b.getTimestamp(), a.getTimestamp()));

        log.debug("Unified search for service '{}' [{},{}] → {} results",
                serviceName, fromTime, toTime, allResults.size());

        return allResults;
    }

    /**
     * Fetch recent events across all services — for the live tail view.
     * Only checks hot + warm tiers (cold is too old for live tail).
     */
    public List<LogEvent> findRecent(int limit) {
        long oneHourAgo = System.currentTimeMillis() - 3_600_000;

        Query query = new Query(
                Criteria.where("timestamp").gte(oneHourAgo)
        ).limit(limit);

        List<LogEvent> results = mongoTemplate.find(query, LogEvent.class, "logs");
        results.sort((a, b) -> Long.compare(b.getTimestamp(), a.getTimestamp()));
        return results;
    }
}