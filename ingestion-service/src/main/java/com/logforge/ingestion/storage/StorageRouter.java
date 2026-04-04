package com.logforge.ingestion.storage;

import com.logforge.common.model.LogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Routes processed events to the correct storage tiers.
 *
 * Every event goes to BOTH Redis (hot) and MongoDB (warm).
 * Redis acts as a fast cache; MongoDB is the durable store.
 *
 * Storage key in Redis: "log:{serviceName}:{eventId}"
 * MongoDB collection: "logs"
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StorageRouter {

    private final RedisTemplate<String, Object> redisTemplate;
    private final MongoTemplate                 mongoTemplate;

    @Value("${logforge.ingestion.hot-ttl-seconds:3600}")
    private long hotTtlSeconds;

    public void route(LogEvent event) {
        // Mark storage tier as HOT initially
        LogEvent hotEvent = withTier(event, LogEvent.StorageTier.HOT);

        // 1. Store in Redis (hot tier — expires after 1 hour)
        storeInRedis(hotEvent);

        // 2. Store in MongoDB (warm tier — persists for 30 days)
        storeInMongo(hotEvent);
    }

    private void storeInRedis(LogEvent event) {
        try {
            String key = "log:" + event.getServiceName() + ":" + event.getEventId();
            redisTemplate.opsForValue()
                    .set(key, event, hotTtlSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Failed to store event in Redis — eventId: {}, error: {}",
                    event.getEventId(), e.getMessage());
            // Don't rethrow — Redis failure shouldn't block MongoDB write
        }
    }

    private void storeInMongo(LogEvent event) {
        try {
            mongoTemplate.save(event, "logs");
        } catch (Exception e) {
            log.error("Failed to store event in MongoDB — eventId: {}, error: {}",
                    event.getEventId(), e.getMessage());
        }
    }

    private LogEvent withTier(LogEvent event, LogEvent.StorageTier tier) {
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
                .country(event.getCountry())
                .city(event.getCity())
                .tags(event.getTags())
                .kafkaPartition(event.getKafkaPartition())
                .kafkaOffset(event.getKafkaOffset())
                .storageTier(tier)
                .build();
    }
}