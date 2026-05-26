package com.logforge.stream.window;

import com.logforge.common.model.LogEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A sliding window over log events for one service.
 *
 * Uses ConcurrentSkipListMap (sorted by bucket timestamp) so:
 * - Multiple threads can add events concurrently (thread-safe)
 * - Eviction of old buckets is O(log n) — just remove from head
 * - Iteration for aggregation is in time order
 *
 * WHY ConcurrentSkipListMap over ConcurrentHashMap?
 * ConcurrentHashMap has O(1) get/put but is unordered.
 * ConcurrentSkipListMap is ordered by key (timestamp here).
 * We need ordered buckets to efficiently evict the oldest ones
 * and aggregate in time sequence — SkipList gives us both.
 *
 * STRUCTURE:
 * bucketStartTime → WindowBucket
 * 10:00:00 → [total:45, errors:3]
 * 10:01:00 → [total:67, errors:1]
 * 10:02:00 → [total:89, errors:8]  ← spike!
 * 10:03:00 → [total:71, errors:7]
 * 10:04:00 → [total:55, errors:6]
 *                                  ← 10:00 evicted when window slides to 10:05
 */
@Slf4j
public class SlidingWindow {

    private final String serviceName;
    private final long   windowSizeMs;
    private final long   bucketSizeMs = 60_000L; // 1 minute per bucket

    // Sorted by bucket start time — oldest first
    private final ConcurrentSkipListMap<Long, WindowBucket> buckets
            = new ConcurrentSkipListMap<>();

    public SlidingWindow(String serviceName, int windowSizeMinutes) {
        this.serviceName  = serviceName;
        this.windowSizeMs = (long) windowSizeMinutes * 60_000L;
    }

    /**
     * Record a log event into the appropriate time bucket.
     */
    public void record(LogEvent event) {
        long bucketKey = roundToBucket(event.getTimestamp());
        buckets.computeIfAbsent(bucketKey, WindowBucket::new)
                .record(event);
    }

    /**
     * Evict buckets that have fallen outside the window.
     * Called by the EvictionScheduler every minute.
     */
    public int evictExpiredBuckets() {
        long cutoff = System.currentTimeMillis() - windowSizeMs;
        int removed = 0;

        // headMap returns all keys < cutoff — these are expired
        for (Long expiredKey : buckets.headMap(cutoff).keySet()) {
            buckets.remove(expiredKey);
            removed++;
        }

        if (removed > 0) {
            log.debug("Evicted {} expired buckets for service: {}",
                    removed, serviceName);
        }
        return removed;
    }

    /**
     * Aggregate all active buckets into a window summary.
     */
    public WindowSummary summarize() {
        long totalEvents = 0;
        long totalErrors = 0;
        long totalWarns  = 0;
        long totalFatal  = 0;

        for (WindowBucket bucket : buckets.values()) {
            totalEvents += bucket.getTotalCount().get();
            totalErrors += bucket.getErrorCount().get();
            totalWarns  += bucket.getWarnCount().get();
            totalFatal  += bucket.getFatalCount().get();
        }

        long errorRate = totalEvents > 0
                ? (totalErrors * 100) / totalEvents : 0;

        return new WindowSummary(
                serviceName,
                System.currentTimeMillis(),
                windowSizeMs / 60_000,  // window size in minutes
                totalEvents,
                totalErrors,
                totalWarns,
                totalFatal,
                errorRate,
                buckets.size()
        );
    }

    public int bucketCount(){ return buckets.size(); }
    public String getServiceName(){ return serviceName; }
    public Collection<WindowBucket> getBuckets() { return buckets.values(); }

    private long roundToBucket(long timestamp) {
        return (timestamp / bucketSizeMs) * bucketSizeMs;
    }
}