package com.logforge.stream.window;

import com.logforge.common.model.LogEvent;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents one time bucket (one minute) in the sliding window.
 *
 * REAL LIFE ANALOGY:
 * Think of a tally sheet for one minute of traffic on a road.
 * It tracks: total vehicles, cars, trucks, accidents (errors).
 * After the window slides past this minute, the sheet is discarded.
 *
 * All counters are AtomicLong — incremented safely by multiple
 * Kafka consumer threads without locking.
 */
@Getter
public class WindowBucket {

    private final long   bucketStartTime; // epoch ms — start of this minute
    private final AtomicLong totalCount  = new AtomicLong(0);
    private final AtomicLong errorCount  = new AtomicLong(0);
    private final AtomicLong warnCount   = new AtomicLong(0);
    private final AtomicLong infoCount   = new AtomicLong(0);
    private final AtomicLong fatalCount  = new AtomicLong(0);

    public WindowBucket(long bucketStartTime) {
        this.bucketStartTime = bucketStartTime;
    }

    public void record(LogEvent event) {
        totalCount.incrementAndGet();
        switch (event.getLevel()) {
            case ERROR -> errorCount.incrementAndGet();
            case WARN  -> warnCount.incrementAndGet();
            case FATAL -> fatalCount.incrementAndGet();
            default    -> infoCount.incrementAndGet();
        }
    }

    public double getErrorRate() {
        long total = totalCount.get();
        if (total == 0) return 0;

        return (errorCount.get() * 100.0) / total;
    }

    public boolean isExpired(long windowSizeMs) {
        return System.currentTimeMillis() - bucketStartTime > windowSizeMs;
    }
}