package com.logforge.sdk.buffer;

import com.logforge.common.model.LogEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A lock-free ring buffer (circular queue) for LogEvent objects.
 *
 * REAL LIFE ANALOGY:
 * Imagine a rotating sushi conveyor belt with exactly N plates.
 * The chef (your app) puts sushi (log events) on plates as fast as possible.
 * The customer (Kafka publisher) picks up plates from the other end.
 * If the belt is full, the chef skips that plate (drops the event) rather
 * than stopping the kitchen — keeping your app ALWAYS responsive.
 *
 * WHY NOT JUST USE A QUEUE?
 * A regular LinkedList queue allocates memory for every new event.
 * A ring buffer pre-allocates all slots once — zero GC pressure,
 * which matters when you're processing thousands of logs per second.
 *
 * SIZE MUST BE POWER OF 2 — this lets us use bitwise AND (&) instead
 * of the expensive modulo (%) operation for index wrapping:
 *   index = sequence & (size - 1)  ← super fast
 *   index = sequence % size        ← slower
 */
@Slf4j
public class LogEventRingBuffer {

    private final LogEvent[] buffer;
    private final int mask;           // size - 1, used for fast index calculation
    private final AtomicLong writeSequence = new AtomicLong(0);
    private final AtomicLong readSequence  = new AtomicLong(0);
    private final AtomicLong droppedCount  = new AtomicLong(0);

    public LogEventRingBuffer(int size) {
        // Enforce power of 2
        if (size <= 0 || (size & (size - 1)) != 0) {
            throw new IllegalArgumentException(
                    "Ring buffer size must be a power of 2, got: " + size
            );
        }
        this.buffer = new LogEvent[size];
        this.mask = size - 1;
    }

    /**
     * Try to add a log event to the buffer.
     * NON-BLOCKING — if buffer is full, the event is dropped (never blocks your app).
     *
     * @return true if event was added, false if buffer was full
     */
    public boolean tryPublish(LogEvent event) {
        long currentWrite = writeSequence.get();
        long currentRead  = readSequence.get();

        // Check if buffer is full
        if (currentWrite - currentRead >= buffer.length) {
            droppedCount.incrementAndGet();
            log.warn("Ring buffer full — dropping log event from service: {}. " +
                    "Consider increasing logforge.buffer-size", event.getServiceName());
            return false;
        }

        // Write the event at the next slot
        int slot = (int)(currentWrite & mask);
        buffer[slot] = event;
        writeSequence.incrementAndGet();
        return true;
    }

    /**
     * Drain up to maxItems events from the buffer into a list.
     * Called by the background publisher thread.
     *
     * @return list of drained events (may be empty if buffer is empty)
     */
    public List<LogEvent> drain(int maxItems) {
        List<LogEvent> events = new ArrayList<>(maxItems);
        long currentRead  = readSequence.get();
        long currentWrite = writeSequence.get();

        int available = (int) Math.min(currentWrite - currentRead, maxItems);
        for (int i = 0; i < available; i++) {
            int slot = (int)((currentRead + i) & mask);
            events.add(buffer[slot]);
            buffer[slot] = null; // help GC
        }

        if (!events.isEmpty()) {
            readSequence.addAndGet(events.size());
        }
        return events;
    }

    public int size()         { return (int)(writeSequence.get() - readSequence.get()); }
    public boolean isEmpty()  { return size() == 0; }
    public long droppedCount(){ return droppedCount.get(); }
}