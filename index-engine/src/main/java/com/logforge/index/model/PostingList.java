package com.logforge.index.model;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A sorted list of Postings for a single term.
 *
 * Thread-safe via ReentrantReadWriteLock:
 * - Multiple threads can READ simultaneously (shared lock)
 * - Only ONE thread can WRITE at a time (exclusive lock)
 *
 * WHY ReentrantReadWriteLock instead of synchronized?
 * In a search engine, reads vastly outnumber writes.
 * synchronized blocks ALL threads even for concurrent reads.
 * ReadWriteLock allows 100 search threads to read in parallel —
 * only blocking when a new log event is being indexed.
 */
@Slf4j
public class PostingList {

    private final List<Posting> postings = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock  readLock  = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final int maxSize;

    public PostingList(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Add a posting — maintains sorted order by timestamp (newest first).
     * If list exceeds maxSize, oldest entries are trimmed.
     */
    public void add(Posting posting) {
        writeLock.lock();
        try {
            postings.add(posting);
            Collections.sort(postings);  // keeps newest-first order

            // Trim if overgrown — cold entries should be flushed to MongoDB
            if (postings.size() > maxSize) {
                postings.subList(maxSize, postings.size()).clear();
                log.debug("PostingList trimmed to maxSize: {}", maxSize);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get all postings for this term (read-only snapshot).
     */
    public List<Posting> getAll() {
        readLock.lock();
        try {
            return new ArrayList<>(postings); // defensive copy
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get postings within a time range.
     * Since list is sorted newest-first, we scan until we pass fromTime.
     */
    public List<Posting> getInRange(long fromTime, long toTime) {
        readLock.lock();
        try {
            List<Posting> result = new ArrayList<>();
            for (Posting p : postings) {
                if (p.getTimestamp() < fromTime) break; // sorted — safe to stop
                if (p.getTimestamp() <= toTime) {
                    result.add(p);
                }
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    public int size() {
        readLock.lock();
        try { return postings.size(); }
        finally { readLock.unlock(); }
    }
}