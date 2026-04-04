package com.logforge.ingestion.dedup;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A space-efficient probabilistic data structure for deduplication.
 *
 * REAL LIFE ANALOGY:
 * Imagine a bouncer at a club with a notebook. When someone enters,
 * he marks 3 different spots in the notebook (3 hash functions).
 * When someone tries to enter again, he checks those same 3 spots.
 * If ALL 3 spots are marked → "probably seen before" → deny entry.
 * If ANY spot is unmarked → "definitely not seen" → allow entry.
 *
 * KEY PROPERTY:
 * - Zero false negatives: if an event was seen, it WILL be caught
 * - Small false positive rate: ~1% of new events wrongly flagged as duplicates
 * - Uses ~1.2MB of memory for 1 million events (vs ~50MB for a HashSet)
 *
 * WHY THIS MATTERS:
 * Kafka can deliver the same message more than once (at-least-once delivery).
 * Without deduplication, you'd store the same log event multiple times.
 * The Bloom Filter catches ~99% of duplicates before they hit MongoDB.
 */
@Slf4j
public class BloomFilter {

    private final BitSet  bitSet;
    private final int     bitSetSize;
    private final int     numHashFunctions;
    private final AtomicLong itemCount    = new AtomicLong(0);
    private final AtomicLong rejectedCount = new AtomicLong(0);

    /**
     * @param expectedInsertions  how many unique events you expect (e.g. 1,000,000)
     * @param falsePositiveRate   acceptable error rate (e.g. 0.01 = 1%)
     */
    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        // Formula to calculate optimal bit set size:
        // m = -(n * ln(p)) / (ln(2)^2)
        this.bitSetSize = optimalBitSetSize(expectedInsertions, falsePositiveRate);

        // Formula for optimal number of hash functions:
        // k = (m/n) * ln(2)
        this.numHashFunctions = optimalHashFunctions(bitSetSize, expectedInsertions);

        this.bitSet = new BitSet(bitSetSize);

        log.info("BloomFilter initialized — bitSetSize: {}, hashFunctions: {}, " +
                        "expectedInsertions: {}, errorRate: {}%",
                bitSetSize, numHashFunctions, expectedInsertions,
                (int)(falsePositiveRate * 100));
    }

    /**
     * Check if an eventId was possibly seen before.
     * @return true = possibly duplicate (might be false positive)
     *         false = definitely NOT a duplicate
     */
    public boolean mightContain(String eventId) {
        int[] hashes = getHashPositions(eventId);
        for (int hash : hashes) {
            if (!bitSet.get(hash)) return false; // definitely new
        }
        rejectedCount.incrementAndGet();
        return true; // probably duplicate
    }

    /**
     * Mark an eventId as seen.
     * Call this AFTER mightContain returns false (i.e. it's a new event).
     */
    public void put(String eventId) {
        int[] hashes = getHashPositions(eventId);
        for (int hash : hashes) {
            bitSet.set(hash);
        }
        itemCount.incrementAndGet();
    }

    /**
     * Compute k different hash positions for a given key.
     * Uses double hashing technique — only 2 actual hash computations,
     * then combined to simulate k independent hash functions.
     */
    private int[] getHashPositions(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);

        int hash1 = murmurHash(bytes, 0);
        int hash2 = murmurHash(bytes, hash1);

        int[] positions = new int[numHashFunctions];
        for (int i = 0; i < numHashFunctions; i++) {
            // Combined hash: h1 + i*h2 (mod bitSetSize)
            int combined = hash1 + (i * hash2);
            positions[i] = Math.abs(combined % bitSetSize);
        }
        return positions;
    }

    /**
     * MurmurHash — fast, non-cryptographic hash function.
     * Much faster than SHA/MD5, good distribution, perfect for Bloom Filters.
     */
    private int murmurHash(byte[] data, int seed) {
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ data.length;

        for (int i = 0; i < data.length - 3; i += 4) {
            int k = (data[i] & 0xFF)
                    | ((data[i+1] & 0xFF) << 8)
                    | ((data[i+2] & 0xFF) << 16)
                    | ((data[i+3] & 0xFF) << 24);
            k *= m; k ^= k >>> r; k *= m;
            h *= m; h ^= k;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    private int optimalBitSetSize(int n, double p) {
        return (int)(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private int optimalHashFunctions(int m, int n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    public long getItemCount()     { return itemCount.get(); }
    public long getRejectedCount() { return rejectedCount.get(); }
}