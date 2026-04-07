package com.logforge.index.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One entry in a PostingList — represents a single document
 * that contains a specific term.
 *
 * Example: term "timeout" has a PostingList containing:
 *   Posting("evt-001", 1711650000000, 2, "payment-service")
 *   Posting("evt-004", 1711650005000, 1, "auth-service")
 *   ...
 *
 * positions = which word positions in the message contain this term.
 * Used for phrase queries: "payment timeout" requires "payment" at
 * position N and "timeout" at position N+1 in the SAME document.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Posting implements Comparable<Posting> {

    /** The log event this posting refers to */
    private String eventId;

    /** Timestamp of the event — used for range queries */
    private long timestamp;

    /** Word positions in the message where this term appears */
    private int[] positions;

    /** Which service emitted this log — for service-scoped searches */
    private String serviceName;

    /**
     * Sort by timestamp descending — newest results first.
     * This is what makes range queries efficient.
     */
    @Override
    public int compareTo(Posting other) {
        return Long.compare(other.timestamp, this.timestamp);
    }
}