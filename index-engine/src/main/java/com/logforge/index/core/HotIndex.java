package com.logforge.index.core;

import com.logforge.common.model.LogEvent;
import com.logforge.index.model.Posting;
import com.logforge.index.model.PostingList;
import com.logforge.index.tokenizer.LogTokenizer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The in-memory (JVM heap) index — the "hot" tier.
 *
 * Structure: HashMap<term, PostingList>
 *
 * Example contents after indexing 3 log messages:
 * {
 *   "timeout"  → PostingList[Posting(evt1), Posting(evt3)],
 *   "payment"  → PostingList[Posting(evt1), Posting(evt2)],
 *   "database" → PostingList[Posting(evt2)],
 *   "failed"   → PostingList[Posting(evt1), Posting(evt2), Posting(evt3)]
 * }
 *
 * Uses ConcurrentHashMap so multiple indexer threads can write in parallel.
 * Individual PostingLists use ReentrantReadWriteLock internally.
 */
@Slf4j
@Component
public class HotIndex {

    // term → PostingList
    private final ConcurrentHashMap<String, PostingList> index
            = new ConcurrentHashMap<>();

    private final LogTokenizer tokenizer;
    private final int maxPostingsPerTerm;

    public HotIndex(LogTokenizer tokenizer,
                    @Value("${logforge.index.max-postings-per-term:10000}")
                    int maxPostingsPerTerm) {
        this.tokenizer        = tokenizer;
        this.maxPostingsPerTerm = maxPostingsPerTerm;
    }

    /**
     * Index a log event — extract terms from message and add to posting lists.
     */
    public void index(LogEvent event) {
        if (event.getMessage() == null) return;

        List<LogTokenizer.TokenPosition> tokens =
                tokenizer.tokenize(event.getMessage());

        // Group positions by term (a term may appear multiple times in a message)
        Map<String, List<Integer>> termPositions = new HashMap<>();
        for (LogTokenizer.TokenPosition tp : tokens) {
            termPositions
                    .computeIfAbsent(tp.term(), k -> new ArrayList<>())
                    .add(tp.position());
        }

        // Add to index
        for (Map.Entry<String, List<Integer>> entry : termPositions.entrySet()) {
            String term       = entry.getKey();
            int[]  positions  = entry.getValue().stream()
                    .mapToInt(Integer::intValue).toArray();

            Posting posting = new Posting(
                    event.getEventId(),
                    event.getTimestamp(),
                    positions,
                    event.getServiceName()
            );

            index.computeIfAbsent(term, k -> new PostingList(maxPostingsPerTerm))
                    .add(posting);
        }

        // Also index serviceName and level as searchable terms
        if (event.getServiceName() != null) {
            addMetaTerm("service:" + event.getServiceName().toLowerCase(), event);
        }
        if (event.getLevel() != null) {
            addMetaTerm("level:" + event.getLevel().name().toLowerCase(), event);
        }
        if (event.getCategory() != null) {
            addMetaTerm("category:" + event.getCategory().toLowerCase(), event);
        }
    }

    /**
     * Boolean AND search — find events containing ALL terms.
     */
    public Set<String> searchAnd(List<String> terms) {
        if (terms.isEmpty()) return Collections.emptySet();

        // Start with the smallest posting list (optimization — fewer intersections)
        List<List<String>> sortedLists = terms.stream()
                .map(t -> t.toLowerCase())
                .map(t -> {
                    PostingList pl = index.get(t);
                    return pl == null ? Collections.<String>emptyList()
                            : pl.getAll().stream()
                            .map(Posting::getEventId).toList();
                })
                .sorted(Comparator.comparingInt(List::size))
                .toList();

        if (sortedLists.isEmpty() || sortedLists.get(0).isEmpty()) {
            return Collections.emptySet();
        }

        // Intersect all posting lists
        Set<String> result = new HashSet<>(sortedLists.get(0));
        for (int i = 1; i < sortedLists.size(); i++) {
            result.retainAll(new HashSet<>(sortedLists.get(i)));
            if (result.isEmpty()) break; // early exit
        }
        return result;
    }

    /**
     * Boolean OR search — find events containing ANY term.
     */
    public Set<String> searchOr(List<String> terms) {
        Set<String> result = new HashSet<>();
        for (String term : terms) {
            PostingList pl = index.get(term.toLowerCase());
            if (pl != null) {
                pl.getAll().stream()
                        .map(Posting::getEventId)
                        .forEach(result::add);
            }
        }
        return result;
    }

    /**
     * Phrase search — find events where terms appear consecutively.
     * e.g., "payment timeout" requires "payment" at pos N, "timeout" at pos N+1
     */
    public Set<String> searchPhrase(List<String> phraseTerms) {
        if (phraseTerms.isEmpty()) return Collections.emptySet();

        // Get posting lists for all phrase terms
        List<PostingList> lists = phraseTerms.stream()
                .map(t -> index.get(t.toLowerCase()))
                .toList();

        if (lists.stream().anyMatch(Objects::isNull)) {
            return Collections.emptySet(); // any missing term = no phrase match
        }

        // Find documents that contain all terms
        Set<String> candidates = searchAnd(phraseTerms);

        // Among candidates, check positional adjacency
        Set<String> phraseMatches = new HashSet<>();
        for (String eventId : candidates) {
            if (hasConsecutivePositions(eventId, phraseTerms, lists)) {
                phraseMatches.add(eventId);
            }
        }
        return phraseMatches;
    }

    public int termCount()     { return index.size(); }
    public boolean isEmpty()   { return index.isEmpty(); }
    public Set<String> terms() { return index.keySet(); }

    // -- private helpers --

    private void addMetaTerm(String term, LogEvent event) {
        Posting posting = new Posting(
                event.getEventId(), event.getTimestamp(), new int[]{0}, event.getServiceName()
        );
        index.computeIfAbsent(term, k -> new PostingList(maxPostingsPerTerm))
                .add(posting);
    }

    private boolean hasConsecutivePositions(String eventId,
                                            List<String> terms,
                                            List<PostingList> lists) {
        // Get positions of first term in this document
        List<int[]> firstPositions = lists.get(0).getAll().stream()
                .filter(p -> p.getEventId().equals(eventId))
                .map(Posting::getPositions)
                .toList();

        for (int[] startPositions : firstPositions) {
            for (int startPos : startPositions) {
                if (isPhrasePresentAt(startPos, eventId, terms, lists)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isPhrasePresentAt(int startPos, String eventId,
                                      List<String> terms,
                                      List<PostingList> lists) {
        for (int i = 1; i < terms.size(); i++) {
            final int expectedPos = startPos + i;
            boolean found = lists.get(i).getAll().stream()
                    .filter(p -> p.getEventId().equals(eventId))
                    .anyMatch(p -> {
                        for (int pos : p.getPositions()) {
                            if (pos == expectedPos) return true;
                        }
                        return false;
                    });
            if (!found) return false;
        }
        return true;
    }
}