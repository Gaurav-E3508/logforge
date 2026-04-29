package com.logforge.index.service;

import com.logforge.common.model.LogEvent;
import com.logforge.index.core.HotIndex;
import com.logforge.index.tokenizer.LogTokenizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Public API for the index engine.
 * Handles indexing and query parsing + execution.
 *
 * Supported query syntax:
 *   level:ERROR                          ← field query
 *   service:payment                      ← field query
 *   timeout                              ← term query
 *   payment AND timeout                  ← boolean AND
 *   timeout OR refused                   ← boolean OR
 *   "payment timeout"                    ← phrase query
 *   level:ERROR AND service:payment      ← combined
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IndexService {

    private final HotIndex     hotIndex;
    private final LogTokenizer tokenizer;

    /**
     * Index a new log event — called by the Kafka consumer.
     */
    public void index(LogEvent event) {
        hotIndex.index(event);
        log.debug("Indexed event: {} | service: {} | terms in index: {}",
                event.getEventId(), event.getServiceName(), hotIndex.termCount());
    }

    /**
     * Execute a search query and return matching event IDs.
     *
     * Query examples:
     *   "timeout"                     → simple term search
     *   "payment AND timeout"         → AND query
     *   "timeout OR refused"          → OR query
     *   "\"payment timeout\""         → phrase query
     *   "level:ERROR AND service:payment" → field + boolean
     */
    public Set<String> search(String query) {
        if (query == null || query.isBlank()) return Collections.emptySet();

        log.debug("Executing search query: {}", query);
        query = query.trim();

        // Phrase query: "payment timeout"
        if (query.startsWith("\"") && query.endsWith("\"")) {
            String phrase = query.substring(1, query.length() - 1);
            List<String> phraseTerms = tokenizer.tokenizeToTerms(phrase);
            return hotIndex.searchPhrase(phraseTerms);
        }

        // AND query: payment AND timeout
        if (query.toUpperCase().contains(" AND ")) {
            List<String> terms = Arrays.stream(query.split("(?i)\\sAND\\s"))
                    .map(String::trim)
                    .map(this::normalizeTerm)
                    .toList();
            return hotIndex.searchAnd(terms);
        }

        // OR query: timeout OR refused
        if (query.toUpperCase().contains(" OR ")) {
            List<String> terms = Arrays.stream(query.split("(?i)\\sOR\\s"))
                    .map(String::trim)
                    .map(this::normalizeTerm)
                    .toList();
            return hotIndex.searchOr(terms);
        }

        // Single term or field query: "timeout" or "level:ERROR"
        String normalized = normalizeTerm(query);
        return hotIndex.searchOr(List.of(normalized));
    }

    /**
     * Normalize a query term.
     * "level:ERROR" stays as "level:error" (field queries use colon prefix).
     * "timeout" → "timeout" (plain term).
     */
    private String normalizeTerm(String term) {
        if (term.contains(":")) {
            // Field query — keep the colon, lowercase everything
            return term.toLowerCase();
        }
        return term.toLowerCase();
    }

    public int indexedTermCount() { return hotIndex.termCount(); }
}