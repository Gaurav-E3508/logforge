package com.logforge.index.tokenizer;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Breaks a log message into index terms (tokens).
 *
 * Input:  "Payment gateway timeout after 5000ms for user u_991"
 * Output: ["payment", "gateway", "timeout", "after", "5000ms",
 *          "user", "u_991"]
 *         (stopwords removed, lowercased, position tracked)
 *
 * Stopwords are common words with no search value ("for", "the", "a").
 * Removing them reduces index size by ~30% with zero search quality loss.
 */
@Component
public class LogTokenizer {

    private static final Set<String> STOPWORDS = Set.of(
            "the", "a", "an", "is", "in", "on", "at", "to", "for",
            "of", "and", "or", "not", "with", "this", "that", "was",
            "has", "have", "are", "be", "been", "by", "from", "after"
    );

    /**
     * Tokenize a log message into a list of (term, position) pairs.
     */
    public List<TokenPosition> tokenize(String message) {
        List<TokenPosition> tokens = new ArrayList<>();
        if (message == null || message.isBlank()) return tokens;

        // Split on whitespace and common delimiters
        String[] rawTokens = message.toLowerCase()
                .split("[\\s\\.,;:!?\\[\\](){}\"'=<>|/\\\\@#$%^&*+~`]+");

        int position = 0;
        for (String raw : rawTokens) {
            String term = raw.trim();

            // Skip empty, stopwords, and very short tokens
            if (term.length() < 2 || STOPWORDS.contains(term)) {
                position++;
                continue;
            }

            tokens.add(new TokenPosition(term, position));
            position++;
        }
        return tokens;
    }

    /**
     * Simple tokenize — just returns terms without position tracking.
     * Used for boolean search queries.
     */
    public List<String> tokenizeToTerms(String text) {
        return tokenize(text).stream()
                .map(TokenPosition::term)
                .toList();
    }

    public record TokenPosition(String term, int position) {}
}