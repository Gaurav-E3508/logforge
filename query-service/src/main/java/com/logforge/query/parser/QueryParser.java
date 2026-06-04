package com.logforge.query.parser;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Parses LogForge mini query language into a structured ParsedQuery.
 *
 * SUPPORTED SYNTAX:
 *   level:ERROR                           field query
 *   service:payment-service               field query
 *   timeout                               free-text term
 *   "payment timeout"                     phrase query
 *   level:ERROR AND service:payment       AND combination
 *   timeout OR refused                    OR combination
 *   level:ERROR AND message:"db timeout"  mixed
 *
 * HOW IT WORKS (AST-lite approach):
 * 1. Split on AND/OR operators
 * 2. Classify each clause as field query or text query
 * 3. Package into ParsedQuery for the QueryExecutor
 */
@Slf4j
@Component
public class QueryParser {

    public ParsedQuery parse(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return ParsedQuery.empty();
        }

        rawQuery = rawQuery.trim();
        log.debug("Parsing query: {}", rawQuery);

        // Determine boolean operator
        ParsedQuery.Operator operator = ParsedQuery.Operator.AND;
        List<String> clauses;

        if (rawQuery.toUpperCase().contains(" OR ")) {
            operator = ParsedQuery.Operator.OR;
            clauses  = splitRespectingQuotes(rawQuery, " OR ");
        } else {
            clauses = splitRespectingQuotes(rawQuery, " AND ");
        }

        List<QueryClause> parsedClauses = new ArrayList<>();
        for (String clause : clauses) {
            parsedClauses.add(parseClause(clause.trim()));
        }

        return new ParsedQuery(parsedClauses, operator, rawQuery);
    }

    private QueryClause parseClause(String clause) {
        // Phrase query: message:"payment timeout"
        if (clause.contains(":\"")) {
            int colonIdx = clause.indexOf(":\"");
            String field  = clause.substring(0, colonIdx).toLowerCase();
            String phrase = clause.substring(colonIdx + 2,
                    clause.endsWith("\"") ? clause.length() - 1
                            : clause.length());
            return new QueryClause(QueryClause.Type.PHRASE, field, phrase);
        }

        // Bare phrase: "payment timeout"
        if (clause.startsWith("\"") && clause.endsWith("\"")) {
            String phrase = clause.substring(1, clause.length() - 1);
            return new QueryClause(QueryClause.Type.PHRASE, "message", phrase);
        }

        // Field query: level:ERROR or service:payment
        if (clause.contains(":")) {
            String[] parts = clause.split(":", 2);
            return new QueryClause(QueryClause.Type.FIELD,
                    parts[0].toLowerCase(),
                    parts[1].toLowerCase());
        }

        // Plain text term
        return new QueryClause(QueryClause.Type.TEXT, "message",
                clause.toLowerCase());
    }

    /**
     * Split a string on a delimiter but not inside quoted strings.
     * "payment AND timeout" AND level:ERROR → splits on outer AND only.
     */
    private List<String> splitRespectingQuotes(String input, String delimiter) {
        List<String> parts = new ArrayList<>();
        int          start = 0;
        boolean      inQuote = false;
        String       upper = input.toUpperCase();
        String       delim = delimiter.toUpperCase();

        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '"') inQuote = !inQuote;
            if (!inQuote && upper.startsWith(delim, i)) {
                parts.add(input.substring(start, i));
                start = i + delimiter.length();
                i    += delimiter.length() - 1;
            }
        }
        parts.add(input.substring(start));
        return parts;
    }
}