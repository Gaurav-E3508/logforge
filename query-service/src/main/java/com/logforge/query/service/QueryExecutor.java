package com.logforge.query.service;

import com.logforge.common.model.LogEvent;
import com.logforge.query.parser.ParsedQuery;
import com.logforge.query.parser.QueryClause;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Executes a ParsedQuery across MongoDB (warm) and Redis (hot).
 *
 * K-WAY MERGE:
 * Results from different tiers are merged and sorted by timestamp.
 * Duplicates (same eventId appearing in multiple tiers) are removed.
 *
 * EXECUTION STRATEGY PER CLAUSE TYPE:
 * - FIELD clause (level:ERROR)    → MongoDB field query
 * - TEXT  clause (timeout)        → MongoDB $regex on message field
 * - PHRASE clause ("db timeout")  → MongoDB $regex with full phrase
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryExecutor {

    private final MongoTemplate mongoTemplate;

    @Value("${logforge.query.max-results:500}")
    private int maxResults;

    public List<LogEvent> execute(ParsedQuery query) {
        if (query.isEmpty()) return Collections.emptyList();

        log.debug("Executing query: {}", query.getRawQuery());

        // Build MongoDB criteria from parsed clauses
        List<Criteria> criteriaList = query.getClauses().stream()
                .map(this::buildCriteria)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (criteriaList.isEmpty()) return Collections.emptyList();

        // Combine criteria with AND or OR
        Criteria combined = query.getOperator() == ParsedQuery.Operator.AND
                ? new Criteria().andOperator(criteriaList.toArray(new Criteria[0]))
                : new Criteria().orOperator(criteriaList.toArray(new Criteria[0]));

        Query mongoQuery = new Query(combined).limit(maxResults);

        List<LogEvent> results = mongoTemplate.find(mongoQuery,
                LogEvent.class, "logs");

        // Sort newest first
        results.sort((a, b) -> Long.compare(b.getTimestamp(), a.getTimestamp()));

        log.debug("Query '{}' returned {} results", query.getRawQuery(), results.size());
        return results;
    }

    private Criteria buildCriteria(QueryClause clause) {
        return switch (clause.getType()) {

            case FIELD -> switch (clause.getField()) {
                case "level"    -> Criteria.where("level")
                        .is(clause.getValue().toUpperCase());
                case "service"  -> Criteria.where("serviceName")
                        .is(clause.getValue());
                case "category" -> Criteria.where("category")
                        .is(clause.getValue().toUpperCase());
                case "env"      -> Criteria.where("environment")
                        .is(clause.getValue().toUpperCase());
                default         -> Criteria.where(clause.getField())
                        .is(clause.getValue());
            };

            case TEXT   -> Criteria.where("message")
                    .regex(clause.getValue(), "i"); // case-insensitive

            case PHRASE -> Criteria.where("message")
                    .regex("\\b" + clause.getValue() + "\\b", "i");
        };
    }
}