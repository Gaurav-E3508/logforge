package com.logforge.query.controller;

import com.logforge.common.model.LogEvent;
import com.logforge.query.parser.ParsedQuery;
import com.logforge.query.parser.QueryParser;
import com.logforge.query.service.QueryExecutor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * The main search REST API.
 *
 * EXAMPLES:
 *   GET /api/query?q=timeout
 *   GET /api/query?q=level:ERROR
 *   GET /api/query?q=level:ERROR AND service:payment
 *   GET /api/query?q=level:ERROR AND message:"db timeout"
 *   GET /api/query?q=level:ERROR OR level:FATAL
 */
@Slf4j
@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
public class QueryController {

    private final QueryParser   queryParser;
    private final QueryExecutor queryExecutor;

    @GetMapping
    public ResponseEntity<Map<String, Object>> query(
            @RequestParam(defaultValue = "") String q,
            @RequestParam(defaultValue = "0") long from,
            @RequestParam(defaultValue = "0") long to) {

        long startMs = System.currentTimeMillis();

        ParsedQuery    parsed  = queryParser.parse(q);
        List<LogEvent> results = queryExecutor.execute(parsed);

        long durationMs = System.currentTimeMillis() - startMs;

        return ResponseEntity.ok(Map.of(
                "query",      q,
                "parsedAs",   describeQuery(parsed),
                "hits",       results.size(),
                "durationMs", durationMs,
                "results",    results
        ));
    }

    @GetMapping("/explain")
    public ResponseEntity<Map<String, Object>> explain(@RequestParam String q) {
        ParsedQuery parsed = queryParser.parse(q);

        return ResponseEntity.ok(Map.of(
                "rawQuery",  q,
                "operator",  parsed.getOperator(),
                "clauses",   parsed.getClauses().stream().map(c -> Map.of(
                        "type",  c.getType(),
                        "field", c.getField(),
                        "value", c.getValue()
                )).toList()
        ));
    }

    private String describeQuery(ParsedQuery parsed) {
        if (parsed.isEmpty()) return "empty";
        return parsed.getClauses().size() + " clause(s) joined by "
                + parsed.getOperator();
    }
}