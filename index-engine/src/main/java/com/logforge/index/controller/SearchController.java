package com.logforge.index.controller;

import com.logforge.index.service.IndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

/**
 * REST API for searching the index.
 *
 * Examples:
 *   GET /api/search?q=timeout
 *   GET /api/search?q=level:ERROR AND service:payment
 *   GET /api/search?q="payment timeout"
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class SearchController {

    private final IndexService indexService;

    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> search(
            @RequestParam String q) {

        Set<String> eventIds = indexService.search(q);

        return ResponseEntity.ok(Map.of(
                "query",      q,
                "hits",       eventIds.size(),
                "eventIds",   eventIds,
                "indexSize",  indexService.indexedTermCount()
        ));
    }

    @GetMapping("/index/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
                "totalTerms", indexService.indexedTermCount()
        ));
    }
}