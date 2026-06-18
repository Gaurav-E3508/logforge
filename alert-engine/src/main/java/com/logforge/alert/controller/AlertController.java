package com.logforge.alert.controller;

import com.logforge.alert.config.ConfigPublisher;
import com.logforge.alert.model.AlertRule;
import com.logforge.alert.service.AlertManager;
import com.logforge.alert.statemachine.AlertInstance;
import com.logforge.alert.statemachine.AlertState;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST API to manage alert rules and view current alert states.
 *
 * GET  /api/alerts/rules           — list all rules
 * POST /api/alerts/rules           — add a new rule
 * GET  /api/alerts/active          — list currently FIRING alerts
 * GET  /api/alerts/status          — full state of all alert instances
 */
@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final ConfigPublisher configPublisher;
    private final AlertManager alertManager;

    @GetMapping("/rules")
    public ResponseEntity<List<AlertRule>> getRules() {
        return ResponseEntity.ok(alertManager.getRules());
    }

    @PostMapping("/rules")
    public ResponseEntity<String> addRule(@RequestBody AlertRule rule) {
        alertManager.addRule(rule);
        return ResponseEntity.ok("Rule added: " + rule.getName());
    }


    @PutMapping("/config")
    public ResponseEntity<String> updateDistributedConfig(
            @RequestBody List<AlertRule> rules) {
        configPublisher.publishRules(rules);
        return ResponseEntity.ok("Published " + rules.size()
                + " rule(s) — hot-reloading across all running alert-engine instances");
    }

    @GetMapping("/active")
    public ResponseEntity<List<Map<String, Object>>> getActiveAlerts() {
        List<Map<String, Object>> firing = alertManager.getInstances()
                .entrySet().stream()
                .filter(e -> e.getValue().getState() == AlertState.FIRING)
                .map(e -> {
                    AlertInstance inst = e.getValue();
                    return Map.<String, Object>of(
                            "ruleId",      inst.getRule().getRuleId(),
                            "ruleName",    inst.getRule().getName(),
                            "service",     inst.getServiceName(),
                            "state",       inst.getState(),
                            "lastValue",   inst.getLastValue(),
                            "threshold",   inst.getRule().getThreshold(),
                            "since",       inst.getLastStateChangeTime()
                    );
                })
                .collect(Collectors.toList());

        return ResponseEntity.ok(firing);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, AlertInstance> instances = alertManager.getInstances();

        long firing   = instances.values().stream()
                .filter(i -> i.getState() == AlertState.FIRING).count();
        long pending  = instances.values().stream()
                .filter(i -> i.getState() == AlertState.PENDING).count();
        long inactive = instances.values().stream()
                .filter(i -> i.getState() == AlertState.INACTIVE).count();

        return ResponseEntity.ok(Map.of(
                "totalInstances", instances.size(),
                "firing",  firing,
                "pending", pending,
                "inactive", inactive,
                "totalRules", alertManager.getRules().size()
        ));
    }
}