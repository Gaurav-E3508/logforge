package com.logforge.alert.service;

import com.logforge.alert.ewma.EwmaCalculator;
import com.logforge.alert.model.AlertRule;
import com.logforge.alert.statemachine.AlertEvent;
import com.logforge.alert.statemachine.AlertInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central coordinator for anomaly detection and alert state machines.
 *
 * Per (rule + service) pair it maintains:
 * - An EwmaCalculator for statistical anomaly detection
 * - An AlertInstance for state machine tracking
 *
 * On each evaluation cycle (every minute):
 * 1. Update EWMA with current metric value
 * 2. Check if value is anomalous
 * 3. Feed result into AlertInstance state machine
 * 4. If state changed → return AlertEvent for notification
 */
@Slf4j
@Service
public class AlertManager {

    @Value("${logforge.alert.ewma-alpha:0.3}")
    private double ewmaAlpha;

    @Value("${logforge.alert.pending-threshold:3}")
    private int pendingThreshold;

    @Value("${logforge.alert.resolve-threshold:5}")
    private int resolveThreshold;

    @Value("${logforge.alert.default-error-rate-threshold:30}")
    private double defaultErrorRateThreshold;

    // key: "ruleId:serviceName"
    private final ConcurrentHashMap<String, AlertInstance>  instances
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, EwmaCalculator> calculators
            = new ConcurrentHashMap<>();

    private final List<AlertRule> rules = new ArrayList<>();

    /**
     * Load default built-in rules on startup.
     * In a full system these would come from a database / config file.
     */
    @PostConstruct
    public void loadDefaultRules() {
        // Rule 1: Alert when any service error rate exceeds 30%
        rules.add(AlertRule.builder()
                .ruleId("rule-error-rate-global")
                .name("High Error Rate")
                .serviceName(null)         // null = applies to ALL services
                .metricType(AlertRule.MetricType.ERROR_RATE_PERCENT)
                .threshold(defaultErrorRateThreshold)
                .useEwma(false)
                .channel(AlertRule.NotificationChannel.LOG_ONLY)
                .notificationTarget("ops-team")
                .enabled(true)
                .build());

        // Rule 2: EWMA-based anomaly detection on error rate
        rules.add(AlertRule.builder()
                .ruleId("rule-ewma-anomaly")
                .name("Statistical Anomaly (EWMA)")
                .serviceName(null)
                .metricType(AlertRule.MetricType.ERROR_RATE_PERCENT)
                .useEwma(true)
                .ewmaSensitivity(3.0)
                .channel(AlertRule.NotificationChannel.LOG_ONLY)
                .notificationTarget("ops-team")
                .enabled(true)
                .build());

        // Rule 3: Any FATAL events
        rules.add(AlertRule.builder()
                .ruleId("rule-fatal-events")
                .name("Fatal Events Detected")
                .serviceName(null)
                .metricType(AlertRule.MetricType.FATAL_COUNT)
                .threshold(1)
                .useEwma(false)
                .channel(AlertRule.NotificationChannel.LOG_ONLY)
                .notificationTarget("ops-team")
                .enabled(true)
                .build());

        log.info("AlertManager initialized with {} rules", rules.size());
    }

    /**
     * Evaluate all rules for a given service with its current metrics.
     * Returns list of AlertEvents that need to be dispatched.
     *
     * @param serviceName     name of the service
     * @param errorRate       current error rate % (0-100)
     * @param fatalCount      number of FATAL events in current window
     * @param totalVolume     total log volume in current window
     */
    public List<AlertEvent> evaluate(String serviceName,
                                     double errorRate,
                                     long   fatalCount,
                                     long   totalVolume) {
        List<AlertEvent> firedEvents = new ArrayList<>();

        for (AlertRule rule : rules) {
            if (!rule.isEnabled()) continue;

            // Check if rule applies to this service
            if (rule.getServiceName() != null
                    && !rule.getServiceName().equals(serviceName)) continue;

            double metricValue = getMetricValue(rule.getMetricType(),
                    errorRate, fatalCount, totalVolume);

            String instanceKey   = rule.getRuleId() + ":" + serviceName;
            String calculatorKey = rule.getRuleId() + ":" + serviceName;

            // Get or create EWMA calculator
            EwmaCalculator calculator = calculators.computeIfAbsent(
                    calculatorKey,
                    k -> new EwmaCalculator(rule.getName() + "_" + serviceName,
                            ewmaAlpha)
            );

            // Update EWMA
            calculator.update(metricValue);

            // Check anomaly
            boolean isAnomaly = rule.isUseEwma()
                    && calculator.isAnomaly(metricValue, rule.getEwmaSensitivity());

            // Get or create alert instance (state machine)
            AlertInstance instance = instances.computeIfAbsent(
                    instanceKey,
                    k -> new AlertInstance(rule, serviceName)
            );

            // Evaluate state machine
            AlertEvent event = instance.evaluate(
                    metricValue, isAnomaly,
                    pendingThreshold, resolveThreshold);

            if (event != null) {
                firedEvents.add(event);
            }
        }

        return firedEvents;
    }

    private double getMetricValue(AlertRule.MetricType type,
                                  double errorRate,
                                  long   fatalCount,
                                  long   totalVolume) {
        return switch (type) {
            case ERROR_RATE_PERCENT -> errorRate;
            case FATAL_COUNT        -> fatalCount;
            case TOTAL_VOLUME       -> totalVolume;
            case ERROR_COUNT        -> errorRate; // simplified
        };
    }

    public void addRule(AlertRule rule) {
        rules.add(rule);
        log.info("Alert rule added: {}", rule.getName());
    }

    public List<AlertRule>     getRules()     { return Collections.unmodifiableList(rules); }
    public Map<String, AlertInstance> getInstances() { return Collections.unmodifiableMap(instances); }
}