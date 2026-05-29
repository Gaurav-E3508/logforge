package com.logforge.alert.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A user-defined alert rule.
 *
 * Examples:
 *   "Alert me when payment-service error rate > 30%"
 *   "Alert me when any service has > 100 errors/min"
 *   "Alert me when auth-service has any FATAL events"
 *
 * Rules are evaluated every minute against the current EWMA values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertRule {

    private String ruleId;
    private String name;
    private String serviceName;      // null = applies to all services

    /** What metric triggers this rule */
    private MetricType metricType;

    /** Raw threshold — used if useEwma = false */
    private double threshold;

    /** If true, use EWMA anomaly detection instead of raw threshold */
    private boolean useEwma;

    /** EWMA sensitivity — standard deviations above mean = anomaly */
    @Builder.Default
    private double ewmaSensitivity = 3.0;

    /** Notification channels for this rule */
    private NotificationChannel channel;

    /** Who to notify (Slack channel, email address, webhook URL) */
    private String notificationTarget;

    private boolean enabled;

    public enum MetricType {
        ERROR_RATE_PERCENT,   // % of events that are errors
        ERROR_COUNT,          // absolute error count in window
        TOTAL_VOLUME,         // total log volume (for detecting log floods)
        FATAL_COUNT           // number of FATAL events
    }

    public enum NotificationChannel {
        SLACK, EMAIL, WEBHOOK, LOG_ONLY
    }
}