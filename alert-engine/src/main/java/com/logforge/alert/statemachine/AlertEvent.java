package com.logforge.alert.statemachine;

import com.logforge.alert.model.AlertRule;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represents a state change that needs to be notified.
 * Published to the ALERTS Kafka topic for fanout to channels.
 */
@Data
@AllArgsConstructor
public class AlertEvent {
    private String ruleId;
    private String ruleName;
    private String serviceName;
    private AlertEventType type;         // FIRING or RESOLVED
    private double currentValue;
    private double threshold;
    private AlertRule.NotificationChannel channel;
    private String notificationTarget;
    private long timestamp;
}