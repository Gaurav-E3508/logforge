package com.logforge.alert.statemachine;

import com.logforge.alert.model.AlertRule;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * A live instance of an AlertRule — tracks its current state.
 *
 * One AlertInstance exists per (rule + service) combination.
 * Example: rule "error rate > 30%" applied to "payment-service"
 *          = one AlertInstance with its own state machine.
 *
 * STATE TRANSITIONS:
 *
 *  INACTIVE
 *     │  threshold breached (consecutiveBreaches = 1)
 *     ▼
 *  PENDING ──── threshold breached again ──► consecutiveBreaches++
 *     │  consecutiveBreaches >= pendingThreshold
 *     ▼
 *  FIRING ─── sends notification ──► reset consecutiveNormalReadings
 *     │  back to normal (consecutiveNormalReadings >= resolveThreshold)
 *     ▼
 *  RESOLVED ──► sends "all clear" notification ──► back to INACTIVE
 */
@Data
@Slf4j
public class AlertInstance {

    private final AlertRule  rule;
    private final String     serviceName;

    private AlertState state = AlertState.INACTIVE;
    private int  consecutiveBreaches       = 0;
    private int  consecutiveNormalReadings = 0;
    private long lastStateChangeTime       = System.currentTimeMillis();
    private double lastValue               = 0;
    private String lastMessage             = "";

    public AlertInstance(AlertRule rule, String serviceName) {
        this.rule        = rule;
        this.serviceName = serviceName;
    }

    /**
     * Evaluate current metric value against the rule.
     * Returns an AlertEvent if state changed (needs notification), else null.
     *
     * @param currentValue  current metric value (e.g. error rate = 47.0)
     * @param isAnomaly     whether EWMA considers this anomalous
     * @param pendingThreshold  how many consecutive breaches before FIRING
     * @param resolveThreshold  how many consecutive normal before RESOLVED
     */
    public AlertEvent evaluate(double currentValue, boolean isAnomaly,
                               int pendingThreshold, int resolveThreshold) {
        this.lastValue = currentValue;
        boolean breached = isBreached(currentValue, isAnomaly);

        return switch (state) {

            case INACTIVE -> {
                if (breached) {
                    consecutiveBreaches = 1;
                    transitionTo(AlertState.PENDING);
                    log.info("[ALERT PENDING] rule='{}' service='{}' value={}",
                            rule.getName(), serviceName, currentValue);
                }
                yield null; // no notification yet
            }

            case PENDING -> {
                if (breached) {
                    consecutiveBreaches++;
                    if (consecutiveBreaches >= pendingThreshold) {
                        transitionTo(AlertState.FIRING);
                        log.warn("[ALERT FIRING] rule='{}' service='{}' value={}",
                                rule.getName(), serviceName, currentValue);
                        yield buildAlertEvent(AlertEventType.FIRING, currentValue);
                    }
                } else {
                    // Recovered before confirming — back to inactive
                    consecutiveBreaches = 0;
                    transitionTo(AlertState.INACTIVE);
                }
                yield null;
            }

            case FIRING -> {
                if (!breached) {
                    consecutiveNormalReadings++;
                    if (consecutiveNormalReadings >= resolveThreshold) {
                        transitionTo(AlertState.RESOLVED);
                        log.info("[ALERT RESOLVED] rule='{}' service='{}' value={}",
                                rule.getName(), serviceName, currentValue);
                        yield buildAlertEvent(AlertEventType.RESOLVED, currentValue);
                    }
                } else {
                    consecutiveNormalReadings = 0;
                }
                yield null;
            }

            case RESOLVED -> {
                // Brief RESOLVED state — immediately transition back to INACTIVE
                transitionTo(AlertState.INACTIVE);
                consecutiveBreaches       = 0;
                consecutiveNormalReadings = 0;
                yield null;
            }
        };
    }

    private boolean isBreached(double value, boolean isAnomaly) {
        if (rule.isUseEwma()) return isAnomaly;
        return value >= rule.getThreshold();
    }

    private void    transitionTo(AlertState newState) {
        log.debug("Alert state transition: {} → {} | rule='{}' service='{}'",
                state, newState, rule.getName(), serviceName);
        state = newState;
        lastStateChangeTime = System.currentTimeMillis();
    }

    private AlertEvent buildAlertEvent(AlertEventType type, double value) {
        return new AlertEvent(
                rule.getRuleId(),
                rule.getName(),
                serviceName,
                type,
                value,
                rule.getThreshold(),
                rule.getChannel(),
                rule.getNotificationTarget(),
                System.currentTimeMillis()
        );
    }
}