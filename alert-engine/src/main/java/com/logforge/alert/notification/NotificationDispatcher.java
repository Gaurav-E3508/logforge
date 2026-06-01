package com.logforge.alert.notification;

import com.logforge.alert.statemachine.AlertEvent;
import com.logforge.alert.statemachine.AlertEventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Dispatches alert notifications to the appropriate channel.
 *
 * Currently supports: LOG_ONLY (for dev), with stubs for
 * Slack, Email, Webhook — easy to wire up in production.
 *
 * REAL LIFE ANALOGY:
 * The alarm dispatcher. When the fire alarm fires, they call
 * the fire station (Slack), send a pager to the on-call engineer
 * (Email), and trigger the automated response system (Webhook).
 */
@Slf4j
@Component
public class NotificationDispatcher {

    public void dispatch(AlertEvent event) {
        String emoji  = event.getType() == AlertEventType.FIRING ? "🔴" : "✅";
        String status = event.getType() == AlertEventType.FIRING
                ? "ALERT FIRING" : "ALERT RESOLVED";

        String message = String.format(
                "%s [%s] %s — service: %s | value: %.1f | threshold: %.1f",
                emoji, status, event.getRuleName(),
                event.getServiceName(), event.getCurrentValue(), event.getThreshold()
        );

        switch (event.getChannel()) {
            case SLACK   -> sendSlack(event, message);
            case EMAIL   -> sendEmail(event, message);
            case WEBHOOK -> sendWebhook(event, message);
            default      -> log.warn("NOTIFICATION → {}", message);
        }
    }

    private void sendSlack(AlertEvent event, String message) {
        // Production: POST to Slack Incoming Webhook URL
        // https://hooks.slack.com/services/...
        log.info("[SLACK → {}] {}", event.getNotificationTarget(), message);
    }

    private void sendEmail(AlertEvent event, String message) {
        // Production: use Spring Mail / AWS SES
        log.info("[EMAIL → {}] {}", event.getNotificationTarget(), message);
    }

    private void sendWebhook(AlertEvent event, String message) {
        // Production: HTTP POST to notificationTarget URL
        log.info("[WEBHOOK → {}] {}", event.getNotificationTarget(), message);
    }
}