package com.logforge.query.websocket;

import com.logforge.common.model.LogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Broadcasts log events to WebSocket subscribers.
 *
 * Topics:
 *   /topic/logs              — ALL events from ALL services
 *   /topic/logs/{service}    — events from one specific service
 *   /topic/logs/errors       — only ERROR and FATAL events
 *
 * A browser can subscribe to one or many of these.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LiveTailBroadcaster {

    private final SimpMessagingTemplate messagingTemplate;

    public void broadcast(LogEvent event) {
        try {
            // Broadcast to all-events topic
            messagingTemplate.convertAndSend("/topic/logs", event);

            // Broadcast to service-specific topic
            if (event.getServiceName() != null) {
                messagingTemplate.convertAndSend(
                        "/topic/logs/" + event.getServiceName(), event);
            }

            // Broadcast to errors-only topic
            if (event.getLevel() == LogEvent.LogLevel.ERROR
                    || event.getLevel() == LogEvent.LogLevel.FATAL) {
                messagingTemplate.convertAndSend("/topic/logs/errors", event);
            }

        } catch (Exception e) {
            log.error("Failed to broadcast event to WebSocket: {}",
                    e.getMessage());
        }
    }
}