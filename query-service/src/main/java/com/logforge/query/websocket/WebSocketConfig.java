package com.logforge.query.websocket;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;

/**
 * Configures WebSocket with STOMP protocol.
 *
 * STOMP (Simple Text Oriented Messaging Protocol) is a simple
 * messaging protocol that runs over WebSocket. It gives us:
 * - Topics (/topic/logs) — broadcast to all subscribers
 * - Easy client libraries (SockJS + StompJS in the browser)
 *
 * HOW LIVE TAIL WORKS:
 * 1. Browser connects to ws://localhost:8086/ws
 * 2. Browser subscribes to /topic/logs (or /topic/logs/payment-service)
 * 3. When Kafka delivers a new log event, we broadcast it
 * 4. All subscribed browsers see it instantly — no polling needed
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // /topic prefix = broadcast topics (one-to-many)
        registry.enableSimpleBroker("/topic");
        // /app prefix = incoming messages from browser to server
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Main WebSocket endpoint — browsers connect here
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                .withSockJS(); // SockJS fallback for browsers without WebSocket
    }
}