package com.logforge.query.consumer;

import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import com.logforge.query.websocket.LiveTailBroadcaster;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to processed logs and broadcasts them to WebSocket clients.
 * Uses auto-offset-reset=latest so live tail only shows NEW events —
 * not replaying history from the beginning of the topic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QueryConsumer {

    private final LiveTailBroadcaster broadcaster;

    @KafkaListener(
            topics  = KafkaTopics.PROCESSED_LOGS,
            groupId = "query-service-group"
    )
    public void onEvent(LogEvent event) {
        broadcaster.broadcast(event);
    }
}