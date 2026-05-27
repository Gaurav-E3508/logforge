package com.logforge.stream.consumer;

import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import com.logforge.stream.window.WindowManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes processed log events from Kafka and feeds them into
 * the sliding window manager for real-time aggregation.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamConsumer {

    private final WindowManager windowManager;

    @KafkaListener(
            topics  = KafkaTopics.PROCESSED_LOGS,
            groupId = "stream-processor-group"
    )
    public void onEvent(LogEvent event) {
        try {
            windowManager.record(event);
        } catch (Exception e) {
            log.error("Failed to record event in window manager: {}",
                    e.getMessage());
        }
    }
}