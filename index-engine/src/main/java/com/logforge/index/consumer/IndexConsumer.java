package com.logforge.index.consumer;

import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import com.logforge.index.service.IndexService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.stereotype.Component;

/**
 * Listens on the PROCESSED_LOGS topic (output of ingestion service)
 * and indexes every event for full-text search.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IndexConsumer {

    private final IndexService indexService;

    @KafkaListener(
            topics  = KafkaTopics.PROCESSED_LOGS,
            groupId = "index-group"
    )
    public void onEvent(LogEvent event) {
        try {
            indexService.index(event);
        } catch (Exception e) {
            log.error("Failed to index event: {} — {}",
                    event.getEventId(), e.getMessage());
        }
    }
}