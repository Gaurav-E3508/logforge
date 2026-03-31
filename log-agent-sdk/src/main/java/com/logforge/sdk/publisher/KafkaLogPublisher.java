package com.logforge.sdk.publisher;

import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import com.logforge.sdk.buffer.LogEventRingBuffer;
import com.logforge.sdk.config.LogAgentProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Background publisher — drains the ring buffer and sends events to Kafka.
 *
 * REAL LIFE ANALOGY:
 * This is the delivery truck that comes to the restaurant every 500ms,
 * picks up all the order slips that piled up (drain), and delivers them
 * to headquarters (Kafka) in one batch trip — much more efficient than
 * one car per order slip.
 *
 * The @Scheduled annotation makes Spring call flushBuffer() every 500ms
 * automatically — no manual thread management needed.
 */
@Slf4j
@Component
public class KafkaLogPublisher {

    private final KafkaTemplate<String, LogEvent> kafkaTemplate;
    private final LogEventRingBuffer ringBuffer;
    private final LogAgentProperties properties;

    public KafkaLogPublisher(KafkaTemplate<String, LogEvent> kafkaTemplate,
                             LogEventRingBuffer ringBuffer,
                             LogAgentProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.ringBuffer    = ringBuffer;
        this.properties    = properties;
    }

    /**
     * Runs every `batchFlushMs` milliseconds.
     * Drains the ring buffer and sends events to Kafka in a batch.
     */
    @Scheduled(fixedDelayString = "${logforge.batch-flush-ms:500}")
    public void flushBuffer() {
        if (ringBuffer.isEmpty()) return;

        List<LogEvent> batch = ringBuffer.drain(properties.getBatchSize());
        if (batch.isEmpty()) return;

        log.debug("Flushing {} log events to Kafka topic: {}", batch.size(), KafkaTopics.RAW_LOGS);

        for (LogEvent event : batch) {
            // Use serviceName as the Kafka key — this ensures all logs
            // from the same service go to the same partition (ordering guarantee)
            kafkaTemplate.send(KafkaTopics.RAW_LOGS, event.getServiceName(), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish log event to Kafka: {}", ex.getMessage());
                            // Put it back? For now we log and move on.
                            // In production you'd have a retry queue here.
                        }
                    });
        }
    }
}