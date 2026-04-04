package com.logforge.ingestion.consumer;

import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import com.logforge.ingestion.pipeline.IngestionPipeline;
import com.logforge.ingestion.storage.StorageRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes log events from Kafka and runs them through the pipeline.
 *
 * concurrency=12 means Spring creates 12 consumer threads,
 * each reading from a different partition — matching our 12-partition topic.
 *
 * REAL LIFE ANALOGY:
 * 12 workers, each responsible for their own section of the conveyor belt.
 * They never interfere with each other — total parallelism.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LogEventConsumer {

    private final IngestionPipeline pipeline;
    private final StorageRouter     storageRouter;

    @KafkaListener(
            topics          = KafkaTopics.RAW_LOGS,
            groupId         = "ingestion-group",
            concurrency     = "12",       // 12 parallel consumer threads
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, LogEvent> record,
                        Acknowledgment acknowledgment) {
        LogEvent rawEvent = record.value();

        try {
            // Stamp Kafka metadata onto the event
            LogEvent withMeta = LogEvent.builder()
                    .eventId(rawEvent.getEventId())
                    .timestamp(rawEvent.getTimestamp())
                    .ingestedAt(rawEvent.getIngestedAt())
                    .level(rawEvent.getLevel())
                    .serviceName(rawEvent.getServiceName())
                    .hostName(rawEvent.getHostName())
                    .environment(rawEvent.getEnvironment())
                    .category(rawEvent.getCategory())
                    .message(rawEvent.getMessage())
                    .stackTrace(rawEvent.getStackTrace())
                    .traceId(rawEvent.getTraceId())
                    .spanId(rawEvent.getSpanId())
                    .sourceIp(rawEvent.getSourceIp())
                    .country(rawEvent.getCountry())
                    .city(rawEvent.getCity())
                    .tags(rawEvent.getTags())
                    .kafkaPartition(record.partition())
                    .kafkaOffset(record.offset())
                    .build();

            // Run through the full pipeline
            pipeline.process(withMeta)
                    .ifPresent(storageRouter::route);

            // Manual acknowledgment — only commit offset after successful processing
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process event — eventId: {}, error: {}",
                    rawEvent != null ? rawEvent.getEventId() : "null",
                    e.getMessage(), e);
            // Still acknowledge to avoid infinite retry loop
            // In production: send to dead-letter topic instead
            acknowledgment.acknowledge();
        }
    }
}