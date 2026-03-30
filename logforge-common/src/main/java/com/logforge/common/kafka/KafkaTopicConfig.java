package com.logforge.common.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares all Kafka topics as Spring Beans.
 * Spring Kafka automatically creates these topics on startup
 * if they don't already exist.
 */
@Configuration
public class KafkaTopicConfig {


    @Bean
    public NewTopic rawLogsTopic() {
        return TopicBuilder
                .name(KafkaTopics.RAW_LOGS)
                .partitions(KafkaTopics.RAW_LOGS_PARTITIONS)
                .replicas(KafkaTopics.DEFAULT_REPLICATION_FACTOR)
                .config("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)) // 7 days
                .build();
    }

    @Bean
    public NewTopic processedLogsTopic() {
        return TopicBuilder
                .name(KafkaTopics.PROCESSED_LOGS)
                .partitions(KafkaTopics.PROCESSED_LOGS_PARTITIONS)
                .replicas(KafkaTopics.DEFAULT_REPLICATION_FACTOR)
                .config("retention.ms", String.valueOf(3 * 24 * 60 * 60 * 1000L)) // 3 days
                .build();
    }

    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder
                .name(KafkaTopics.ALERTS)
                .partitions(3)
                .replicas(KafkaTopics.DEFAULT_REPLICATION_FACTOR)
                .build();
    }

    @Bean
    public NewTopic metricsTopic() {
        return TopicBuilder
                .name(KafkaTopics.METRICS)
                .partitions(3)
                .replicas(KafkaTopics.DEFAULT_REPLICATION_FACTOR)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder
                .name(KafkaTopics.DEAD_LETTER)
                .partitions(3)
                .replicas(KafkaTopics.DEFAULT_REPLICATION_FACTOR)
                .config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000L)) // 30 days
                .build();
    }
}