package com.logforge.sdk.config;

import com.logforge.common.model.LogEvent;
import com.logforge.sdk.buffer.LogEventRingBuffer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration class — Spring Boot picks this up automatically
 * when log-agent-sdk is on the classpath.
 *
 * Any app that adds this dependency to their pom.xml gets:
 * - Ring buffer wired up
 * - Kafka publisher running
 * - HTTP filter active
 * - LogCaptureService available for injection
 *
 * Zero boilerplate for the importing app.
 */
@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(LogAgentProperties.class)
@ComponentScan(basePackages = "com.logforge.sdk")
public class LogAgentAutoConfiguration {

    @Bean
    public LogEventRingBuffer logEventRingBuffer(LogAgentProperties properties) {
        return new LogEventRingBuffer(properties.getBufferSize());
    }

    @Bean
    public KafkaTemplate<String, LogEvent> logEventKafkaTemplate(
            LogAgentProperties properties) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getKafkaBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);
        // Batch settings for throughput
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // Reliability
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);

        ProducerFactory<String, LogEvent> factory =
                new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(factory);
    }
}
