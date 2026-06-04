package com.logforge.ingestion.config;

import com.logforge.common.model.LogEvent;
import com.logforge.ingestion.dedup.BloomFilter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${logforge.ingestion.bloom-filter-capacity:1000000}")
    private int bloomFilterCapacity;

    @Value("${logforge.ingestion.bloom-filter-error-rate:0.01}")
    private double bloomFilterErrorRate;


    @Bean
    public KafkaTemplate<String, LogEvent> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        ProducerFactory<String, LogEvent> factory =
                new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public ConsumerFactory<String, LogEvent> consumerFactory() {
        JsonDeserializer<LogEvent> deserializer =
                new JsonDeserializer<>(LogEvent.class, false);
        deserializer.addTrustedPackages("com.logforge.common.model");

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "ingestion-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // manual ack
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);     // batch size per poll

        return new DefaultKafkaConsumerFactory<>(config,
                new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LogEvent>
    kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, LogEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(12);
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public BloomFilter bloomFilter() {
        return new BloomFilter(bloomFilterCapacity, bloomFilterErrorRate);
    }
}