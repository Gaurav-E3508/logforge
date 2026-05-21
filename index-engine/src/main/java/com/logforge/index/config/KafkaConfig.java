package com.logforge.index.config;

import com.logforge.common.model.LogEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, LogEvent> indexConsumerFactory() {
        JsonDeserializer<LogEvent> deserializer =
                new JsonDeserializer<>(LogEvent.class, false);
        deserializer.addTrustedPackages("com.logforge.common.model");

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "index-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(
                config, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LogEvent>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, LogEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(indexConsumerFactory());
        return factory;
    }
}