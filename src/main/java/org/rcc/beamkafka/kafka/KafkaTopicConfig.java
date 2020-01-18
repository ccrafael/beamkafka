package org.rcc.beamkafka.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin kafkaAdmin(KafkaConfig config) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapAddress);

        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1(KafkaConfig config) {
        return new NewTopic(config.topicName, 2, (short) 2);
    }
}
