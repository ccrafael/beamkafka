package org.rcc.beamkafka.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:application.properties")
public class KafkaConfig {

    @Value(value = "${topic}")
    String topicName;

    @Value(value = "${kafka.bootstrapAddress}")
    String bootstrapAddress;
}
