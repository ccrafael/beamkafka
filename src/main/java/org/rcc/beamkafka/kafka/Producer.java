package org.rcc.beamkafka.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;

@Component
public class Producer {

    @Autowired
    private KafkaTemplate<String, Integer> kafkaTemplate;

    @Autowired
    private KafkaConfig config;

    void send(Integer key, Integer value) {
        kafkaTemplate.send(config.topicName, String.valueOf(key), value);
    }

    void send(Integer value) {
        IntStream.range(0, 5).forEach(key -> send(key, value));
        kafkaTemplate.flush();
    }

    public static void main(String[] args) {
        ApplicationContext ctx = new AnnotationConfigApplicationContext("org.rcc.beamkafka");

        Producer producer = ctx.getBean(Producer.class);
        IntStream.range(0, 100_000).forEach(producer::send);

    }

}
