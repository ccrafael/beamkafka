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

    public void send(Integer i) {
        kafkaTemplate.send(config.topicName, String.valueOf(i), i);
        kafkaTemplate.flush();

    }

    public static void main(String [] args) {
        ApplicationContext ctx = new AnnotationConfigApplicationContext("org.rcc.beamkafka");

        Producer producer = ctx.getBean(Producer.class);

        for (int j = 0; j < 100_000; j ++) {
            IntStream.range(1, 4).forEach(i -> producer.send(i));
        }

    }

}
