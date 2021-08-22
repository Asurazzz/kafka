package com.ymj.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author : yemingjie
 * @date : 2021/8/22 23:11
 */
@Component
public class MyConsumer {
    @KafkaListener(topics = "topic-spring-02")
    public void onMessage(ConsumerRecord<Integer, String> record) {
        Optional<ConsumerRecord<Integer, String>> optional = Optional.ofNullable(record);
        if (optional.isPresent()) {
            System.out.println(
                    record.topic() + "\t"
                    + record.partition() + "\t"
                    + record.offset() + "\t"
                    + record.key() + "\t"
                    + record.value()
            );
        }
    }
}
