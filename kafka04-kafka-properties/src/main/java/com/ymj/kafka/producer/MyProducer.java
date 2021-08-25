package com.ymj.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Classname MyProducer
 * @Description TODO
 * @Date 2021/8/25 13:28
 * @Created by yemingjie
 */
public class MyProducer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();

        configs.put("bootstrap.servers", "node1:9092,node2:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        configs.put("acks", "all");
        configs.put("compression.type", "gzip");
        configs.put("retries", 3);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
    }
}
