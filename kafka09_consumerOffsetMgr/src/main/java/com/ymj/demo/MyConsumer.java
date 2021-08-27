package com.ymj.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @Classname MyConsumer
 * @Description TODO
 * @Date 2021/8/27 10:07
 * @Created by yemingjie
 */
public class MyConsumer {
    public static void main(String[] args) {
        Map<String, Object> configs= new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.241:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // group.id很重要
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "mygrp1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);

        consumer.subscribe(Arrays.asList("tp_demo_01"));
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(3_000);
            records.forEach(new Consumer<ConsumerRecord<String, String>>() {
                @Override
                public void accept(ConsumerRecord<String, String> record) {
                    System.out.println(record);
                }
            });
        }
    }
}
