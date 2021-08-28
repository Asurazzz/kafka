package com.ymj.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 *
 * kafka-console-producer.sh --broker-list node1:9092 --topic tp_demo_01
 * @author : yemingjie
 * @date : 2021/8/27 21:12
 */
public class MyConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.195.131:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "mygrp");
//        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "myclient");

        // 如果在kafka中找不到当前消费者的偏移量，则设置为最旧的
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 配置拦截器   One -> Two -> Three  接收消息和发送偏移量确认都是这个顺序
        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.ymj.kafka.interceptor.OneInterceptor" +
                ",com.ymj.kafka.interceptor.TwoInterceptor" +
                ",com.ymj.kafka.interceptor.ThreeInterceptor");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singleton("tp_demo_01"));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(3_000);
            records.forEach(record -> {
                System.out.println(record.topic() + "\t"
                        + record.partition() + "\t"
                        + record.offset() + "\t"
                        + record.key() + "\t"
                        + record.value());
            });
        }



    }

}
