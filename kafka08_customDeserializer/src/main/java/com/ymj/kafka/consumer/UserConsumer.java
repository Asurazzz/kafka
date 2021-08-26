package com.ymj.kafka.consumer;

import com.ymj.kafka.deserializer.UserDeserializer;
import com.ymj.kafka.entity.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * kafka-console-consumer.sh --bootstrap-server 192.168.1.240:9092 --topic tp_user_01 --from-beginning
 * @author : yemingjie
 * @date : 2021/8/26 22:16
 */
public class UserConsumer {

    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.195.131:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user_consumer");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_id");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, User> consumer = new KafkaConsumer<String, User>(configs);

        // 订阅主题
        consumer.subscribe(Collections.singleton("tp_user_01"));
        final ConsumerRecords<String, User> records = consumer.poll(Long.MAX_VALUE);

        records.forEach(record -> {
            System.out.println(record.value());
        });

        consumer.close();
    }
}
