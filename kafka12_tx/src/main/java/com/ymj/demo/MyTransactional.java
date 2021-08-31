package com.ymj.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @Classname MyTransactional
 * @Description
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic tp_tx_out_01 --partitions 1 --replication-factor 1
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tp_tx_out_01 --isolation-level read_committed --from-beginning
 * @Date 2021/8/31 11:39
 * @Created by yemingjie
 */
public class MyTransactional {

    public static KafkaProducer<String, String> getProducer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.241:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 设置client_id
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "tx_producer_01");

        // 设置事务id
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx_id_02");

        // 需要所有ISR确认
        configs.put(ProducerConfig.ACKS_CONFIG, "all");

        // 启用幂等性
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        return producer;
    }

    public static KafkaConsumer<String, String> getConsumer(String consumerGroupId) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.241:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 设置消费组id
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_grp_02");

        // 不启用消费者偏移量的自动确认
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_client_02");

        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 只读取已提交的消息
        // configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        return consumer;
    }

    public static void main(String[] args) {
        String consumerGroupId = "consumer_grp_id_02";

        KafkaProducer<String, String> producer = getProducer();
        KafkaConsumer<String, String> consumer = getConsumer(consumerGroupId);

        producer.initTransactions();

        // 订阅主题
        consumer.subscribe(Collections.singleton("tp_tx_01"));

        // 消费消息
        final ConsumerRecords<String, String> records = consumer.poll(1_000);
        // 开启事务
        producer.beginTransaction();

        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

            for (ConsumerRecord<String, String> record : records) {

                System.out.println(record);

                producer.send(new ProducerRecord<>("tp_tx_out_01", record.key(), record.value()));

                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)); // 偏移量表示下一条要消费的消息
            }

            // 将该消息的偏移量提交作为事务的一部分，随事务提交和回滚
            producer.sendOffsetsToTransaction(offsets, consumerGroupId);

            //int i = 1 / 0;

            // 提交事务
            producer.commitTransaction();
        } catch (Exception ex) {
            ex.printStackTrace();
            // 回滚事务
            producer.abortTransaction();
        } finally {
            // 关闭资源
            producer.close();
            consumer.close();
        }


    }
}
