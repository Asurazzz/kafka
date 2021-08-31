package com.ymj.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @Classname MyTransactionProducer
 * @Description
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic tp_tx_01  --partitions 1 --replication-factor 1
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tp_tx_01 --from-beginning
 * @Date 2021/8/31 10:00
 * @Created by yemingjie
 */
public class MyTransactionProducer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.241:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 提供生产者client_id
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "tx_producer");

        // 提供生产者事务id
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id_1");

        // 需要ISR全体确认消息
        configs.put(ProducerConfig.ACKS_CONFIG, "all");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        // 初始化事务
        producer.initTransactions();

        try {
            // 开启事务
            producer.beginTransaction();
            // 发送事务消息
            producer.send(new ProducerRecord<>("tp_tx_01", "txkey1", "txmsg4"));
            producer.send(new ProducerRecord<>("tp_tx_01", "txkey2", "txmsg5"));
            producer.send(new ProducerRecord<>("tp_tx_01", "txkey3", "txmsg6"));

            // 测试回滚
            int i = 1/0;
            // 提交事务
            producer.commitTransaction();
        } catch (Exception ex) {
            // 事务回滚
            producer.abortTransaction();
        } finally {
            // 关闭生产者
            producer.close();
        }

    }
}
