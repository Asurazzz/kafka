package com.ymj.kafka02_kafkademo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author : yemingjie
 * @date : 2021/8/21 14:25
 */
public class MyProducer1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> configs = new HashMap<>();
        // 设置连接Kafka的初始连接用到的服务器地址
        // 如果是集群，则可以通过此初始连接发现集群中的其他broker
        configs.put("bootstrap.servers", "192.168.195.131:9092");
        // 设置key的序列化器
        configs.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        // 设置value的序列化器
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("acks", "1");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);

        // 用于封装Producer的消息
        ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                "topic_1", // 主题名称
                0, // 分区编号,现在只有一个分区，所以是0
                0, //数字作为key
                "message 0"   // 字符串作为value
        );

        // 发送消息，同步等待消息的确认
        producer.send(record).get(3_000, TimeUnit.MILLISECONDS);

        // 关闭生产者
        producer.close();
    }
}
