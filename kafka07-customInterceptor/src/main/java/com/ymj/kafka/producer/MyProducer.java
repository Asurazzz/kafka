package com.ymj.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic tp_inter_01 --partitions 4 --replication-factor 1
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --list
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tp_inter_01
 * @author : yemingjie
 * @date : 2021/8/25 22:04
 */
public class MyProducer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.195.131:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 如果有多个拦截器，则设置为多个拦截器的全限定类名，中间用逗号隔开
        configs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.ymj.kafka.interceptor.InterceptorOne," +
                "com.ymj.kafka.interceptor.InterceptorTwo," +
                "com.ymj.kafka.interceptor.InterceptorThree");

        configs.put("classContent", "this is ymj`s kafka class");


        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(configs);

        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "tp_inter_01",
                0,
                1001,
                "this is ymj`s 1001 message"
        );

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                    System.out.println("消息发送成功："
                                    + metadata.topic() + "\t"
                                    + metadata.partition() + "\t"
                            + metadata.offset());

                } else {
                    System.out.println("消息发送异常");
                }
            }
        });

        // 关闭
        producer.close();
    }
}
