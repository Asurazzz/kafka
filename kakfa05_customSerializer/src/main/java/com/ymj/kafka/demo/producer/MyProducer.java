package com.ymj.kafka.demo.producer;

import com.ymj.kafka.demo.entity.User;
import com.ymj.kafka.demo.serialization.UserSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic tp_user_01 --partitions 3 --replication-factor 1
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tp_user_01
 * @Classname MyProducer
 * @Description TODO
 * @Date 2021/8/26 9:52
 * @Created by yemingjie
 */
public class MyProducer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.240:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);

        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(configs);

        User user = new User();
        user.setUserId(1001);
        user.setUsername("张三");

        ProducerRecord record = new ProducerRecord(
                "tp_user_01",
                0,
                user.getUsername(),
                user
        );

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("消息发送成功："
                        + metadata.topic() + "\t"
                        + metadata.partition() + "\t"
                        + metadata.offset());
            } else {
                System.out.println("消息发送异常");
            }
        });

        // 关闭生产者
        producer.close();
    }
}
