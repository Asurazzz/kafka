package com.ymj.kafka.demo.producer;

import com.ymj.kafka.demo.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * 创建topic
 * kafka-topics.sh --zookeeper node1:2181/myKafka --create --topic tp_part_01 --partitions 4 --replication-factor 1
 * kafka-topics.sh --zookeeper node1:2181/myKafka --list
 * 使用控制台的消费者消费：kafka-console-consumer.sh --bootstrap-server node1:9092 --topic tp_part_01
 *
 * 然后启动该类，可以看到控制台输出了值
 * @author : yemingjie
 * @date : 2021/8/25 21:28
 */
public class MyProducer {

    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.195.131:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 自定义的分区器
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        // 此处不要设置partition的值，设置了就不会用到自定义的
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "tp_part_01",
                "mykey",
                "myvalue"
        );

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println("消息发送失败！");
                } else {
                    System.out.println(recordMetadata.topic());
                    System.out.println(recordMetadata.partition());
                    System.out.println(recordMetadata.offset());
                }
            }
        });

        // 关闭
        producer.close();
    }
}
