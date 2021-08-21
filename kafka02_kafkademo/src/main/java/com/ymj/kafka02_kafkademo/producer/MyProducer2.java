package com.ymj.kafka02_kafkademo.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import scala.Int;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : yemingjie
 * @date : 2021/8/21 14:31
 */
public class MyProducer2 {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "192.168.195.131:9092");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "topic_1",
                0,
                1,
                "lagou message 2"
        );


//        producer.send(record, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                if (exception == null) {
//                    System.out.println(
//                            "主题：" + metadata.topic() + "\n"
//                                    + "分区：" + metadata.partition() + "\n"
//                                    + "偏移量：" + metadata.offset() + "\n"
//                                    + "序列化的key字节：" + metadata.serializedKeySize() + "\n"
//                                    + "序列化的value字节：" + metadata.serializedValueSize() + "\n"
//                                    + "时间戳" + metadata.timestamp()
//                    );
//                } else {
//                    System.out.println("有异常：" + exception.getMessage());
//                }
//            }
//        });
        // 使用回调异步等待消息的确认
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(
                        "主题：" + metadata.topic() + "\n"
                        + "分区：" + metadata.partition() + "\n"
                        + "偏移量：" + metadata.offset() + "\n"
                        + "序列化的key字节：" + metadata.serializedKeySize() + "\n"
                        + "序列化的value字节：" + metadata.serializedValueSize() + "\n"
                        + "时间戳" + metadata.timestamp()
                );
            } else {
                System.out.println("有异常：" + exception.getMessage());
            }
        });

        // 关闭连接
        producer.close();
    }
}
