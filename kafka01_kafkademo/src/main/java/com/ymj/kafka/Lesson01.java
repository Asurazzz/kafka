package com.ymj.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author : yemingjie
 * @date : 2021/6/26 9:08
 */
public class Lesson01 {


    /**
     * 创建topic
     * kafka-topics.sh --zookeeper node03:2181/kafka --create --topic msb-items
     * --partitions 2   --replication-factor 2
     */
    @Test
    public void producer() throws ExecutionException, InterruptedException {

        String topic = "ymj-items";
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.195.132:9092,192.168.195.133:9092,192.168.195.134:9092");
        // kafka 持久化数据的MQ  数据 -> byte[]
        // 不会对数据进行干预，双方要约定编码
        // kafka是一个app：使用零拷贝  sendfile 系统调用实现快速数据消费
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(p);

        // 现在的producer就是一个提供者，面向的是broker

        /**
         * msb-items
         * 2 partition
         * 三种商品，每种商品有个线性的3个ID
         * 相同商品最好去一个分区里面
         */
        while (true) {
            for (int i = 0 ; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(topic, "item" + j, "val" + i);
                    Future<RecordMetadata> send = producer.send(record);

                    RecordMetadata rm = send.get();
                    int partition = rm.partition();
                    long offset = rm.offset();
                    System.out.println("key:" + record.key() + " val:" + record.value() + " partition:" + partition +
                            " offset:" + offset);

                }
            }
        }


    }



    @Test
    public void consumer() {
        // 基础配置
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.195.132:9092,192.168.195.133:9092,192.168.195.134:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费的细节
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX01");
        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "latest");

        // 自动提交时候异步提交， 丢数据&&重复数据
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // 一个运行的consumer，自己会维护自己消费进度
        // 一旦自动提交但是是异步的
        // 1.还没到时间，挂了，没提交。重启一个consumer，参照offset的时候会重复消费
        // 2.一个批次的数据还没写数据库成功，但是这个批次的offset被异步提交了，挂了，重启一个consumer，参照offset的时候会丢失消息

        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        // consumer.subscribe(Arrays.asList("ymj-items"));

        consumer.subscribe(Arrays.asList("ymj-items"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("--onPartitionsRevoked:");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("--onPartitionsAssigned:");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }
        });


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

            if (!records.isEmpty()) {

                System.out.println("===========" + records.count() + "============");
                // 每次poll的时候是取多个分区的数据
                Set<TopicPartition> partitions = records.partitions();
                // 且每个分区内的数据都是有序的

                /**
                 * 如果手动提交offset
                 * 1.按照消息进度同步提交
                 * 2.按照分区粒度同步提交
                 * 3.按照当前poll批次同步提交
                 *
                 * 思考：如果在多线程下
                 * 1.以上1的方式不用多线程
                 * 2.以上2的方式使用多线程处理
                 */
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                    // 在一个微批里，按分区获取poll回来的数据
                    // 线性按分区处理，还可以并行按分区处理多线程的方式
                    Iterator<ConsumerRecord<String, String>> piter = pRecords.iterator();
                    while (piter.hasNext()) {
                        ConsumerRecord<String, String> record = piter.next();
                        int par = record.partition();
                        long offset = record.offset();
                        System.out.println("key: " + record.key() + " val:" + record.value() +
                                " partition:" + par + " offset:" + offset);
                    }
                }

//                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
//                while (iterator.hasNext()) {
//                    // 因为一个consumer可以消费多个分区，但是一个分区只能给一个组里面的一个consumer消费
//                    ConsumerRecord<String, String> record = iterator.next();
//                    int partition = record.partition();
//                    long offset = record.offset();
//                    System.out.println("key: " + record.key() + " val:" + record.value() +
//                            " partition:" + partition + " offset:" + offset);
//                }
            }

        }
    }
}




















