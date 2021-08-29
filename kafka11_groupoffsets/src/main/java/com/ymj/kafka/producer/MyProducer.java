package com.ymj.kafka.producer;

/**
 *
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic tp_demo_02 --partitions 3 --replication-factor 1
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --describe --topic tp_demo_02
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list    列出当前kafka上的消费组
 *
 * @author : yemingjie
 * @date : 2021/8/29 22:05
 */
public class MyProducer {
    public static void main(String[] args) {
        Thread thread = new Thread(new ProducerHandler("hello ymj"));
        thread.start();
    }
}
