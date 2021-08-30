package com.ymj.kafka.producer;

/**
 *
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic tp_demo_02 --partitions 3 --replication-factor 1
 * kafka-topics.sh --zookeeper localhost:2181/myKafka --describe --topic tp_demo_02
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list    列出当前kafka上的消费组
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group  消费组的偏移量
 *
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group group --topic tp_demo_02 --to-earliest
 * 重置在group消费组里面topic为tp_demo_02的主题的偏移量为最早，此时并没有执行，再加上 --execute 才会执行。
 *
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group group --topic tp_demo_02:0 --shift-by -10 --execute
 * 将group消费组中为tp_demo_02的topic的0号分区的偏移量往前移动10个
 *
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group group --topic tp_demo_02 --to-latest
 * 将偏移量移到最新
 *
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group group --topic tp_demo_02:0,2 --shift-by -5 --execute
 * 将group消费组中主题为tp_demo_02的0号分区和2号分区的偏移量向左移动5个
 * @author : yemingjie
 * @date : 2021/8/29 22:05
 */
public class MyProducer {
    public static void main(String[] args) {
        Thread thread = new Thread(new ProducerHandler("hello ymj"));
        thread.start();
    }
}
