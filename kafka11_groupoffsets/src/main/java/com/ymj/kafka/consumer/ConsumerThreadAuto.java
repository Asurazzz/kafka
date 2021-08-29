package com.ymj.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author : yemingjie
 * @date : 2021/8/29 22:29
 */
public class ConsumerThreadAuto implements Runnable {
    private ConsumerRecords<String, String> records;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThreadAuto(ConsumerRecords<String, String> records,
                              KafkaConsumer<String, String> consumer) {
        this.records = records;
        this.consumer = consumer;
    }

    @Override
    public void run() {

        for(ConsumerRecord<String,String> record : records){
            System.out.println("当前线程:" + Thread.currentThread()
                    + "\t主题:" + record.topic()
                    + "\t偏移量:" + record.offset() + "\t分区:" + record.partition()
                    + "\t获取的消息:" + record.value());
        }
    }
}
