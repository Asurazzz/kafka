package com.ymj.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author : yemingjie
 * @date : 2021/8/25 22:15
 */
public class InterceptorOne implements ProducerInterceptor<Integer, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InterceptorOne.class);

    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
        System.out.println("拦截器1 -- go");
        // 消息发送的时候，经过拦截器，调用该方法

        // 要发送的消息内容
        final String topic = record.topic();
        final Integer partition = record.partition();
        final Integer key = record.key();
        final String value = record.value();
        final Long timestamp = record.timestamp();
        final Headers headers = record.headers();

        // 拦截器拦下来之后根据原来消息创建的新的消息
        // 此处对原消息可以做改动
        ProducerRecord<Integer, String> newRecord = new ProducerRecord<>(
                topic,
                partition,
                timestamp,
                key,
                value,
                headers
        );
        // 传递新消息
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("拦截器1 -- back");
        // 消息确认或者异常的时候，调用该方法，该方法中不应实现较重的任务
        // 会影响kafka生产者的性能
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Object classContent = configs.get("classContent");
        System.out.println(classContent);
    }
}
