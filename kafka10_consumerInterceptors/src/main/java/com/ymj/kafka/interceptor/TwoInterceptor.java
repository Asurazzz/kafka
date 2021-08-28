package com.ymj.kafka.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author : yemingjie
 * @date : 2021/8/27 21:28
 */
public class TwoInterceptor implements ConsumerInterceptor<String, String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        // poll方法返回结果之前最后要调用的方法

        System.out.println("Two -- 开始");
        // 消息不做处理直接返回
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // 消费者提交偏移量的时候，结果该方法
        System.out.println("Two -- 结束");

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 用于获取消费者的设置参数
        configs.forEach((k, v) -> {
            System.out.println(k + "\t" + v);
        });
    }
}
