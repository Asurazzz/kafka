package com.ymj.es;

import com.alibaba.fastjson.JSON;
import com.ymj.es.entity.Message;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * @author : yemingjie
 * @date : 2021/8/31 22:13
 */
public class MessageProducer {

    private kafka.javaapi.producer.Producer<String,String> producer;

    public static void main(String[] args) {
        new MessageProducer().start();
    }

    public void init(){
        Properties props = new Properties();
        /**
         * 用于自举（bootstrapping ），producer只是用它来获得元数据（topic, partition, replicas）
         * 实际用户发送消息的socket会根据返回的元数据来确定
         */
        props.put("metadata.broker.list", "localhost:9092");
        /**
         * 消息的序列化类
         * 默认是 kafka.serializer.DefaultEncoder， 输入时 byte[] 返回是同样的字节数组
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        /**
         * producer发送消息后是否等待broker的ACK，默认是0
         * 1 表示等待ACK，保证消息的可靠性
         */
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void produceMsg(Long i){
        // 构建发送的消息
        long timestamp = System.currentTimeMillis();
        Message message =  new Message();
        message.setId(i);
        message.setMsg("系统点击A");
        message.setSendTime(new Date());
        String msg = JSON.toJSONString(message);
        String topic = "topictest";  // 确保有这个topic
        System.out.println("发送消息" + msg);
        String key = "Msg-Key" + timestamp;

        /**
         * topic: 消息的主题
         * key：消息的key，同时也会作为partition的key
         * message:发送的消息
         */
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, msg);

        producer.send(data);
    }

    public void start() {
        System.out.println("开始发送消息 ...");
        Executors.newFixedThreadPool(10).execute(new Runnable() {
            public void run() {
                init();
                while (true) {
                    try {
                        for (Long i = 0L; i <10000 ; i++) {
                            produceMsg(Long.valueOf(i+"="+System.currentTimeMillis()));
                        }
//                        Thread.sleep(10);
                    } catch (Throwable e) {
                        if (producer != null) {
                            try {
                                producer.close();
                            } catch (Throwable e1) {
                                System.out.println("Turn off Kafka producer error! " + e);
                            }
                        }
                    }

                }

            }
        });
    }
}

