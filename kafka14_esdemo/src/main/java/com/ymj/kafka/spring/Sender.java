package com.ymj.kafka.spring;

import com.alibaba.fastjson.JSONObject;
import com.ymj.kafka.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * @author : yemingjie
 * @date : 2021/8/31 22:11
 */
@Component
public class Sender {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    public void sendMessage(){

        Message message=new Message();
        message.setId(System.currentTimeMillis());
        message.setMsg("Test Kafka send");
        message.setSendTime(new Date());
        // 这里的test是要发送的topic,message要发送的消息内容
        kafkaTemplate.send("topictest", JSONObject.toJSON(message).toString());
    }
}
