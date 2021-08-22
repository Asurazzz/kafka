package com.ymj.kafka.demo.controller;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @author : yemingjie
 * @date : 2021/8/22 22:57
 */
@RestController
public class KafkaSyncProducerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("send/sync/{message}")
    public String sendSync(@PathVariable String message) {
        ListenableFuture future = kafkaTemplate.send(
                new ProducerRecord<Integer, String>(
                        "topic-spring-02",
                        0,
                        1,
                        message
                        ));
        try {
            // 同步等待broker的响应
            Object o = future.get();
            SendResult<Integer,String> result = (SendResult<Integer, String>) o;
            System.out.println(result.getRecordMetadata().topic()
                    + result.getRecordMetadata().partition()
                    + result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return "success";
    }
}
