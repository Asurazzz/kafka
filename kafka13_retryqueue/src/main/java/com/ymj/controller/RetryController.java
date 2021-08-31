package com.ymj.controller;

import com.ymj.service.KafkaService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Classname RetryController
 * @Description TODO
 * @Date 2021/8/31 17:17
 * @Created by yemingjie
 */
@RestController
public class RetryController {

    @Autowired
    private KafkaService kafkaService;

    @Value("${spring.kafka.topics.test}")
    private String topic;

    @RequestMapping("/send/{message}")
    public String sendMessage(@PathVariable String message) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                message
        );

        String result = kafkaService.sendMessage(record);
        return result;
    }
}
