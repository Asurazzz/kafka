package com.ymj.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * @Classname KafkaService
 * @Description TODO
 * @Date 2021/8/31 17:22
 * @Created by yemingjie
 */
@Service
public class KafkaService {


    private Logger logger = LoggerFactory.getLogger(KafkaService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public String sendMessage(ProducerRecord<String, String> record) throws Exception{
        SendResult<String, String> result = this.kafkaTemplate.send(record).get();
        RecordMetadata metadata = result.getRecordMetadata();
        String retrunResult = metadata.topic() + "\t" + metadata.partition() + "\t" + metadata.offset();
        logger.info("发送消息成功：" + retrunResult);
        return retrunResult;
    }
}
