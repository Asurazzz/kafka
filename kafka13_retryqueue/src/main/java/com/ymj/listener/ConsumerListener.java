package com.ymj.listener;

import com.ymj.service.KafkaRetryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @Classname ConsumerListener
 * @Description TODO
 * @Date 2021/8/31 17:27
 * @Created by yemingjie
 */
@Component
public class ConsumerListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerListener.class);

    @Autowired
    private KafkaRetryService kafkaRetryService;

    private static int index = 0;

    @KafkaListener(topics = "${spring.kafka.topics.test}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            // 业务处理
            log.info("消费的消息：" + record);
            index++;
            if (index % 2 == 0) {
                throw new Exception("该重发了！");
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
            // 消息重试
            kafkaRetryService.consumerLater(record);
        }
    }
}
