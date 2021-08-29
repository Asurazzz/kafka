package com.ymj.kafka.consumer;

/**
 * @author : yemingjie
 * @date : 2021/8/29 22:24
 */
public class ConsumerAutoMain {
    public static void main(String[] args) {
        KafkaConsumerAuto kafka_consumerAuto = new KafkaConsumerAuto();
        try {
            kafka_consumerAuto.execute();
            Thread.sleep(20000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            kafka_consumerAuto.shutdown();
        }
    }
}
