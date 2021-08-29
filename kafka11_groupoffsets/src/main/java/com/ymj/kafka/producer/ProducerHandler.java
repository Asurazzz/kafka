package com.ymj.kafka.producer;

/**
 * @author : yemingjie
 * @date : 2021/8/29 22:06
 */
public class ProducerHandler implements Runnable{

    private String message;

    public ProducerHandler(String message) {
        this.message = message;
    }

    @Override
    public void run() {
        KafkaProducerSingleton kafkaProducerSingleton = KafkaProducerSingleton.getInstance();
        kafkaProducerSingleton.init("tp_demo_02", 3);
        int i = 0;

        while (true) {
            try {
                System.out.println("当前线程:" + Thread.currentThread().getName()
                        + "\t获取的kafka实例:" + kafkaProducerSingleton);
                kafkaProducerSingleton.sendKafkaMessage("发送消息: " + message + " " + (++i));
                Thread.sleep(100);
            } catch (Exception ex) {

            }
        }
    }
}
