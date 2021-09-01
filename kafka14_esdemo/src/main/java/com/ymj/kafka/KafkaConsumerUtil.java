package com.ymj.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * kafka消息端控制层
 * @author : yemingjie
 * @date : 2021/8/31 22:00
 */
public class KafkaConsumerUtil {

    private Logger log = LoggerFactory.getLogger(KafkaConsumerUtil.class);

    /**
     * 锁
     */
    private static ReentrantLock lock = new ReentrantLock();

    private KafkaConsumer<String, String> initConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.195.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费消息组名称
        props.put("group.id", "CMMtest");
        props.put("enable.auto.commit", "false");
        // 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        // 如果采用latest，消费者只能得道其启动后，生产者生产的消息
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");

        return new KafkaConsumer<String, String>(props);
    }

    public void syncPull() {
        try {
            ScheduledExecutorService ses = Executors.newScheduledThreadPool(10);
            // 如果前面的任务还未完成，则调度不会启动。
            ses.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        log.info("run being:"
                                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS").format(new Date()));
                        execute();
                        log.info("run end:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS").format(new Date()));
                    } catch (Exception e) {
                        log.error("syncPull_run_error:" + e.getMessage());
                    }
                }
            }, 30, 1, TimeUnit.SECONDS);
        } catch (Exception ex) {
            log.error("syncPull_error:" + ex.getMessage());
        }
    }

    /**
     * 执行
     */
    private void execute() {
        if (lock.tryLock()) {
            KafkaConsumer<String, String> consumer = null;
            try {
                consumer = initConsumer();
                ArrayList<String> list = new ArrayList<>();
                list.add("topicteset");
                consumer.subscribe(list);

                // 循环次数
                int foreach = 5;
                for (int k = 0; k < foreach; k++) {
                    log.info("foreach is :" + (k + 1));
                    // 若无消息则停在这里,一直等待消息
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    // 消息集合
                    List<String> recordValues = new ArrayList<String>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        if (null == partitionRecords  || partitionRecords.size() == 0) {
                            continue;
                        }
                        // 默认500
                        log.info("get partitionRecords size is :" + partitionRecords.size());
                        // 清空集合
                        recordValues.clear();
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            log.info(
                                    "now consumer the message it's offset is :"
                                            + record.offset() + " key:"
                                            + record.key()
                                            + " and the value is :"
                                            + record.value());
                            recordValues.add(record.value());
                        }


                        // 插入数据
//                        int result = mongoDBUtil.insertDB(recordValues);
//                        if (result == 1) {
//                            long lastOffset = partitionRecords
//                                    .get(partitionRecords.size() - 1).offset();
//                            LOGGER.info("now commit the partition[ "
//                                    + partition.partition() + "] offset");
//                            // 消费消息修改状态
//                            consumer.commitSync(Collections.singletonMap(
//                                    partition,
//                                    new OffsetAndMetadata(lastOffset + 1)));
//                        }


                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                log.error("[KafkaConsumerUtil.execute]" + e.getMessage());
            } finally {
                if (null != consumer) {
                    consumer.close();
                }
                lock.unlock();
            }
        }
    }





}
