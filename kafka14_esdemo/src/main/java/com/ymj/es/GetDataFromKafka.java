package com.ymj.es; /**
  *接收消息队列
  */

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetDataFromKafka implements Runnable {

    private String topic;
    private String path;
    
    public GetDataFromKafka(String topic,String path) {
        this.path=path;
        this.topic = topic;
    }
    public static void main(String[] args) {
        GetDataFromKafka gdkast=new GetDataFromKafka("topictest", "d:\\clusterMonitor.rrd");
        new Thread(gdkast).start();
    }   
    @Override
    public void run() {
        System.out.println("start runing consumer");

        Properties properties = new Properties();
        properties.put("zookeeper.connect",
                "127.0.0.1:2181");// 声明zk
        properties.put("group.id", "CMMtest");// 必须要使用别的组名称，                                                // 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        properties.put("auto.offset.reset", "largest"); 
        ConsumerConnector consumer = Consumer
                .createJavaConsumerConnector(new ConsumerConfig(properties));
        // TODO Auto-generated method stub

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer
                .createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            //hostName+";"+ip+";"+commandName+";"+res+";"+System.currentTimeMillis();
            //这里指的注意，如果没有下面这个语句的执行很有可能回从头来读消息的
            consumer.commitOffsets();
            System.out.println(message);
//client
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "my-application").build();
            // 创建client
            TransportClient client = null;
            try {
                client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            } catch (UnknownHostException e) {

            }


            IndexResponse response = client.prepareIndex("accounts", "person")
                    .setSource(message, XContentType.JSON)
                    .get();

            String _index = response.getIndex();
            System.out.println(_index);



        }
    }
}