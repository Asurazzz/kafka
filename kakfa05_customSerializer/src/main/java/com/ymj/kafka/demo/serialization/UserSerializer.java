package com.ymj.kafka.demo.serialization;

import com.ymj.kafka.demo.entity.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @Classname UserSerializer
 * @Description TODO
 * @Date 2021/8/25 17:29
 * @Created by yemingjie
 */
public class UserSerializer implements Serializer<User> {
    @Override
    public void configure(Map map, boolean b) {
        // 用于接收对序列化器的配置参数，并对当前序列化器进行配置和初始化
    }

    @Override
    public byte[] serialize(String topic, User data) {
        try {
            if (data == null) {
                return null;
            }
            Integer userId = data.getUserId();
            String username = data.getUsername();

            int length = 0;
            byte[] bytes = null;
            if (username != null) {
                bytes = username.getBytes(StandardCharsets.UTF_8);
                length = bytes.length;
            }
            // 第一个4个字节用于存储userId的值
            // 第二个4个字节用于存储username字节数组的长度int值
            // 第三个存储username序列化
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + length);
            buffer.putInt(userId);
            buffer.putInt(length);
            buffer.put(bytes);

            return buffer.array();

        } catch (Exception ex) {
            throw new SerializationException("序列化数据异常");
        }

    }

    @Override
    public void close() {
        // 用于关闭资源等操作。需要幂等，即多次调用效果一样
    }
}
