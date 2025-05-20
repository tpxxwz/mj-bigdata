package com.mj.basic4.partitioner.data;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class ProducerDemo {

    public static void main(String[] args) {
        // 1. 配置Producer参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mj01:6667");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // 可选优化参数
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // 2. 创建Producer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        // 3. 发送消息
        try {
            // 同步发送
            MjDeviceData d1 = new MjDeviceData("IND","11111");
            MjDeviceData d2 = new MjDeviceData("MED","2222");
            String dj1 = JSON.toJSON(d1).toString();
            String dj2 = JSON.toJSON(d2).toString();
            System.out.println(dj1);
            System.out.println(dj2);
            ProducerRecord<String, String> record1 =
                    new ProducerRecord<>("window", "key", dj1);
            ProducerRecord<String, String> record2 =
                    new ProducerRecord<>("window", "key", dj2);
            producer.send(record1).get(); // 阻塞等待结果
            producer.send(record2).get(); // 阻塞等待结果
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
