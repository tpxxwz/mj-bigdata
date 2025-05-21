package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.basic5.data.WaterMarkData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class TumpWindowProducerDemo3 {

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
            //WaterMarkData userWindow = new WaterMarkData("10000","2025-05-20 17:00:00", 100);
            WaterMarkData userWindow = new WaterMarkData("10000","2025-05-20 17:00:10", 100);
            //WaterMarkData userWindow = new WaterMarkData("10000","2025-05-20 17:00:09", 100);
            //WaterMarkData userWindow = new WaterMarkData("10000","2025-05-20 17:00:20", 100);
            String data = JSON.toJSON(userWindow).toString();
            System.out.println(data);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("window", "key", data);
            producer.send(record).get(); // 阻塞等待结果

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
