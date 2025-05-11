package com.mj.basic4.kafka;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.OrderInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class IntervalJoinDemo {
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

        String click1 = "{\"userId\": \"user_001\", \"clickTime\": 1717027200000, \"adId\": \"ad_123\"}";      // 2024-05-30 00:00:00
        String click2 = "{\"userId\": \"user_002\", \"clickTime\": 1717030800000, \"adId\": \"ad_456\"}";      // 2024-05-30 01:00:00

        ProducerRecord<String, String> clickPro1 =
                new ProducerRecord<>("user_clicks", "key", click1);
            ProducerRecord<String, String> clickPro2 =
                    new ProducerRecord<>("user_clicks", "key", click2);
        String purchase1 = "{\"userId\": \"user_001\", \"purchaseTime\": 1717029000000, \"amount\": 99.99}";     // 2024-05-30 00:30:00（匹配成功）
        String purchase2 = "{\"userId\": \"user_002\", \"purchaseTime\": 1717034400000, \"amount\": 199.99}";     // 2024-05-30 02:00:00（超出1小时，不匹配）
        ProducerRecord<String, String> purchasesPro1 =
                new ProducerRecord<>("user_purchases", "key-", purchase1);
        ProducerRecord<String, String> purchasesPro2 =
                new ProducerRecord<>("user_purchases", "key-", purchase2);
        producer.send(clickPro1).get(); // 阻塞等待结果
        producer.send(clickPro2).get(); // 阻塞等待结果
        producer.send(purchasesPro1).get(); // 阻塞等待结果
        producer.send(purchasesPro2).get(); // 阻塞等待结果

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
