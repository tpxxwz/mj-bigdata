package com.mj.basic4.kafka;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.OrderInfo;
import com.mj.bean.PaymentEvent;
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
public class OrderInfoProducerDemo {

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
            OrderInfo offOrder = new OrderInfo("1003", random.nextInt(500), System.currentTimeMillis());
            PaymentEvent onOrder = new PaymentEvent("100002", System.currentTimeMillis());
            String offData = JSON.toJSON(offOrder).toString();
            String onData = JSON.toJSON(onOrder).toString();
            ProducerRecord<String, String> offTopic =
                    new ProducerRecord<>("orders", "key", offData);
            ProducerRecord<String, String> onTopic =
                    new ProducerRecord<>("payments", "key", onData);
            producer.send(offTopic).get(); // 阻塞等待结果
            producer.send(onTopic).get(); // 阻塞等待结果
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
