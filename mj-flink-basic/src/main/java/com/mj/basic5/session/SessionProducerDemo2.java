package com.mj.basic5.session;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SessionProducerDemo2 {

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
            WaterMarkData userWindow1 = new WaterMarkData("10000","2025-05-20 17:00:00", 100);
            WaterMarkData userWindow2 = new WaterMarkData("10000","2025-05-20 17:00:06", 100);
            WaterMarkData userWindow3 = new WaterMarkData("10000","2025-05-20 17:00:20", 100);
            String data1 = JSON.toJSON(userWindow1).toString();
            String data2 = JSON.toJSON(userWindow2).toString();
            String data3 = JSON.toJSON(userWindow3).toString();
            System.out.println(data1);
            System.out.println(data2);
            System.out.println(data3);
            ProducerRecord<String, String> record1 =
                    new ProducerRecord<>("window", "key", data1);
            ProducerRecord<String, String> record2 =
                    new ProducerRecord<>("window", "key", data2);
            ProducerRecord<String, String> record3 =
                    new ProducerRecord<>("window", "key", data3);
            //producer.send(record1).get(); // 阻塞等待结果
            Thread.sleep(1000);
            //producer.send(record2).get(); // 阻塞等待结果
            Thread.sleep(1000);
            producer.send(record3).get(); // 阻塞等待结果
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
