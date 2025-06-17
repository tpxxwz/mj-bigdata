package com.mj.forst;

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
public class CheckpointProducerDemo1 {

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
            for (int i = 0; i < 20000; i++) {
                Thread.sleep(1000);
                // 同步发送
                WaterMarkData userWindow1 = new WaterMarkData(random.nextInt(1000000)+"",System.currentTimeMillis(), 100);
                WaterMarkData userWindow2 = new WaterMarkData(random.nextInt(1000000)+"",System.currentTimeMillis(),100);
                String wd1 = JSON.toJSON(userWindow1).toString();
                String wd2 = JSON.toJSON(userWindow2).toString();
                System.out.println(wd1);
                System.out.println(wd2);
                ProducerRecord<String, String> record1 =
                        new ProducerRecord<>("window", "key-" + i, wd1);
                ProducerRecord<String, String> record2 =
                        new ProducerRecord<>("window", "key-" + i, wd2);
                producer.send(record1).get(); // 阻塞等待结果
                producer.send(record2).get(); // 阻塞等待结果
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
