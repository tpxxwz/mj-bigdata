package com.mj.basic5.watermark;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

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
            for (int i = 0; i < 2; i++) {
                //Thread.sleep(4000);
                // 同步发送
                UserWindow userWindow = new UserWindow("10000", random.nextInt(500), System.currentTimeMillis());
                String wd = JSON.toJSON(userWindow).toString();
                System.out.println(wd);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("window", "key-" + i, wd);
                RecordMetadata metadata = producer.send(record).get(); // 阻塞等待结果
           /*     System.out.printf("Sent record(key=%s value=%s) to partition=%d offset=%d%n",
                        record.key(), record.value(), metadata.partition(), metadata.offset());*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
