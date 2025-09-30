package com.mj.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class KafkaUtils {

    /**
     * 创建 KafkaSource 公共方法
     *
     * @param bootstrapServers Kafka 集群地址（逗号分隔）
     * @param topic            订阅的主题
     * @param groupId          消费者组ID
     * @param startingOffset   起始偏移量（默认 latest）
     * @return KafkaSource<String>
     */
    public static KafkaSource<String> createKafkaSource(
            String bootstrapServers,
            String topic,
            String groupId,
            OffsetsInitializer startingOffset) {

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(startingOffset)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    // 重载方法（简化调用，默认从 latest 开始消费）
    public static KafkaSource<String> createKafkaSource(
            String bootstrapServers,
            String topic,
            String groupId) {
        return createKafkaSource(bootstrapServers, topic, groupId,
                OffsetsInitializer.latest());
    }

    // 重载方法（简化调用，默认从 latest 开始消费）
    public static KafkaSource<String> createKafkaSourceEar(
            String bootstrapServers,
            String topic,
            String groupId) {
        return createKafkaSource(bootstrapServers, topic, groupId,
                OffsetsInitializer.earliest());
    }

    private static AdminClient adminClient;
    private static KafkaProducer<String, String> producer;

    private static AdminClient getAdminClient() {
        if (adminClient == null) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "mj01:6667");
            adminClient = AdminClient.create(props);
        }
        return adminClient;
    }

    private static KafkaProducer<String, String> getProducer() {
        if (producer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "mj01:6667");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }
        return producer;
    }

    public static void recrate(String... topics) {
        deleteTopics(topics);
        createTopicsIfNotExist(topics);
    }

    public static void createTopicsIfNotExist(String... topics) {
        try {
            AdminClient adminClient = getAdminClient();
            Set<String> existingTopics = adminClient.listTopics().names().get();
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                if (!existingTopics.contains(topic)) {
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                    newTopics.add(newTopic);
                }
            }
            if (!newTopics.isEmpty()) {
                adminClient.createTopics(newTopics).all().get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteTopics(String... topics) {
        try {
            AdminClient adminClient = getAdminClient();
            Set<String> existingTopics = adminClient.listTopics().names().get();
            Set<String> deleteTopics = new HashSet<>();
            for (String existingTopic : topics) {
                if (existingTopics.contains(existingTopic)) {
                    deleteTopics.add(existingTopic);
                }
            }
            adminClient.deleteTopics(deleteTopics).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void sendMessage(String topic, String message, boolean print) {
        sendMessage(topic, message, null, print);
    }

    public static void sendMessage(String topic, String message, String key, boolean print) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        getProducer().send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else if (print) {
//                System.out.printf("message %s Sent to topic %s partition %d with offset %d%n",
//                        message, topic, metadata.partition(), metadata.offset());
                System.out.printf("%s\n", message);
            }
        });
    }

}
