package com.mj.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class KafkaUtils {

    /**
     * 创建 KafkaSource 公共方法
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
}
