package com.mj.basic4.sink;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.MjOrderInfo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SinkDemo1 {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 模拟输入数据流（实际场景可能来自Kafka/Socket等）
        DataStream<MjOrderInfo> orders = env.fromElements(
                new MjOrderInfo("order1","user1", 199, System.currentTimeMillis()),
                new MjOrderInfo("order2","user2", 299, System.currentTimeMillis()),
                new MjOrderInfo("order11","user1", 599, System.currentTimeMillis()),
                new MjOrderInfo("order22","user2", 99, System.currentTimeMillis()),
                new MjOrderInfo("order111","user2", 899, System.currentTimeMillis())
        );
        // 1. 按用户分组（根据userId进行KeyBy）
        KeyedStream<MjOrderInfo, String> keyedOrders = orders.keyBy(order -> order.getUserId());
        // 2. 计算每个用户的总金额（累加操作）
        DataStream<MjOrderInfo> totalAmount = keyedOrders.sum("amount");
        //转成json格式
        DataStream<String> result=  totalAmount.map(order -> JSON.toJSON(order).toString());

        // 配置 Kafka Sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("mj01:6667")  // Kafka集群地址
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sink1")  // 目标Topic
                        .setValueSerializationSchema(new SimpleStringSchema())  // 字符串序列化
                        .build()
                )
                // 写到kafka的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("flink-form-sink1-")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000 + "")
                .build();

        // 数据写入Kafka
        result.sinkTo(kafkaSink);
        env.execute("订单数据分析");
    }
}
