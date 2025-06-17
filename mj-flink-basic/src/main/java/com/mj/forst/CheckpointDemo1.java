package com.mj.forst;


import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.state.forst.ForStOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 码界探索
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * @desc: CheckpointDemo1
 */
public class CheckpointDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "forst");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://mj01:8020/flink-chk");
        config.set(ForStOptions.PRIMARY_DIRECTORY, "hdfs://mj01:8020/forst");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        env.configure(config);
        // 2. 配置检查点存储
        env.enableCheckpointing(5000);

        // Kafka源配置（保持不变）
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics("window")
                .setGroupId("mj-flink-basic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "kafka-source"
        ).uid("kafka-1").name("kafka-1");

        // 解析JSON数据
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)
        ).uid("map-1").name("map-1");

        // 按用户ID分组处理金额汇总
        parsedStream.keyBy(WaterMarkData::getUserId)
                .process(new MoneySumProcessFunction()).uid("process-1").name("process-1")
                .print();

        env.execute("Money Sum Tracking with RocksDB");
    }

    // 改造后的处理函数
    public static class MoneySumProcessFunction
            extends KeyedProcessFunction<String, WaterMarkData, String> {

        private ValueState<Integer> sumState;

        @Override
        public void open(OpenContext openContext) {
            // 1. 配置状态TTL（自动清理过期状态）
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            // 2. 创建状态描述符（RocksDB需要明确序列化）
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("moneySum", Integer.class);
            //ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("moneySum", DoubleSerializer.INSTANCE);

            descriptor.enableTimeToLive(ttlConfig);

            sumState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(WaterMarkData data, Context ctx, Collector<String> out) throws Exception {
            // 同步状态访问（更稳定）
            Integer currentSum = sumState.value();
            if (currentSum == null) {
                currentSum = 0;
            }

            int newSum = currentSum + data.getMoney();
            sumState.update(newSum);

            // 输出结果
            out.collect("用户 " + data.getUserId() +
                    " 当前总金额: " + newSum);
        }
    }
}
