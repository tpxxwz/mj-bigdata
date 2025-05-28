package com.mj.basic7;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

public class StateBeginDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "8081"); // 设置WebUI端口为8081
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        // 模拟用户行为数据流（用户ID, 页面名称）
        // 2. 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics("window")
                .setGroupId("mj-flink-basic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 3. 从Kafka源创建数据流
        SingleOutputStreamOperator<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        ).uid("KafkaSource-1").name("KafkaSource-1");
        // 4. 解析JSON数据
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)
        ).uid("Map-1").name("Map-1");

        // 按用户ID分组处理金额汇总
        DataStream<String> result = parsedStream.keyBy(WaterMarkData::getUserId)
                .process(new MoneySumProcessFunction()).uid("Process-1").name("Process-1");

        // 数据写入Kafka
        result.print();

        env.execute("Money Sum Tracking");
    }

    // 自定义处理函数-金额累加
    public static class MoneySumProcessFunction
            extends KeyedProcessFunction<String, WaterMarkData, String> {
        // 声明值状态（保存当前用户的金额总和）
        private ValueState<Integer> sumState;

        @Override
        public void open(OpenContext openContext) {
            // 初始化值状态（默认值为0）
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("moneySum", Integer.class);
            sumState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(WaterMarkData data, Context ctx, Collector<String> out) throws Exception {
            // 获取当前状态值（若为空则默认为0）
            Integer currentSum = sumState.value() == null ? 0 : sumState.value();
            // 累加新金额
            Integer newSum = currentSum + data.getMoney();
            // 更新状态
            sumState.update(newSum);
            // 输出当前总金额
            out.collect("用户 " + data.getUserId()
                    + " 当前总金额: " + newSum);
        }
    }
}
