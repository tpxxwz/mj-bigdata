package com.mj.basic7;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631 
 * @desc 版权所有，请勿外传
 */
public class AggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建Kafka数据源配置
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")       // Kafka集群地址
                .setTopics("window")                    // 订阅的主题
                .setGroupId("mj-flink-basic")           // 消费者组ID
                .setStartingOffsets(OffsetsInitializer.latest())  // 从最新偏移量开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 字符串反序列化器
                .build();

        // 3. 从Kafka源创建数据流（不设置水位线）
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),  // 禁用水位线（根据场景需要可调整）
                "kafka-source"                     // 数据源名称
        );

        // 4. 解析JSON数据为Java对象（WaterMarkData是自定义数据结构）
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)  // FastJSON解析
        );

        // 5. 按用户ID分组并处理
        parsedStream.keyBy(value -> value.getUserId())  // 根据用户ID分组
                .process(new AvgProcessFunciton())      // 使用自定义处理函数
                .print();                               // 打印计算结果

        // 6. 启动流处理任务
        env.execute();
    }

    /**
     * 自定义处理函数 - 计算每个用户的平均金额
     * 输入类型: WaterMarkData
     * 输出类型: String
     */
    public static class AvgProcessFunciton extends KeyedProcessFunction<String, WaterMarkData, String> {

        // 声明聚合状态对象：存储累加器(Tuple2<总和, 计数>)和最终结果(Double)
        private AggregatingState<Integer, Double> vcAvgAggregatingState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 初始化聚合状态
            vcAvgAggregatingState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                            "vcAvgAggregatingState",  // 状态名称
                            new avgFunction(),         // 聚合函数实现
                            Types.TUPLE(Types.INT, Types.INT)  // 累加器类型信息
                    )
            );
        }

        @Override
        public void processElement(
                WaterMarkData value,  // 输入数据
                Context ctx,           // 上下文信息（可访问时间、键等）
                Collector<String> out  // 输出收集器
        ) throws Exception {
            // 将当前数据的金额添加到聚合状态
            vcAvgAggregatingState.add(value.getMoney());

            // 从聚合状态获取当前平均值
            Double vcAvg = vcAvgAggregatingState.get();

            // 输出结果：用户ID及其对应的平均金额
            out.collect("用户Id为:" + value.getUserId() + ",平均金额为:" + vcAvg);
        }
    }

    /**
     * 自定义聚合函数 - 计算整数平均值
     * 输入类型: Integer (单个金额)
     * 累加器: Tuple2<Integer, Integer> (f0=总和, f1=计数)
     * 输出类型: Double (平均值)
     */
    public static class avgFunction implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double> {

        // 创建初始累加器 (0, 0)
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        // 添加新值到累加器：更新总和和计数
        @Override
        public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
        }

        // 从累加器计算最终结果：总和/计数
        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return accumulator.f0 * 1D / accumulator.f1;  // 转换为double避免整数除法
        }

        // 合并两个累加器（用于会话窗口等需要合并的场景）
        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}