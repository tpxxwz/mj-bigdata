package com.mj.basic7;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟包裹数据流（快递员ID, 包裹重量kg）
        DataStream<Tuple2<String, Integer>> packages = env.fromElements(
                Tuple2.of("courier1", 20),
                Tuple2.of("courier1", 35),
                Tuple2.of("courier2", 50),
                Tuple2.of("courier1", 30),
                Tuple2.of("courier2", 60)
        );

        // 按快递员ID分组处理
        packages.keyBy(value -> value.f0)
                .process(new WeightReducer())
                .print();

        env.execute("Package Weight Monitoring");
    }

    // 自定义处理函数
    public static class WeightReducer extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {

        // 1️ 声明归约状态（存储累计重量）
        private ReducingState<Integer> totalWeightState;

        @Override
        public void open(OpenContext openContext) {
            // 2️ 初始化归约状态（定义累加规则）
            ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>(
                    "totalWeight",
                    (currentWeight, newWeight) -> currentWeight + newWeight,  // 归约函数（加法）
                    Integer.class
            );
            totalWeightState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> pkg, Context ctx, Collector<String> out) throws Exception {
            // 3️ 将新包裹重量合并到总重量
            totalWeightState.add(pkg.f1);

            // 4️ 获取当前总重量
            Integer currentTotal = totalWeightState.get();

            // 触发超重告警
            if (currentTotal > 100) {
                out.collect("⚠️ 快递员 " + pkg.f0 + " 包裹超重！当前重量：" + currentTotal + "kg");
                totalWeightState.clear(); // 清空状态（模拟卸货）
            }
        }
    }
}
