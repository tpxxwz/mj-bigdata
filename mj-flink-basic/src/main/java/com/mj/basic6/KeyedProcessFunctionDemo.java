package com.mj.basic6;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟设备温度数据流 (设备ID, 温度, 时间戳)
        DataStream<Tuple3<String, Double, Long>> inputStream = env.fromElements(
                Tuple3.of("device_1", 55.0, System.currentTimeMillis()),
                Tuple3.of("device_1", 52.0, System.currentTimeMillis() + 1000),
                Tuple3.of("device_1", 58.0, System.currentTimeMillis() + 2000),
                Tuple3.of("device_2", 48.0, System.currentTimeMillis() + 3000),
                Tuple3.of("device_2", 53.0, System.currentTimeMillis() + 4000)
        );

        inputStream
                // 按设备ID分组
                .keyBy(value -> value.f0)
                // 应用处理逻辑
                .process(new TemperatureAlertFunction(50.0, 3))
                .print();

        env.execute("Temperature Alert Job");
    }

    // 自定义处理函数
    public static class TemperatureAlertFunction
            extends KeyedProcessFunction<String, Tuple3<String, Double, Long>, String> {

        private final Double threshold;  // 温度阈值
        private final Integer maxCount;  // 触发报警的连续次数

        // 状态：记录连续超标的次数
        private ValueState<Integer> counterState;

        public TemperatureAlertFunction(Double threshold, Integer maxCount) {
            this.threshold = threshold;
            this.maxCount = maxCount;
        }

        @Override
        public void open(OpenContext openContext) {
            // 初始化状态（存储连续超标次数）
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("counter", Integer.class);
            counterState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                Tuple3<String, Double, Long> event,
                Context ctx,
                Collector<String> out) throws Exception {

            Integer currentCount = counterState.value() == null ? 0 : counterState.value();

            if (event.f1 > threshold) {
                currentCount += 1;
                counterState.update(currentCount);

                // 当连续超标次数达到阈值
                if (currentCount >= maxCount) {
                    out.collect(String.format(
                            "[ALERT] 设备 %s 连续 %d 次温度超标！当前温度 %.1f℃",
                            event.f0, currentCount, event.f1
                    ));
                    counterState.clear();  // 报警后重置计数器
                }

                // 注册1小时后清除状态的定时器（使用处理时间）
                long clearTime = ctx.timerService().currentProcessingTime() + 3600_000;
                ctx.timerService().registerProcessingTimeTimer(clearTime);

            } else {
                // 温度正常时重置计数器
                counterState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            // 定时器触发时清除状态
            counterState.clear();
        }
    }
}