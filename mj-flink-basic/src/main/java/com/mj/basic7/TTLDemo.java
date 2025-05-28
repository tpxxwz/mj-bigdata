package com.mj.basic7;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟点击事件数据流（用户ID, 点击时间）
        DataStream<Tuple2<String, Long>> clicks = env.fromElements(
                Tuple2.of("user1", 1627800000000L),
                Tuple2.of("user1", 1627800001000L),
                Tuple2.of("user1", 1627800002000L),
                Tuple2.of("user1", 1627800003000L),
                Tuple2.of("user1", 1627800004000L),
                Tuple2.of("user1", 1627800005000L)
        );

        // 按用户ID分组处理
        clicks.keyBy(value -> value.f0)
                .process(new CountWithValueState())
                .print();

        env.execute("Click Counter");
    }

    // 自定义处理函数
    public static class CountWithValueState extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

        // 1 声明值状态
        private ValueState<Integer> countState;

        @Override
        public void open(OpenContext openContext) {
            // 配置TTL：1小时过期，读写时刷新时间，全量快照清理
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupFullSnapshot()
                    .build();
            // 2️ 初始化状态（相当于为每个用户创建记账本）
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("clickCounter", Integer.class);
            descriptor.enableTimeToLive(ttlConfig);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Long> click, Context ctx, Collector<String> out) throws Exception {
            // 3️ 读取当前状态值（查看记账本）
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0; // 第一次点击初始化
            }

            // 4️ 更新状态（在记账本上写新数字）
            currentCount += 1;
            countState.update(currentCount);

            // 达到5次点击触发提示
            if (currentCount >= 5) {
                out.collect("用户 " + click.f0 + " 达到第5次点击！");
            }
        }
    }
}
