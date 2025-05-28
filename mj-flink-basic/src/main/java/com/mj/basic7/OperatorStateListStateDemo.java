package com.mj.basic7;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

public class OperatorStateListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 设置并行度为2

        // 自定义数据源（每秒生成一个数字）
        DataStream<Long> inputStream = env.addSource(new CounterSource());

        inputStream
                .map(new MapFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "当前总数: " + value;
                    }
                })
                .print();

        env.execute("OperatorState with ListState Demo");
    }

    // 自定义数据源（带状态）
    public static class CounterSource implements SourceFunction<Long>, CheckpointedFunction {
        private volatile boolean isRunning = true;
        private Long currentCount = 0L; // 当前计数
        private ListState<Long> state;  // OperatorState中的ListState

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    currentCount++;
                    ctx.collect(currentCount); // 发送计数
                }
                Thread.sleep(1000); // 每秒发送一次
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        // ----------------- 状态管理核心方法 -----------------
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 保存状态：清空旧值，写入当前计数
            state.clear();
            state.add(currentCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 初始化状态：从检查点恢复或创建新状态
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                    "counterState",
                    TypeInformation.of(new TypeHint<Long>() {})
            );
            state = context.getOperatorStateStore().getListState(descriptor);

            // 恢复状态（如果是故障重启）
            if (context.isRestored()) {
                for (Long value : state.get()) {
                    currentCount = value; // 取最后一个值（合并后的状态可能有多个值）
                }
            }
        }
    }
}