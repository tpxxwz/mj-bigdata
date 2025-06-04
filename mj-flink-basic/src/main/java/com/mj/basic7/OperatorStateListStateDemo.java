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

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class OperatorStateListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 必须设为1（SourceFunction不支持并行执行）
        env.getCheckpointConfig().setCheckpointInterval(5000L);
        DataStream<Long> inputStream = env.addSource(new CounterSource());

        inputStream
                .map(new MapFunction<Long, String>() {
                    @Override
                    public String map(Long value) {
                        return "当前总数: " + value;
                    }
                })
                .print();

        env.execute("OperatorState with ListState Demo");
    }

    // 自定义数据源（带状态）
    public static class CounterSource implements SourceFunction<Long>, CheckpointedFunction {
        private volatile boolean isRunning = true;
        private long currentCount = 0L;  // 使用基本类型避免空指针
        private ListState<Long> state;   // OperatorState中的ListState

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    currentCount++;
                    ctx.collect(currentCount);
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        // ============== 状态管理核心方法 ==============
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 1. 清空旧状态
            state.clear();
            // 2. 写入当前状态值
            state.add(currentCount);

            // 生产环境应移除调试输出
            System.out.println("Checkpoint保存: " + currentCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 状态描述符（名称+类型）
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                    "counterState",
                    TypeInformation.of(new TypeHint<Long>() {})
            );

            // 获取或创建状态
            state = context.getOperatorStateStore().getListState(descriptor);

            // 状态恢复逻辑
            if (context.isRestored()) {
                // 处理可能的多个状态值（取最大值）
                long maxValue = 0L;
                for (Long value : state.get()) {
                    maxValue = Math.max(maxValue, value);
                }
                currentCount = maxValue;
                System.out.println("从检查点恢复: " + currentCount);
            }
        }
    }
}