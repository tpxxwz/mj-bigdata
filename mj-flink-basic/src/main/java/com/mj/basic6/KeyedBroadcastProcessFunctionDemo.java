package com.mj.basic6;

import com.mj.bean.Alert;
import com.mj.bean.FraudRule;
import com.mj.bean.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class KeyedBroadcastProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟交易数据流（主流）
        DataStream<Transaction> transactionStream = env
                .fromElements(
                        new Transaction("user1", 500.0, "Beijing"),
                        new Transaction("user2", 12000.0, "Shanghai"),
                        new Transaction("user3", 8000.0, "Guangzhou")
                )
                .keyBy(t -> t.getUserId());  // 按用户分区

        // 模拟规则更新流（广播流）
        DataStream<FraudRule> ruleStream = env
                .fromElements(
                        new FraudRule("rule1", 10000.0),
                        new FraudRule("rule2", 7000.0)  // 动态更新规则
                );

        // 将规则流广播
        MapStateDescriptor<String, FraudRule> ruleDescriptor =
                new MapStateDescriptor<>("rules", String.class, FraudRule.class);
        BroadcastStream<FraudRule> broadcastRuleStream = ruleStream.broadcast(ruleDescriptor);

        // 连接流并处理
        transactionStream
                .connect(broadcastRuleStream)
                .process(new FraudDetectionProcessFunction())
                .print();

        env.execute("Dynamic Fraud Detection");
    }
    public static class FraudDetectionProcessFunction
            extends KeyedBroadcastProcessFunction<String, Transaction, FraudRule, Alert> {

        // 定义广播状态描述符
        private final MapStateDescriptor<String, FraudRule> ruleDescriptor =
                new MapStateDescriptor<>("rules", String.class, FraudRule.class);

        @Override
        public void processElement(
                Transaction transaction,
                ReadOnlyContext ctx,
                Collector<Alert> out) throws Exception {

            // 从广播状态获取规则
            ReadOnlyBroadcastState<String, FraudRule> rules =
                    ctx.getBroadcastState(ruleDescriptor);

            // 检查所有规则
            for (Map.Entry<String,FraudRule>  rule : rules.immutableEntries()) {
                FraudRule rRule = rule.getValue();
                if (transaction.getAmount() > rRule.getMaxAmount()) {
                    out.collect(new Alert("规则 " + rRule.getRuleId() + " 触发: " + transaction,System.currentTimeMillis()));
                }
            }
        }

        @Override
        public void processBroadcastElement(
                FraudRule rule,
                Context ctx,
                Collector<Alert> out) throws Exception {

            // 更新广播状态
            BroadcastState<String, FraudRule> rules =
                    ctx.getBroadcastState(ruleDescriptor);
            rules.put(rule.getRuleId(), rule);
            System.out.println("更新规则: " + rule.getRuleId());
        }
    }
}