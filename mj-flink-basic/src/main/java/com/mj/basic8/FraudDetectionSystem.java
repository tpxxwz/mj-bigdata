package com.mj.basic8;

import com.mj.bean.TransactionBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * @author 码界探索
 * 微信: 252810631 
 * @desc 版权所有，请勿外传
 */
public class FraudDetectionSystem {
    public static void main(String[] args) throws Exception {
        // 1. 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义水印策略（处理事件时间和乱序事件）
        WatermarkStrategy<TransactionBean> transWatermarkStrategy =
                WatermarkStrategy
                        // 允许2秒的乱序时间窗口
                        .<TransactionBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 从事件中提取时间戳作为事件时间
                        .withTimestampAssigner((event, ts) -> event.getTimestamp());

        // 3. 创建模拟交易事件流
        DataStream<TransactionBean> TransactionBeanStream = env.fromElements(
                        // 测试数据说明：
                        new TransactionBean("TX001", "userA", 50.0, "success", "2025-05-29 15:00:00"),   // 小额测试 (userA)
                        new TransactionBean("TX002", "userB", 2000.0, "success", "2025-05-29 15:00:00"), // 中等金额 (不会触发规则)
                        new TransactionBean("TX003", "userA", 5000.0, "success", "2025-05-29 15:01:00"),  // 大额转账 (与TX001间隔1分钟)
                        new TransactionBean("TX004", "userC", 80.0, "success", "2025-05-29 15:00:00"),    // 小额测试 (userC)
                        new TransactionBean("TX005", "userC", 6000.0, "success", "2025-05-29 15:06:00")   // 大额转账 (与TX004间隔6分钟，超出时间窗口)
                )
                // 为数据流分配时间戳和水印
                .assignTimestampsAndWatermarks(transWatermarkStrategy);

        // 4. 定义欺诈检测模式（复杂事件处理模式）
        Pattern<TransactionBean, ?> fraudPattern = Pattern
                // 第一步：匹配小额测试交易
                .<TransactionBean>begin("smallTx")
                .where(new SimpleCondition<TransactionBean>() {
                    @Override
                    public boolean filter(TransactionBean tx) {
                        // 条件：交易成功且金额小于100
                        return "success".equals(tx.status) && tx.amount < 100;
                    }
                })
                // 第二步：紧接着发生的大额交易
                .next("largeTx")  // 使用next()确保两个事件连续发生
                .where(new SimpleCondition<TransactionBean>() {
                    @Override
                    public boolean filter(TransactionBean tx) {
                        // 条件：交易成功且金额大于3000
                        return "success".equals(tx.status) && tx.amount > 3000;
                    }
                })
                // 时间窗口限制：两步操作需在5分钟内完成
                .within(Duration.ofMinutes(5));

        // 5. 将模式应用到数据流
        PatternStream<TransactionBean> patternStream = CEP.pattern(
                // 按用户ID分组（相同用户的事件才会被关联）
                TransactionBeanStream.keyBy(tx -> tx.getUserId()),
                fraudPattern
        );

        // 6. 处理匹配到的欺诈模式
        DataStream<String> fraudAlerts = patternStream.process(
                new PatternProcessFunction<TransactionBean, String>() {
                    @Override
                    public void processMatch(
                            Map<String, List<TransactionBean>> match, // 匹配到的事件集合
                            Context ctx,
                            Collector<String> out) {

                        // 提取匹配的小额交易和大额交易
                        TransactionBean smallTx = match.get("smallTx").get(0);
                        TransactionBean largeTx = match.get("largeTx").get(0);

                        // 计算两次交易的时间差（秒）
                        long timeDiff = (largeTx.getTimestamp() - smallTx.getTimestamp()) / 1000;

                        // 生成欺诈警报信息
                        String alertMsg = String.format(
                                "[欺诈警报] 用户 %s 在 %d 秒内执行可疑操作："
                                        + "\n  小额测试: TX%s (%.2f元)"
                                        + "\n  大额转账: TX%s (%.2f元)",
                                smallTx.getUserId(), timeDiff,
                                smallTx.txId, smallTx.amount,
                                largeTx.txId, largeTx.amount
                        );
                        out.collect(alertMsg);
                    }
                });

        // 7. 输出检测结果（生产环境中可替换为Kafka/邮件/日志等）
        fraudAlerts.print().setParallelism(1); // 设置单并行度保证输出顺序

        // 8. 启动作业
        env.execute("Real-time Fraud Detection");
    }
}