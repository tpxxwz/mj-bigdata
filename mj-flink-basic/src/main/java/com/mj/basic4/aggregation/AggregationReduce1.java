package com.mj.basic4.aggregation;

import com.mj.bean.MjOrderInfo;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class AggregationReduce1 {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 模拟输入数据流（实际场景可能来自Kafka/Socket等）
        DataStream<MjOrderInfo> orders = env.fromElements(
                new MjOrderInfo("order1","user1", 199, System.currentTimeMillis()),
                new MjOrderInfo("order2","user2", 299, System.currentTimeMillis()),
                new MjOrderInfo("order11","user1", 599, System.currentTimeMillis()),
                new MjOrderInfo("order22","user2", 99, System.currentTimeMillis()),
                new MjOrderInfo("order111","user2", 899, System.currentTimeMillis())
        );

        // 1. 按用户分组（根据userId进行KeyBy）
        KeyedStream<MjOrderInfo, String> keyedOrders = orders.keyBy(order -> order.getUserId());
        // 使用Reduce获取最大订单
        DataStream<MjOrderInfo> maxOrder = keyedOrders.reduce(new ReduceFunction<MjOrderInfo>() {
            @Override
            public MjOrderInfo reduce(MjOrderInfo currentMax, MjOrderInfo newOrder) {
                // 比较并保留金额更大的订单
                return (newOrder.getAmount() > currentMax.getAmount()) ?
                        new MjOrderInfo(newOrder.getOrderId(),newOrder.getUserId(), newOrder.getAmount(), newOrder.getTs()) :
                        new MjOrderInfo(currentMax.getOrderId(),currentMax.getUserId(), currentMax.getAmount(), System.currentTimeMillis());
            }
        });

        // 输出结果
        maxOrder.print("最大订单记录");

        env.execute("订单数据分析");
    }
}
