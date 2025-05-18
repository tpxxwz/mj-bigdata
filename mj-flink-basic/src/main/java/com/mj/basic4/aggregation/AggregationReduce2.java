package com.mj.basic4.aggregation;

import com.mj.dto.Order;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class AggregationReduce2 {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 模拟输入数据流（实际场景可能来自Kafka/Socket等）
        DataStream<Order> orders = env.fromElements(
                new Order("user1", 199.0, "O1001"),
                new Order("user2", 299.0, "O1002"),
                new Order("user1", 599.0, "O1003"),
                new Order("user2", 99.0,  "O1004"),
                new Order("user1", 899.0, "O1005")
        );

        // 1. 按用户分组（根据userId进行KeyBy）
        KeyedStream<Order, String> keyedOrders = orders.keyBy(order -> order.userId);

        // 使用Reduce获取最大订单
        DataStream<Order> maxOrder = keyedOrders.reduce(new ReduceFunction<Order>() {
            @Override
            public Order reduce(Order currentMax, Order newOrder) {
                Order result = new Order();
                result.setUserId(currentMax.userId);
                result.setAmount(currentMax.getAmount() + newOrder.getAmount());
                result.setOrderId(currentMax.orderId);
                return result;
            }
        });
        // 输出结果
        maxOrder.print("根据user汇总订单金额");
        env.execute("订单数据分析");
    }
}
