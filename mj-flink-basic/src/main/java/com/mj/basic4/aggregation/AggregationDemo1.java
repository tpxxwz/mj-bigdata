package com.mj.basic4.aggregation;

import com.mj.dto.MjOrderInfo;
import com.mj.dto.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class AggregationDemo1 {
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

        // 2. 计算每个用户的总金额（累加操作）
        DataStream<MjOrderInfo> totalAmount = keyedOrders.sum("amount");

        // 3. 计算每个用户的最高单笔订单（完整记录）
        DataStream<MjOrderInfo> maxByOrder = keyedOrders.maxBy("amount");

        // 4. 计算每个用户的最低单笔订单（完整记录）
        DataStream<MjOrderInfo> minByOrder = keyedOrders.minBy("amount");

        // 5. 计算每个用户的最高单笔订单（仅目标字段）
        DataStream<MjOrderInfo> maxOrder = keyedOrders.max("amount");

        // 4. 计算每个用户的最低单笔订单（仅目标字段）
        DataStream<MjOrderInfo> minOrder = keyedOrders.min("amount");

        // 输出结果到控制台（带描述前缀）
        //totalAmount.print("总金额统计");
        //maxOrder.print("最高单笔订单");
        //minOrder.print("最低单笔订单");
        //maxByOrder.print("最高单笔订单");
        minByOrder.print("最低单笔订单");

        env.execute("订单数据分析");
    }
}
