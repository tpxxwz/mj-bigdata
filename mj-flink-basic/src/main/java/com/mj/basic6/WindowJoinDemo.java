package com.mj.basic6;

import com.mj.bean.LogisticsEvent;
import com.mj.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义订单事件的水印策略
        WatermarkStrategy<OrderEvent> orderStrategy =
                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 从订单事件中提取时间戳（需要转换为毫秒时间戳）
                        .withTimestampAssigner((event, ts) -> event.orderTime);

        // 定义物流事件的水印策略
        WatermarkStrategy<LogisticsEvent> logisticsStrategy =
                WatermarkStrategy.<LogisticsEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 从物流事件中提取时间戳（需要转换为毫秒时间戳）
                        .withTimestampAssigner((event, ts) -> event.shippingTime);

        // 2. 定义测试数据（直接使用 fromElements 生成）
        // 订单事件流（订单ID, 用户ID, 下单时间戳）
        DataStream<OrderEvent> orderStream = env.fromElements(
                new OrderEvent("order1", "userA", "2025-05-20 17:00:00"),
                new OrderEvent("order2", "userB", "2025-05-20 17:00:00")
        );
        // 为订单流分配水印和事件时间
        orderStream = orderStream.assignTimestampsAndWatermarks(orderStrategy);

        // 物流事件流（订单ID, 发货时间戳, 状态）
        DataStream<LogisticsEvent> logisticsStream = env.fromElements(
                new LogisticsEvent("order1", "2025-05-20 17:59:00", "shipped"),  // 属于[18:00-19:00)窗口
                new LogisticsEvent("order2", "2025-05-20 17:24:00", "shipped")   // 属于[17:00-18:00)窗口
        );
        // 为物流流分配水印和事件时间
        logisticsStream = logisticsStream.assignTimestampsAndWatermarks(logisticsStrategy);

        // 3. 双流Join操作
        DataStream<String> join = orderStream.join(logisticsStream)
                // 指定第一个流（订单流）的key：按orderId分组
                .where(r1 -> r1.orderId)
                // 指定第二个流（物流流）的key：按orderId分组
                .equalTo(r2 -> r2.orderId)
                // 使用事件时间滚动窗口（1小时大小）
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                // 应用Join函数处理匹配结果
                .apply(new JoinFunction<OrderEvent, LogisticsEvent, String>() {
                    @Override
                    public String join(OrderEvent orderEvent, LogisticsEvent logisticsEvent) throws Exception {
                        // 返回匹配的订单和物流信息
                        return logisticsEvent + "<----->" + orderEvent;
                    }
                });

        // 4. 输出结果
        join.print();

        // 5. 执行任务
        env.execute();
    }
}
