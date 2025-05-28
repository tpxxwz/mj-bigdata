package com.mj.basic6;

import com.mj.bean.LogisticsEvent;
import com.mj.bean.OrderEvent;
import com.mj.utils.TimeConverter;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 *
 * 本类演示了如何使用Flink的Interval Join功能计算订单发货延迟
 * 通过连接订单事件和物流事件，计算从下单到发货的时间差
 */
public class ProcessJoinFunctionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义水印策略和时间戳分配器

        // 订单事件的水印策略（允许2秒乱序）
        WatermarkStrategy<OrderEvent> orderStrategy =
                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 从订单事件中提取事件时间戳（毫秒）
                        .withTimestampAssigner((event, ts) -> event.orderTime);

        // 物流事件的水印策略（允许2秒乱序）
        WatermarkStrategy<LogisticsEvent> logisticsStrategy =
                WatermarkStrategy.<LogisticsEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 从物流事件中提取事件时间戳（毫秒）
                        .withTimestampAssigner((event, ts) -> event.shippingTime);

        // 3. 创建测试数据流

        // 订单事件流（订单ID, 用户ID, 下单时间戳）
        DataStream<OrderEvent> orderStream = env.fromElements(
                new OrderEvent("order1", "userA", "2025-05-20 17:00:00"),
                new OrderEvent("order2", "userB", "2025-05-20 17:00:00")
        ).assignTimestampsAndWatermarks(orderStrategy); // 分配水印和时间戳

        // 物流事件流（订单ID, 发货时间戳, 状态）
        DataStream<LogisticsEvent> logisticsStream = env.fromElements(
                new LogisticsEvent("order1", "2025-05-20 22:00:00", "shipped"),  // 属于[18:00-19:00)窗口
                new LogisticsEvent("order2", "2025-05-20 17:24:00", "shipped")   // 属于[17:00-18:00)窗口
        ).assignTimestampsAndWatermarks(logisticsStrategy); // 分配水印和时间戳


        // 4. 按键分组（为Interval Join做准备）

        // 按订单ID对订单流分组
        KeyedStream<OrderEvent, String> keyedOrders = orderStream.keyBy(OrderEvent::getOrderId);

        // 按订单ID对物流流分组
        KeyedStream<LogisticsEvent, String> keyedLogistics = logisticsStream.keyBy(LogisticsEvent::getOrderId);

        // 5. 执行Interval Join操作

        /*
         * Interval Join核心逻辑：
         * - 连接条件：相同订单ID（key匹配）
         * - 时间范围：以订单事件时间为基准，前后2小时窗口
         * - 处理方式：使用ProcessJoinFunction处理匹配事件
         */
        DataStream<String> result = keyedOrders.intervalJoin(keyedLogistics)
                // 定义时间窗口范围：订单时间前2小时到后2小时（共4小时窗口）
                .between(Duration.ofHours(-2), Duration.ofHours(10))
                // 应用自定义处理函数计算发货延迟
                .process(new OrderShippingDelayCalculator());

        // 6. 打印计算结果
        result.print();

        // 7. 执行Flink任务
        env.execute("订单发货延迟计算");
    }

    /**
     * 自定义ProcessJoinFunction计算订单发货延迟
     *
     * 功能：
     * 1. 接收匹配的订单事件和物流事件
     * 2. 计算发货延迟（物流时间 - 订单时间）
     * 3. 格式化为可读字符串输出
     */
    public static class OrderShippingDelayCalculator
            extends ProcessJoinFunction<OrderEvent, LogisticsEvent, String> {

        @Override
        public void processElement(
                OrderEvent order,          // 订单事件
                LogisticsEvent logistics,  // 物流事件
                Context ctx,               // 上下文（可访问时间戳等信息）
                Collector<String> out      // 输出收集器
        ) {
            // 计算发货延迟（分钟）
            long delayMinutes = (logistics.shippingTime - order.orderTime) / (1000 * 60);

            // 格式化输出结果
            String output = String.format("订单 %s | 用户 %s | 发货延迟 %d 分钟",
                    order.orderId, order.userId, delayMinutes);

            // 发送计算结果
            out.collect(output);
        }
    }
}