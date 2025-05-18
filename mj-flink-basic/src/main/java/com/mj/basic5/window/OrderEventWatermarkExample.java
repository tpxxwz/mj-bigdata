package com.mj.basic5.window;

import com.mj.dto.OrderEvent;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class OrderEventWatermarkExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟订单事件流（包含乱序数据）
        DataStreamSource<OrderEvent> orderStream = env.fromElements(
                // 正常数据（用户u1在1小时窗口内）
                new OrderEvent("o1", "u1", 100, 1672502400000L),  // 2023-01-01 00:00:00
                new OrderEvent("o2", "u1", 200, 1672506000000L),  // 2023-01-01 01:00:00（属于下一个窗口）

                // 乱序数据（比水位线延迟到达的数据）
                new OrderEvent("o3", "u2", 150, 1672502400000L),  // 正常时间
                new OrderEvent("o4", "u2", 300, 1672502400500L),  // 延迟500ms
                new OrderEvent("o5", "u2", 250, 1672502401900L)   // 延迟1.9s（在允许的2秒延迟范围内）
        );

        // 定义水印策略（允许2秒乱序）
        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, ts) -> event.getTs());

        SingleOutputStreamOperator<OrderEvent> watermarkedStream = orderStream
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 按用户分组，计算每小时订单总金额
        watermarkedStream
                .keyBy(order -> order.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1))) // 1小时滚动窗口
                .apply(new WindowFunction<OrderEvent, String, String, TimeWindow>() {
                    @Override
                    public void apply(String userId,
                                      TimeWindow window,
                                      Iterable<OrderEvent> orders,
                                      Collector<String> out) {
                        // 获取窗口时间范围
                        String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                        String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());

                        long total = 0L;
                        List<String> orderIds = new ArrayList<>();

                        for (OrderEvent order : orders) {
                            total += order.amount;
                            orderIds.add(order.orderId);
                        }

                        String result = String.format(
                                "用户 %s 在窗口 %s 至 %s 的订单总金额：%d，订单ID：%s",
                                userId,
                                new Date(window.getStart()),
                                new Date(window.getEnd()),
                                total,
                                orderIds
                        );
                        out.collect(result);
                    }
                })
                .print();

        env.execute("Order Amount Hourly Statistics");
    }
}