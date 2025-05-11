package com.mj.basic4.union;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.ClickEvent;
import com.mj.dto.PaymentEvent;
import com.mj.dto.PurchaseEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KafkaIntervalJoinDemo {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义 Kafka 配置
        String clickTopic = "user_clicks";
        String purchaseTopic = "user_purchases";
        String brokers = "mj01:9092";
        String groupId = "interval-join-group";

        // 3. 创建 Kafka Source（点击事件）
        KafkaSource<String> clickSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(clickTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 4. 创建 Kafka Source（购买事件）
        KafkaSource<String> purchaseSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(purchaseTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStream<ClickEvent> clicks = env.fromSource(
                clickSource,
                WatermarkStrategy.noWatermarks(),  // 后续单独设置 Watermark
                "Click Source"
        ).process(new ProcessFunction<String, ClickEvent>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ClickEvent> out) {
                try {
                    ClickEvent event = JSON.parseObject(value, ClickEvent.class);
                    out.collect(event);
                } catch (Exception e) {
                    System.err.println("解析 ClickEvent 失败: " + value);
                }
            }
        });

        DataStream<PurchaseEvent> purchases = env.fromSource(
                purchaseSource,
                WatermarkStrategy.noWatermarks(),  // 后续单独设置 Watermark
                "Purchase Source"
        ).process(new ProcessFunction<String, PurchaseEvent>() {
            @Override
            public void processElement(String value, Context ctx, Collector<PurchaseEvent> out) {
                try {
                    PurchaseEvent event = JSON.parseObject(value, PurchaseEvent.class);
                    out.collect(event);
                } catch (Exception e) {
                    System.err.println("解析 PurchaseEvent 失败: " + value);
                }
            }
        });

        // 6. 设置事件时间和 Watermark
        WatermarkStrategy<ClickEvent> clickWatermarkStrategy =
                WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.clickTime);

        WatermarkStrategy<PurchaseEvent> purchaseWatermarkStrategy =
                WatermarkStrategy.<PurchaseEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.purchaseTime);
        clicks = clicks.assignTimestampsAndWatermarks(clickWatermarkStrategy);
        purchases = purchases.assignTimestampsAndWatermarks(purchaseWatermarkStrategy);
        // 7. 执行 Interval Join
        DataStream<String> result = clicks
                .keyBy(click -> click.userId)
                .intervalJoin(purchases.keyBy(purchase -> purchase.userId))
                .between(Duration.ofHours(-1), Duration.ofMinutes(0))  // 购买时间在点击后1小时内
                .process(new ProcessJoinFunction<ClickEvent, PurchaseEvent, String>() {
                    @Override
                    public void processElement(
                            ClickEvent click,
                            PurchaseEvent purchase,
                            Context ctx,
                            Collector<String> out) {
                        String msg = String.format(
                                "用户 %s 点击广告 %s 后购买，金额：%.2f (点击时间: %tF %tT, 购买时间: %tF %tT)",
                                click.userId, click.adId, purchase.amount,
                                click.clickTime, click.clickTime,
                                purchase.purchaseTime, purchase.purchaseTime
                        );
                        out.collect(msg);
                    }
                });

        // 8. 输出结果
        result.print();
        // 9. 执行作业
        env.execute("Kafka Interval Join Example");
    }

}
