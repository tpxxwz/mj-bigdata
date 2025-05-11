package com.mj.basic4.union;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.OrderInfo;
import com.mj.dto.PaymentEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class KafkaOrderPaymentCheck {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义 Kafka 配置
        String orderTopic = "orders";
        String paymentTopic = "payments";
        String bootstrapServers = "mj01:6667";
        String groupId = "order-payment-check";
        // 2. 创建Kafka数据源
        KafkaSource<String> orderSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics(orderTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 2. 创建Kafka数据源
        KafkaSource<String> paymentSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics(paymentTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 3. 从Kafka源创建数据流
        DataStreamSource<String> orderStream = env.fromSource(
                orderSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );
        // 3. 从Kafka源创建数据流
        DataStreamSource<String> paymentStream = env.fromSource(
                paymentSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        // 4. 解析JSON数据
        DataStream<OrderInfo> orderInfoStream = orderStream.map(
                value -> JSON.parseObject(value, OrderInfo.class)
        );
        // 4. 解析JSON数据
        DataStream<PaymentEvent> paymentEventStream = paymentStream.map(
                value -> JSON.parseObject(value, PaymentEvent.class)
        );
        // 5. KeyBy 订单ID
        KeyedStream<OrderInfo, String> orderKeyedStream = orderInfoStream.keyBy(OrderInfo::getOrderId);
        KeyedStream<PaymentEvent, String> paymentKeyedStream = paymentEventStream.keyBy(PaymentEvent::getOrderId);
        ConnectedStreams<OrderInfo,PaymentEvent> connectedStreams = orderKeyedStream.connect(paymentKeyedStream);
        // 5. 双流关联处理

        connectedStreams.process(new OrderPaymentMatchFunction())
                .print("告警信息"); // 输出到控制台

        env.execute("Kafka Order-Payment Check");
    }
    // 6. 实现 CoProcessFunction
    public static class OrderPaymentMatchFunction
            extends CoProcessFunction<OrderInfo, PaymentEvent, String> {

        private ValueState<OrderInfo> orderState;
        private ValueState<Long> timerState;

        @Override
        public void open(OpenContext openContext) {
            orderState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("orderState", OrderInfo.class));
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timerState", Long.class));
        }

        @Override
        public void processElement1(OrderInfo order, Context ctx, Collector<String> out) {
            try {
                // 保存订单并注册5分钟定时器
                orderState.update(order);
                //long timer = order.eventTime + 300_000; // 5分钟（毫秒）
                long timer = order.eventTime + 5000; // 5秒（毫秒）
                ctx.timerService().registerEventTimeTimer(timer);
                timerState.update(timer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void processElement2(PaymentEvent payment, Context ctx, Collector<String> out) {
            try {
                OrderInfo order = orderState.value();
                if (order != null && order.orderId.equals(payment.getOrderId())) {
                    out.collect("支付成功: " + payment.getOrderId());
                    ctx.timerService().deleteEventTimeTimer(timerState.value());
                    orderState.clear();
                    timerState.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            try {
                OrderInfo order = orderState.value();
                if (order != null) {
                    out.collect("[告警] 订单超时未支付: " + order.orderId);
                    orderState.clear();
                    timerState.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
