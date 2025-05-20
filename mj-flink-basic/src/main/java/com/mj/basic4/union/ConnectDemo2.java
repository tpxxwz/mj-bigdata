package com.mj.basic4.union;

import com.mj.basic4.union.data.MjOrderEvent;
import com.mj.basic4.union.data.MjStockUpdateEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class ConnectDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟订单流（实际从 Kafka 读取）
        DataStream<MjOrderEvent> orderStream = env.fromElements(
                new MjOrderEvent("order1", "productA", 2, 1625000000),
                new MjOrderEvent("order2", "productB", 5, 1625000001)
        );

        // 模拟库存更新流（实际从 Kafka 读取）
        DataStream<MjStockUpdateEvent> stockUpdateStream = env.fromElements(
                new MjStockUpdateEvent("productA", 10, 1625000000),
                new MjStockUpdateEvent("productB", 3, 1625000001)
        );

        // 连接两个流（KeyedStream 确保按商品分组）
        ConnectedStreams<MjOrderEvent, MjStockUpdateEvent> connectedStreams = orderStream
                .keyBy(MjOrderEvent::getProductId)
                .connect(stockUpdateStream.keyBy(MjStockUpdateEvent::getProductId));

        // 处理连接流
        DataStream<String> resultStream = connectedStreams
                .process(new InventoryCheckProcessFunction());

        resultStream.print();
        env.execute("Real-time Inventory Management");
    }
    public static class InventoryCheckProcessFunction
            extends CoProcessFunction<MjOrderEvent, MjStockUpdateEvent, String> {

        // 定义状态描述符（存储当前库存）
        private ValueStateDescriptor<Integer> stockDescriptor =
                new ValueStateDescriptor<>("productStock", Integer.class);
        private ValueState<Integer> stockState;

        @Override
        public void open(OpenContext openContext) {
            // 初始化状态
            stockState = getRuntimeContext().getState(stockDescriptor);
        }

        // 处理订单流（Element1）
        @Override
        public void processElement1(
                MjOrderEvent order,
                CoProcessFunction<MjOrderEvent, MjStockUpdateEvent, String>.Context ctx,
                Collector<String> out) throws Exception {

            Integer currentStock = stockState.value();
            if (currentStock == null) {
                // 库存未初始化（可能尚未收到库存更新事件）
                out.collect("[警告] 商品 " + order.getProductId() + " 库存数据未就绪，订单 " + order.getOrderId() + " 暂无法处理");
                return;
            }

            if (currentStock >= order.getQuantity()) {
                // 库存足够：扣减库存，生成确认消息
                stockState.update(currentStock - order.getQuantity());
                out.collect(String.format(
                        "[订单确认] 订单 %s 成功，商品 %s 剩余库存 %d",
                        order.getOrderId(), order.getProductId(), currentStock - order.getQuantity()
                ));
            } else {
                // 库存不足：触发告警
                out.collect(String.format(
                        "[库存不足] 订单 %s 失败，商品 %s 需求 %d，当前库存 %d",
                        order.getOrderId(), order.getProductId(), order.getQuantity(), currentStock
                ));
            }
        }

        // 处理库存更新流（Element2）
        @Override
        public void processElement2(
                MjStockUpdateEvent stockUpdate,
                CoProcessFunction<MjOrderEvent, MjStockUpdateEvent, String>.Context ctx,
                Collector<String> out) throws Exception {

            // 更新库存状态
            stockState.update(stockUpdate.getStock());
            out.collect(String.format(
                    "[库存更新] 商品 %s 最新库存 %d (更新时间 %d)",
                    stockUpdate.getProductId(), stockUpdate.getStock(), stockUpdate.getUpdateTime()
            ));
        }
    }
}
