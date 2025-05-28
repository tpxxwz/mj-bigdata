package com.mj.basic7;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟用户点击数据流（用户ID, 商品ID）
        DataStream<Tuple2<String, String>> clicks = env.fromElements(
                Tuple2.of("user1", "iPhone"),
                Tuple2.of("user1", "MacBook"),
                Tuple2.of("user2", "iPad"),
                Tuple2.of("user1", "iPhone"),
                Tuple2.of("user1", "iPhone"),
                Tuple2.of("user2", "iPad")
        );

        // 按用户ID分组处理
        clicks.keyBy(value -> value.f0)
                .process(new TrackWithMapState())
                .print();

        env.execute("Product Click Analysis");
    }

    // 自定义处理函数
    public static class TrackWithMapState extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        // 1️ 声明Map状态（Key:商品ID，Value:点击次数）
        private MapState<String, Integer> productClickMap;

        @Override
        public void open(OpenContext openContext) {
            // 2️ 初始化Map状态（为每个用户创建积分卡）
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("productClicks", String.class, Integer.class);
            productClickMap = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> click, Context ctx, Collector<String> out)
                throws Exception {
            String productId = click.f1;

            // 3️ 获取当前商品的点击次数（查积分卡）
            Integer count = productClickMap.get(productId) == null ? 0 : productClickMap.get(productId);

            // 4️ 更新状态（更新积分）
            count += 1;
            productClickMap.put(productId, count);

            // 当某商品点击达3次时触发提示
            if (count >= 3) {
                out.collect("用户 " + click.f0 + " 对商品【" + productId + "】点击已达" + count + "次！");
                productClickMap.remove(productId); // 可选：重置该商品计数
            }
        }
    }
}