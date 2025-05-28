package com.mj.basic7;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟用户行为数据流（用户ID, 页面名称）
        DataStream<Tuple2<String, String>> behaviors = env.fromElements(
                Tuple2.of("user1", "首页"),
                Tuple2.of("user1", "商品页"),
                Tuple2.of("user2", "首页"),
                Tuple2.of("user1", "购物车"),
                Tuple2.of("user2", "订单页"),
                Tuple2.of("user2", "支付页")
        );

        // 按用户ID分组处理
        behaviors.keyBy(value -> value.f0)
                .process(new TrackWithListState())
                .print();

        env.execute("User Behavior Tracking");
    }

    // 自定义处理函数
    public static class TrackWithListState extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        // 1️ 声明列表状态
        private ListState<String> pageListState;

        @Override
        public void open(OpenContext openContext) {
            // 2️ 初始化列表状态（为每个用户创建专属清单）
            ListStateDescriptor<String> descriptor =
                    new ListStateDescriptor<>("pageTrack", String.class);
            pageListState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> behavior, Context ctx, Collector<String> out) throws Exception {
            // 3️ 将新页面添加到列表状态（往购物清单添加商品）
            pageListState.add(behavior.f1);

            // 4️读取当前列表内容
            Iterable<String> pages = pageListState.get();
            List<String> currentList = new ArrayList<>();
            pages.forEach(currentList::add);

            // 当访问3个不同页面时触发提示
            if (currentList.size() >= 3) {
                out.collect("用户 " + behavior.f0 + " 完成路径: " + currentList);
                pageListState.clear(); // 清空状态（可选）
            }
        }
    }
}
