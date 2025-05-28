package com.mj.basic6;

import com.mj.bean.MjOrderInfo;
import com.mj.bean.User;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class BroadcastProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 订单测试数据（orderId,userId,amount,ts）
        DataStream<MjOrderInfo> orderDs = env.fromElements(
                "o1,u1,100,1625000000000",
                "o2,u2,200,1625000001000",
                "o3,u1,150,1625000002000"
        ).map(o -> {
            String[] lines = o.split(",");
            return new MjOrderInfo(lines[0], lines[1], Integer.valueOf(lines[2]), Long.valueOf(lines[3]));
        });

        // 用户测试数据（userId,name,balance,age,email）
        DataStream<User> userDs = env.fromElements(
                "u1,Alice,500.0,30,alice@example.com",
                "u2,Bob,300.0,25,bob@example.com"
        ).map(o -> {
            String[] lines = o.split(",");
            return new User(lines[0], lines[1], Double.valueOf(lines[2]), Integer.valueOf(lines[3]), lines[4]);
        }).setParallelism(1);
        // 将user流（维表）定义为广播流
        final MapStateDescriptor<String, User> broadcastDesc = new MapStateDescriptor("Alan_RulesBroadcastState",
                Integer.class,
                User.class);

        BroadcastStream<User> broadcastStream = userDs.broadcast(broadcastDesc);

        // 需要由非广播流来进行调用
        DataStream result = orderDs.connect(broadcastStream)
                .process(new JoinBroadcastProcessFunctionImpl(broadcastDesc));

        result.print();

        env.execute();

    }
    // final BroadcastProcessFunction<IN1, IN2, OUT> function)
    public static class JoinBroadcastProcessFunctionImpl extends BroadcastProcessFunction<MjOrderInfo, User, Tuple2<MjOrderInfo, String>> {
        // 用于存储规则名称与规则本身的 map 存储结构
        MapStateDescriptor<String, User> broadcastDesc;

        JoinBroadcastProcessFunctionImpl(MapStateDescriptor<String, User> broadcastDesc) {
            this.broadcastDesc = broadcastDesc;
        }

        // 负责处理广播流的元素
        @Override
        public void processBroadcastElement(User value,
                                            BroadcastProcessFunction<MjOrderInfo, User, Tuple2<MjOrderInfo, String>>.Context ctx,
                                            Collector<Tuple2<MjOrderInfo, String>> out) throws Exception {
            System.out.println("收到广播数据：" + value);
            // 得到广播流的存储状态
            ctx.getBroadcastState(broadcastDesc).put(value.getUserId(), value);
        }

        // 处理非广播流，关联维度
        @Override
        public void processElement(MjOrderInfo value,
                                   BroadcastProcessFunction<MjOrderInfo, User, Tuple2<MjOrderInfo, String>>.ReadOnlyContext ctx,
                                   Collector<Tuple2<MjOrderInfo, String>> out) throws Exception {
            // 得到广播流的存储状态
            ReadOnlyBroadcastState<String, User> state = ctx.getBroadcastState(broadcastDesc);

            out.collect(new Tuple2<>(value, state.get(value.getUserId()).getName()));
        }
    }
}





