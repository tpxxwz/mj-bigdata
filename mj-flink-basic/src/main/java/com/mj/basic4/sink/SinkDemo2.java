package com.mj.basic4.sink;

import com.mj.bean.MjOrderInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SinkDemo2 {
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
        totalAmount.addSink(new MysqlRichSink());
        env.execute("订单数据分析");
    }
    static class MysqlRichSink extends RichSinkFunction<MjOrderInfo> {
        private String url = "jdbc:mysql://mj_mysql:13306/test?serverTimezone=Asia/Shanghai&useSSL=false";
        private String username="root";
        private String password="mj20240313_";
        private  String sql="INSERT INTO `orders` (`user_id`, `amount`, `order_id`) \n" +
                "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE amount = VALUES(amount)";
        private transient Connection connection;
        private transient PreparedStatement preparedStatement;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 初始化数据库连接（每个并行实例独立连接）
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false); // 关闭自动提交
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(MjOrderInfo order, Context context) throws Exception {
            try {
                // 设置参数
                preparedStatement.setString(1, order.getUserId());
                preparedStatement.setDouble(2, order.getAmount());
                preparedStatement.setString(3, order.getOrderId());
                preparedStatement.execute();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback(); // 异常回滚
                throw new RuntimeException("数据库写入失败", e);
            }
        }

        @Override
        public void close() throws Exception {
            // 最终提交剩余批次
            if (preparedStatement != null) {
                preparedStatement.executeBatch();
                connection.commit();
            }
            // 关闭资源
            if (preparedStatement != null) preparedStatement.close();
            if (connection != null) connection.close();
            super.close();
        }
    }
}


