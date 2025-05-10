package com.mj.function;

import com.mj.dto.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlRichSink extends RichSinkFunction<Order> {
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
    public void invoke(Order order, Context context) throws Exception {
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
