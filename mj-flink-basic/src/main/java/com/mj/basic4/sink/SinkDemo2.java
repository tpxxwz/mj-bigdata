package com.mj.basic4.sink;

import com.mj.dto.Order;
import com.mj.function.MysqlRichSink;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
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
        DataStream<Order> orders = env.fromElements(
                new Order("user1", 199.0, "O1001"),
                new Order("user2", 299.0, "O1002"),
                new Order("user1", 599.0, "O1003"),
                new Order("user2", 99.0,  "O1004"),
                new Order("user1", 899.0, "O1005")
        );
        // 1. 按用户分组（根据userId进行KeyBy）
        KeyedStream<Order, String> keyedOrders = orders.keyBy(order -> order.userId);
        // 2. 计算每个用户的总金额（累加操作）
        DataStream<Order> totalAmount = keyedOrders.sum("amount");
        totalAmount.addSink(new MysqlRichSink());


        //将流中的数据写入到Mysql
        /*SinkFunction<Order> jdbcSink = JdbcSink.<Order>builder().buildExactlyOnce(
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        //按照使用的数据库来创建对应的XADataSource
                        MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
                        mysqlXADataSource.setUrl("jdbc:mysql://hadoop102:3306/test");
                        mysqlXADataSource.setUser("root");
                        mysqlXADataSource.setPassword("000000");
                        return mysqlXADataSource;
                    }
                }
        );*/



        env.execute("订单数据分析");
    }
}
