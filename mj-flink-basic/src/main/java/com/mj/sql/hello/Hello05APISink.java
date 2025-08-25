package com.mj.sql.hello;

import com.mj.bean.Emp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


public class Hello05APISink {

    public static void main(String[] args) throws Exception {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // Pojo类型
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("mj-flink-basic/tmp/emp.txt")).build();
        DataStreamSource<String> empSource = environment.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        SingleOutputStreamOperator<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table empTable = tableEnvironment.fromDataStream(empStream);

        // TableApi查询的结果为table
        Table table1 = empTable.select($("empno"), $("ename"));
        // SQL查询的结果为table
        Table table2 = tableEnvironment.sqlQuery("select empno,ename,job from " + empTable);
        // 方案1: 将表转成流
        tableEnvironment.toDataStream(table1).print();
        // 方案2: 连接器
        tableEnvironment.executeSql(
                """
                        CREATE TABLE print_table (
                        empno INT,
                        ename STRING,
                        job STRING
                        ) WITH (
                        'connector' = 'print'
                        )
                        """);
        table2.insertInto("print_table").execute();
        // 执行环境
        environment.execute();
    }

}
