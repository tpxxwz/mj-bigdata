package com.mj.sql.hello;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Hello02APIDDL {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表：方案1 流表转换
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("mj-flink-basic/tmp/dept.txt")).build();
        DataStreamSource<String> deptSource = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        Table deptTable = tableEnvironment.fromDataStream(deptSource);
        deptTable.execute().print();

        // 创建表：方案2 TableAPI
        // tableEnvironment.createTable();
        // tableEnvironment.createTemporaryView();
        //已经过时了：推荐使用上面的方法
        // tableEnvironment.registerDataStream();

        // 创建表： 方案3 SQL
        // tableEnvironment.executeSql("create table ...");
    }

}
