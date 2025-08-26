package com.mj.sql.hello;

import com.mj.bean.Emp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;


public class Hello03APIDataTypes {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // Tuple类型
        DataStreamSource<String> tupleSource = environment.fromData("aa,11", "bb,22", "cc,33");
        SingleOutputStreamOperator<Tuple2<String, String>> stream = tupleSource.map(word -> {
            String[] split = word.split(",");
            return Tuple2.of(split[0], split[1]);
        }, Types.TUPLE(Types.STRING, Types.STRING));
        Table tupleTable = tableEnvironment.fromDataStream(stream);
        System.out.println("=============== TUPLE TYPE ===============");
        tupleTable.execute().print();

        // Pojo类型
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("mj-flink-basic/tmp/emp.txt")).build();
        DataStreamSource<String> empSource = environment.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        SingleOutputStreamOperator<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table empTable = tableEnvironment.fromDataStream(empStream);
        System.out.println("=============== POJO TYPE ===============");
        empTable.execute().print();

        // Row类型
        DataStreamSource<Row> dataStream = environment.fromData(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
        Table rowTable = tableEnvironment.fromChangelogStream(dataStream);
        System.out.println("=============== ROW TYPE ===============");
        rowTable.execute().print();

    }

}
