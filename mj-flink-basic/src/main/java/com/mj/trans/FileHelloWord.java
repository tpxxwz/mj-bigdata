package com.mj.trans;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * @desc: ApiHelloWord
 */
public class FileHelloWord {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        /*FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat()
                , new Path("D:/tar_source/hello.txt")).build();*/
        DataStream<String> socketStream = env.socketTextStream("192.168.110.21", 9000);
        SingleOutputStreamOperator<String> mapStream = socketStream.map(String :: toUpperCase).setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String,Integer>> flatMapStream = mapStream.flatMap(new Split()).setParallelism(2);
        KeyedStream<Tuple2<String,Integer>,String> keyStream = flatMapStream.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String,Integer>> sumStream = keyStream.sum(1);
        sumStream.print().setParallelism(1);
        env.execute("WordCount");
    }
    public static class Split implements FlatMapFunction<String, Tuple2<String,Integer>> {
        /**
         * 按照空格切分数据
         * @param element
         * @param collector
         * @throws Exception
         */
        @Override
        public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String [] eles = element.split(" ");
            for(String chr : eles){
                collector.collect(new Tuple2<>(chr,1));
            }
        }
    }
}