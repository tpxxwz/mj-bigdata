package com.mj.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Split implements FlatMapFunction<String, Tuple2<String,Integer>> {
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
