package com.atguigu.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello world", "hello world").setParallelism(1)
                .flatMap((String in, Collector<Tuple2<String, Integer>> out) -> {
                    String[] arr = in.split(" ");
                    for (String word : arr) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                // keyby不能设置并行度
                .keyBy(r -> r.f0)
                .reduce((Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1))
                .setParallelism(4)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                // print继承了全局并行度
                .print();

        env.execute();
    }
}
