package com.atguigu.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r -> r % 2)
                .sum(0)
                .print("sum累加奇偶数：");

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r -> 1)
                .sum(0)
                .print("sum累加所有：");

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r -> r % 2)
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                })
                .print("reduce累加奇偶数：");

        env
                .fromElements(1,2,3,4,5)
                .map(r -> Tuple2.of(r, r))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(
                                value1.f0 > value2.f0 ? value2.f0 : value1.f0,
                                value1.f1 > value2.f1 ? value1.f1 : value2.f1
                        );
                    }
                })
                .print("最大最小值：");

        env
                .fromElements(1,2,3,4,5)
                .map(r -> Tuple2.of(r, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1);
                    }
                })
                .map(r -> r.f0 / r.f1)
                .print("平均值：");

        env
                .addSource(new Example2.ClickSource())
                .map(r -> Tuple2.of(r.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print("pv统计：");

        env.execute();
    }
}
