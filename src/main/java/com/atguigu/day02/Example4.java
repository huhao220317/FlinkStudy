package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// MAP
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .map(new MapFunction<Example2.Event, String>() {
                    @Override
                    public String map(Example2.Event value) throws Exception {
                        return value.user;
                    }
                }).setParallelism(2)
                .print();

        env
                .addSource(new Example2.ClickSource())
                .map(new MyMap()).setParallelism(2)
                .print();

        env
                .addSource(new Example2.ClickSource())
                .map(r -> r.user).setParallelism(2)
                .print();

        env
                .addSource(new Example2.ClickSource())
                .flatMap(new FlatMapFunction<Example2.Event, String>() {
                    @Override
                    public void flatMap(Example2.Event value, Collector<String> out) throws Exception {
                        out.collect(value.user);
                    }
                }).setParallelism(2)
                .print();


        env.execute();
    }

    public static class MyMap implements MapFunction<Example2.Event, String> {
        @Override
        public String map(Example2.Event value) throws Exception {
            return value.user;
        }
    }
}
