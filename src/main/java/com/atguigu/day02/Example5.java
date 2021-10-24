package com.atguigu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .filter(r -> r.user.equals("Mary"))
                .print();

        env
                .addSource(new Example2.ClickSource())
                .filter(new FilterFunction<Example2.Event>() {
                    @Override
                    public boolean filter(Example2.Event value) throws Exception {
                        return value.user.equals("Mary");
                    }
                })
                .print();

        env
                .addSource(new Example2.ClickSource())
                .filter(new MyFilter())
                .print();

        env
                .addSource(new Example2.ClickSource())
                .flatMap(new FlatMapFunction<Example2.Event, Example2.Event>() {
                    @Override
                    public void flatMap(Example2.Event value, Collector<Example2.Event> out) throws Exception {
                        if (value.user.equals("Mary")) out.collect(value);
                    }
                });

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Example2.Event> {
        @Override
        public boolean filter(Example2.Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
