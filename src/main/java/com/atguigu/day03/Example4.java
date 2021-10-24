package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        ctx.collect(1);
                        ctx.collect(2);
                        ctx.collect(3);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print();

        env.execute();
    }
}
