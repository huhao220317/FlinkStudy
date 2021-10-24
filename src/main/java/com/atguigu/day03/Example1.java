package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// broadcast
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .broadcast()
                .print("广播: ").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .shuffle()
                .print("shuffle: ").setParallelism(2);

        // round-robin
        env
                .fromElements(1,2,3,4).setParallelism(1)
                .rebalance()
                .print("rebalance: ").setParallelism(2);

        env.execute();
    }
}
