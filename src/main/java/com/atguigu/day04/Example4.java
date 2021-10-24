package com.atguigu.day04;

import com.atguigu.day02.Example2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .keyBy(r -> r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Example2.Event, Long, Long> {
        // 当创建窗口时，创建一个累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 累加规则
        @Override
        public Long add(Example2.Event value, Long accumulator) {
            return accumulator + 1L;
        }

        // 当窗口闭合的时候，输出结果
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
