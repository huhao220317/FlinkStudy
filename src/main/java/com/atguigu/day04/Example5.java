package com.atguigu.day04;

import com.atguigu.day02.Example2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .keyBy(r -> r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        // 当机器时间到达窗口结束时间的时候调用process方法
        // 迭代器中只有一个元素，就是getResult的结果
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            // 将迭代器中唯一的元素取出，就是聚合结果
            long count = elements.iterator().next();
            out.collect(
                    "url: " + key + "在窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                            "的pv次数是：" + count
            );
        }
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
