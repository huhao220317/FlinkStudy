package com.atguigu.day04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// 计算每个url在每个窗口中被访问的次数（pv）
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new com.atguigu.day02.Example2.ClickSource())
                .keyBy(r -> r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<com.atguigu.day02.Example2.Event, String, String, TimeWindow> {
        // 当机器时间到达窗口结束时间的时候调用process方法
        // 迭代器中包含了窗口中的所有元素
        @Override
        public void process(String key, Context context, Iterable<com.atguigu.day02.Example2.Event> elements, Collector<String> out) throws Exception {
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            long count = 0;
            for (com.atguigu.day02.Example2.Event e : elements) count += 1L;
            out.collect(
                    "url: " + key + "在窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                            "的pv次数是：" + count
            );
        }
    }
}
