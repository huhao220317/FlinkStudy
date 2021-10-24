package com.atguigu.day04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        ctx.collect(1);
                        Thread.sleep(1000L);
                        ctx.collect(1);
                        Thread.sleep(1000L);
                        ctx.collect(1);
                        Thread.sleep(10000L);
                        ctx.collect(1);
                        Thread.sleep(10000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();
                        long count = 0;
                        for (Integer e : elements) count += 1L;
                        out.collect(
                                "在窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                                        "的一共有：" + count
                        );
                    }
                })
                .print();

        env.execute();
    }
}
