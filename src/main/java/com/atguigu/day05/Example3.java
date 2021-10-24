package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

// 水位线
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每隔1分钟插入一次水位线
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                // 水位线 = 观察到事件包含的最大时间戳 - 最大延迟时间 - 1毫秒
                // 1 2 5 3 4 6
                // 机器默认每隔200毫秒在map出来的数据流中插入一次水位线
                .assignTimestampsAndWatermarks(
                        // `Duration.ofSeconds(5)`设置了最大延迟时间为5秒钟
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        // 指定数据的哪一个字段是时间戳
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        String windowStart = new Timestamp(context.window().getStart()).toString();
                        String windowEnd = new Timestamp(context.window().getEnd()).toString();
                        long count = 0L;
                        for (Tuple2<String, Long> e : elements) count += 1L;
                        out.collect("key: " + key + " 窗口：" + windowStart + "~" + windowEnd + " 个数是：" + count);
                    }
                })
                .print();

        env.execute();
    }
}
