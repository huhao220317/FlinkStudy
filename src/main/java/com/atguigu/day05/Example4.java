package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.*;
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
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                // a 1
                // a 2
                // a 10
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    private long bound = 5000L; // 最大延迟时间是5秒钟
                                    private long maxTs = -Long.MAX_VALUE + bound + 1L; // 用来保存观察到的最大事件时间
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        // 每来一条数据调用一次
                                        maxTs = Math.max(maxTs, event.f1);
//                                        if (event.f0.equals("a")) {
//                                            output.emitWatermark(new Watermark(event.f1 - 1L));
//                                        }
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 周期性调用，默认200毫秒调用一次
                                        output.emitWatermark(new Watermark(maxTs - bound - 1L));
                                    }
                                };
                            }

                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                };
                            }
                        }
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
