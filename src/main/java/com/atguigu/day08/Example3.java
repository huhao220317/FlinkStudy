package com.atguigu.day08;

import com.atguigu.day05.Example7;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

// uv，unique visitor，独立访客
// pv 按照userId去重，得到的就是独立访客数量
// 如果窗口中的独立访客数量是1亿，滑动窗口长度是1小时，滑动距离是5秒钟，这个问题如何解决？
// 100000000 * 100byte = 10G
// uv 的统计无需完全精确，亿级别的uv，只需要精确到百万就可以了
// uv 的统计是允许误判的，所以才可以使用布隆过滤器
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flink0519tutorial/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, Example7.UserBehavior>() {
                    @Override
                    public Example7.UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        // etl to POJO class
                        return new Example7.UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        // 设置最大延迟时间
                        // 指定数据源中的时间戳是哪一个字段
                        WatermarkStrategy.<Example7.UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Example7.UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(Example7.UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            long count = elements.iterator().next();
            out.collect("窗口" + windowStart + "~" + windowEnd + "的uv数据是：" + count);
        }
    }

    public static class CountAgg implements AggregateFunction<Example7.UserBehavior, HashSet<String>, Long> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Example7.UserBehavior value, HashSet<String> accumulator) {
            accumulator.add(value.userId);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }
}
