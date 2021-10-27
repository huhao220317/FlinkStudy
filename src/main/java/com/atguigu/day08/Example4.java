package com.atguigu.day08;

import com.atguigu.day05.Example7;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
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
public class Example4 {
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

    // 累加器的第一个元素是布隆过滤器，第二个元素是可能来过的userId的数量
    public static class CountAgg implements AggregateFunction<Example7.UserBehavior, Tuple2<BloomFilter<Long>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            // create三个参数，过滤的数据类型，预期数据量，误判率
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(), 100000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Example7.UserBehavior value, Tuple2<BloomFilter<Long>, Long> accumulator) {
            // 如果用户userId一定没来过
            if (!accumulator.f0.mightContain(Long.parseLong(value.userId))) {
                // 根据hash函数计算下标，将下标对应的bit位置为1
                accumulator.f0.put(Long.parseLong(value.userId));
                // 用户一定没来过，所以uv值加一
                accumulator.f1 += 1L;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> a, Tuple2<BloomFilter<Long>, Long> b) {
            return null;
        }
    }
}
