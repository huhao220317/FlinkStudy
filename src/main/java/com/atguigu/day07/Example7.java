package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

// read from kafka
// 先启动消费者
// 再启动生产者
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        env
                .addSource(
                        new FlinkKafkaConsumer<String>(
                                "user-behavior-test",
                                new SimpleStringSchema(),
                                properties
                        )
                )
                .map(r -> new com.atguigu.day05.Example7.UserBehavior(
                        r.split(",")[0], r.split(",")[1], r.split(",")[2], r.split(",")[3],
                        Long.parseLong(r.split(",")[4]) * 1000L
                ))
                // 当返回值类型是POJO或者TUPLE这样的类型时，需要returns方法
                .returns(Types.POJO(com.atguigu.day05.Example7.UserBehavior.class))
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        // 设置最大延迟时间为0,forMonotonousTimestamps()等价于：forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        WatermarkStrategy.<com.atguigu.day05.Example7.UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<com.atguigu.day05.Example7.UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(com.atguigu.day05.Example7.UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // ProcessWindowFunction不能注册定时器
                .aggregate(
                        new AggregateFunction<com.atguigu.day05.Example7.UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(com.atguigu.day05.Example7.UserBehavior value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, com.atguigu.day05.Example7.ItemViewCountPerWindow, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<com.atguigu.day05.Example7.ItemViewCountPerWindow> out) throws Exception {
                                out.collect(
                                        new com.atguigu.day05.Example7.ItemViewCountPerWindow(
                                                s, elements.iterator().next(),
                                                context.window().getStart(),
                                                context.window().getEnd()
                                        )
                                );
                            }
                        }
                )
                .keyBy(r -> r.windowEnd)
                .process(new KeyedProcessFunction<Long, com.atguigu.day05.Example7.ItemViewCountPerWindow, String>() {
                    private ListState<com.atguigu.day05.Example7.ItemViewCountPerWindow> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<com.atguigu.day05.Example7.ItemViewCountPerWindow>(
                                        "list-state",
                                        Types.POJO(com.atguigu.day05.Example7.ItemViewCountPerWindow.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(com.atguigu.day05.Example7.ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        ArrayList<com.atguigu.day05.Example7.ItemViewCountPerWindow> arrayList = new ArrayList<>();
                        // listState.get()方法返回的是包含所有元素的迭代器
                        for (com.atguigu.day05.Example7.ItemViewCountPerWindow e : listState.get()) {
                            arrayList.add(e);
                        }
                        listState.clear();

                        arrayList.sort(
                                new Comparator<com.atguigu.day05.Example7.ItemViewCountPerWindow>() {
                                    @Override
                                    public int compare(com.atguigu.day05.Example7.ItemViewCountPerWindow t2, com.atguigu.day05.Example7.ItemViewCountPerWindow t1) {
                                        return t1.count.intValue() - t2.count.intValue();
                                    }
                                }
                        );

                        StringBuilder result = new StringBuilder();
                        result.append("=====================================================\n");
                        result.append("窗口结束时间是：" + new Timestamp(timestamp - 1L) + "\n");
                        for (int i = 0; i < 3; i++) {
                            com.atguigu.day05.Example7.ItemViewCountPerWindow currItem = arrayList.get(i);
                            result.append("第" + (i+1) + "名的itemId是：" + currItem.itemId + "；浏览次数是：" + currItem.count);
                            result.append("\n");
                        }
                        result.append("=====================================================\n");
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}
