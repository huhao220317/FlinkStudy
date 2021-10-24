package com.atguigu.day04;

import com.atguigu.day02.Example2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

// 实时热门的url
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .keyBy(r -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Example2.Event, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Example2.Event> elements, Collector<String> out) throws Exception {
                        // url => pv-count
                        HashMap<String, Long> hashMap = new HashMap<>();
                        for (Example2.Event e : elements) {
                            if (!hashMap.containsKey(e.url)) {
                                hashMap.put(e.url, 1L);
                            } else {
                                hashMap.put(e.url, hashMap.get(e.url) + 1L);
                            }
                        }

                        // 排序
                        // 将hashMap的键值对放入列表中
                        ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<>();
                        for (String key : hashMap.keySet()) {
                            arrayList.add(Tuple2.of(key, hashMap.get(key)));
                        }

                        // 降序排列
                        arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
                                return t2.f1.intValue() - t1.f1.intValue();
                            }
                        });

                        Tuple2<String, Long> topUrl = arrayList.get(0);
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();
                        out.collect("在窗口" +
                                "" + new Timestamp(windowStart) + "~" +
                                "" + new Timestamp(windowEnd) + "访问次数最多的url是：" +
                                "" + topUrl.f0 + "，访问次数是：" + topUrl.f1);
                    }
                })
                .print();

        env.execute();
    }
}
