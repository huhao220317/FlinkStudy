package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

// 每5分钟统计过去一个小时的浏览量最高的3个商品
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flink0519tutorial/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCountPerWindow, String> {
        private int n;

        // 取出前 n 名的商品浏览统计数据
        public TopN(int n) {
            this.n = n;
        }

        private ListState<ItemViewCountPerWindow> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCountPerWindow>("list-state", Types.POJO(ItemViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，添加到列表状态变量
            listState.add(value);
            // 注册排序定时器，只有第一条数据到达时，会注册定时器，因为数据的windowEnd都是一样的
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 将列表状态变量中的元素都取出然后放入arrayList中
            ArrayList<ItemViewCountPerWindow> arrayList = new ArrayList<>();
            for (ItemViewCountPerWindow e : listState.get()) {
                arrayList.add(e);
            }
            listState.clear();

            // 按照浏览量降序排列
            arrayList.sort(new Comparator<ItemViewCountPerWindow>() {
                @Override
                public int compare(ItemViewCountPerWindow t2, ItemViewCountPerWindow t1) {
                    return t1.count.intValue() - t2.count.intValue();
                }
            });

            // 输出统计信息
            StringBuilder result = new StringBuilder();
            result.append("=====================================================\n");
            result.append("窗口结束时间是：" + new Timestamp(timestamp - 100L) + "\n");
            for (int i = 0; i < n; i++) {
                ItemViewCountPerWindow currItem = arrayList.get(i);
                result.append("第" + (i+1) + "名的itemId是：" + currItem.itemId + "；浏览次数是：" + currItem.count);
                result.append("\n");
            }
            result.append("=====================================================\n");
            out.collect(result.toString());
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow> out) throws Exception {
            out.collect(new ItemViewCountPerWindow(
                    s, elements.iterator().next(), context.window().getStart(), context.window().getEnd()
            ));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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
    }

    public static class ItemViewCountPerWindow {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCountPerWindow() {
        }

        public ItemViewCountPerWindow(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCountPerWindow{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }

    public static class UserBehavior {
        public String userId;
        public String itemId;
        public String categoryId;
        public String type;
        public Long ts;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String type, Long ts) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.type = type;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", type='" + type + '\'' +
                    ", ts=" + new Timestamp(ts) +
                    '}';
        }
    }
}
