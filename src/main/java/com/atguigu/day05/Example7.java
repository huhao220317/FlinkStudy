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
// 滑动窗口，长度1小时，滑动距离5分钟
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
                        // etl to POJO class
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        // 设置最大延迟时间
                        // 指定数据源中的时间戳是哪一个字段
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                // 使用商品id进行逻辑分区
                .keyBy(r -> r.itemId)
                // 在每一条支流上，使用窗口进一步进行逻辑分区
                // 先分流再开窗
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 第一步要计算的结果是：每个商品id在每个窗口中的浏览次数
                // 增量聚合函数和全窗口聚合函数结合使用的方式来聚合
                // 增量聚合函数负责聚合
                // 全窗口聚合函数负责包裹一层窗口信息
                .aggregate(new CountAgg(), new WindowResult())
                // 聚合结果：ItemViewCountPerWindow, ItemViewCountPerWindow, ...
                // 要按照窗口再一次分组
                // 每一条支流的元素都是同一个窗口的不同商品id的ItemViewCountPerWindow
                .keyBy(r -> r.windowEnd)
                // 按照浏览量进行降序排列
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

        // 声明一个列表状态变量，用来保存到达的ItemViewCountPerWindow数据
        private ListState<ItemViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化状态变量
            listState = getRuntimeContext().getListState(
                    // `Types.POJO(ItemViewCountPerWindow.class)`指明了列表状态变量中的元素类型是什么
                    new ListStateDescriptor<ItemViewCountPerWindow>("list-state", Types.POJO(ItemViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，添加到列表状态变量
            // ListState的使用方式和ArrayList相同
            // ListState的可见范围是当前key，还是一个单例
            listState.add(value);
            // 注册排序定时器，只有第一条数据到达时，会注册定时器，因为数据的windowEnd都是一样的
            // 不会发生在ItemViewCountPerWindow都到达之前，就触发排序定时事件的情况
            // 因为高于windowEnd + 100毫秒的水位线总是跟在所有数据的后面到达，不会提前到达
            // 只要记住一个原则：分流水位线广播进行传播，合流水位线选择最小的向下传播
            // 水位线不会绕过数据弯道超车
            // 为了使的所有的ItemViewCountPerWindow数据都进入到ListState，所以要加100毫秒
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        // 定时器的时间戳`timestamp`是windowEnd + 100L
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 将列表状态变量中的元素都取出然后放入arrayList中
            // 因为ListState是无法排序的
            ArrayList<ItemViewCountPerWindow> arrayList = new ArrayList<>();
            for (ItemViewCountPerWindow e : listState.get()) {
                arrayList.add(e);
            }
            // 当数据都保存在ArrayList中以后，就可以清空列表状态变量了，这样可以减少存储压力
            listState.clear();

            // 按照浏览量降序排列
            arrayList.sort(new Comparator<ItemViewCountPerWindow>() {
                @Override
                public int compare(ItemViewCountPerWindow t1, ItemViewCountPerWindow t2) {
                    // 指定降序排列
                    return t2.count.intValue() - t1.count.intValue();
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
        // 接收来自getResult的结果，然后包裹窗口信息
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow> out) throws Exception {
            // 发送给下游的.keyBy方法
            out.collect(new ItemViewCountPerWindow(
                    s, elements.iterator().next(), context.window().getStart(), context.window().getEnd()
            ));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        // 创建窗口中的累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 定义聚合逻辑，每来一条数据就加一
        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        // 窗口闭合的时候，将结果发送出去
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        // 在使用事件时间会话窗口的时候，需要实现merge
        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 某个商品在某个1小时窗口中被浏览的次数
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
