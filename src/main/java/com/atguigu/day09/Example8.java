package com.atguigu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

// flink sql 实现实时热门商品
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<com.atguigu.day05.Example7.UserBehavior> stream = env
                .readTextFile("/home/zuoyuan/flink0519tutorial/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, com.atguigu.day05.Example7.UserBehavior>() {
                    @Override
                    public com.atguigu.day05.Example7.UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        // etl to POJO class
                        return new com.atguigu.day05.Example7.UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        // 设置最大延迟时间
                        // 指定数据源中的时间戳是哪一个字段
                        WatermarkStrategy.<com.atguigu.day05.Example7.UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<com.atguigu.day05.Example7.UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(com.atguigu.day05.Example7.UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("userId"),
                        $("itemId"),
                        $("categoryId"),
                        $("type"),
                        // rowtime()方法告诉了flink sql，ts是事件时间
                        $("ts").rowtime()
                );

        streamTableEnvironment.createTemporaryView("userbehavior", table);

        // HOP_START 是滑动窗口开始时间
        // HOP_END 是滑动窗口结束时间
        // HOP 第一个参数是使用的时间字段，第二个参数是滑动距离，第三个参数是窗口长度
        // 使用sql的话，COUNT(itemId)这一句，翻译成底层api是全窗口聚合函数，所以内存压力很大
        // ItemViewCountPerWindow
        String innerSql = "SELECT itemId, COUNT(itemId) as itemCount, " +
                        "HOP_START(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS) as windowStart, " +
                        "HOP_END(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS) as windowEnd " +
                        "FROM userbehavior GROUP BY " +
                        "itemId, HOP(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS)";

        // 按照windowEnd进行分组
        // 使用partition by来进行分区，而不能使用group by
        String midSql = "SELECT *, ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY itemCount DESC) as row_num" +
                " FROM (" + innerSql + ")";

        // 取出前三名
        String outerSQL = "SELECT row_num, itemId, itemCount, windowEnd FROM (" + midSql + ") WHERE row_num <= 3";

        Table result = streamTableEnvironment
                .sqlQuery(outerSQL);

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
