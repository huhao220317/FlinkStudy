package com.atguigu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

// 将数据流转换成动态表
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据源，且设置水位线
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream = env
                .fromElements(
                        Tuple3.of("Mary", "./home", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Bob", "./cart", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=1", 12 * 60 * 60 * 1000L + 5 * 1000L),
                        Tuple3.of("Liz", "./home", 12 * 60 * 60 * 1000L + 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=3", 12 * 60 * 60 * 1000L + 90 * 1000L),
                        Tuple3.of("Mary", "./prod?id=7", 12 * 60 * 60 * 1000L + 105 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        // 获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        // 将数据流转换成动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        // as 是起别名
                        $("f0").as("user"),
                        $("f1").as("url"),
                        $("f2").as("cTime")
                );

        // 动态表的默认字段名是f0, f1, f2
//        Table table = streamTableEnvironment.fromDataStream(stream);

        // 将动态表再转换成数据流
        // 每一行的泛型是通用泛型Row
        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(table);

        rowDataStream.print();

        env.execute();
    }
}
