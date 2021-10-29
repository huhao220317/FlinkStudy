package com.atguigu.day09;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

// 查询每个用户的pv数据
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        DataStreamSource<Tuple2<String, String>> stream = env
                .fromElements(
                        Tuple2.of("Mary", "./home"),
                        Tuple2.of("Bob", "./cart"),
                        Tuple2.of("Mary", "./prod?id=1"),
                        Tuple2.of("Liz", "./home")
                );

        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("f0").as("user"),
                        $("f1").as("url")
                );

        // 为了执行sql需要将动态表注册为临时视图
        streamTableEnvironment
                .createTemporaryView("clicks", table);

        // 查询结果也是一个动态表
        // sql对应的底层api实现应该怎么做？
        // .map(r -> Tuple2.of(r.user, 1L))
        // .keyBy(r -> r.user)
        // .sum(1)
        Table result = streamTableEnvironment
                .sqlQuery(
                        "SELECT user, COUNT(url) as cnt FROM " +
                                "clicks GROUP BY user"
                );

        // 将结果动态表转换成数据流
        DataStream<Row> rowDataStream = streamTableEnvironment
                .toChangelogStream(result);

        rowDataStream.print();

        env.execute();
    }
}
