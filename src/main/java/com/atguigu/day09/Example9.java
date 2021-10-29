package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// ddl
public class Example9 {
    public static void main(String[] args) throws Exception {
        // 以下三行，获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        // 定义输入表，WITH定义了连接器，连接到文件file.csv
        // 将文件流转化成动态表
        tableEnvironment
                .executeSql("CREATE TABLE clicks (`user` STRING, `url` STRING) " +
                        "WITH (" +
                        "'connector' = 'filesystem'," +
                        "'path' = '/home/zuoyuan/flink0519tutorial/src/main/resources/file.csv'," +
                        "'format' = 'csv')");

        // 定义输出表，连接到标准输出
        // 将结果表转换成数据流
        tableEnvironment
                .executeSql("CREATE TABLE ResultTable (`user` STRING, `cnt` BIGINT) " +
                        "WITH ('connector' = 'print')");

        // 在输入表上进行查询，查询结果写入输出表
        tableEnvironment
                .executeSql("INSERT INTO ResultTable SELECT user, COUNT(url) as cnt FROM clicks GROUP BY user");

        // 注意：没有env.execute()了
    }
}
