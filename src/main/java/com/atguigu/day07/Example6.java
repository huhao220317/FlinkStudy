package com.atguigu.day07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

// sink to kafka
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");

        env
                .readTextFile("D:\\Atguigu\\flink0519\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "user-behavior-test",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
