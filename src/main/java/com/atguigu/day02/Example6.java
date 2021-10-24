package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "black", "gray")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if (value.equals("white")) out.collect(value);
                        else if (value.equals("black")) {
                            out.collect(value);
                            out.collect(value);
                        } else {

                        }
                    }
                })
                .print("stream1: ");

        env
                .fromElements("white", "black", "gray")
                .flatMap((String value, Collector<String> out) -> {
                    if (value.equals("white")) out.collect(value);
                    else if (value.equals("black")) {
                        out.collect(value);
                        out.collect(value);
                    } else {
                    }
                })
                .returns(Types.STRING)
                .print("stream2: ");

        env.execute();
    }
}
