package com.atguigu.day02.ready;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<JSONObject> json = env.addSource(new MySource.ClickEvent());
        //实时获取访问人员名字
        /*json.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.getString("user");
            }
        }).print("0");*/

        //过滤Mary
        json.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getString("user").equals("Mary");
            }
        }).print("1");
        json.filter(new MyFilter()).print("2");
        json.flatMap(new FlatMapFunction<JSONObject, String>() {
            @Override
            public void flatMap(JSONObject value, Collector<String> out) throws Exception {
                if (value.getString("user").equals("Mary"))out.collect(value.getString("user"));
            }
        }).print("3");
        env.execute();
    }
    public static class MyFilter implements FilterFunction<JSONObject>{

        @Override
        public boolean filter(JSONObject value) throws Exception {
            return value.getString("user").equals("Mary");
        }
    }
}
