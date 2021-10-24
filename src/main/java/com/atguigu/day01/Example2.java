package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// word count from socket
public class Example2 {
    // 不要忘记抛出异常！
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行任务的数量
        env.setParallelism(1);

        // 构建一个计算的dag：有向无环图
        DataStreamSource<String> stream = env.readTextFile("/home/zuoyuan/flink0519tutorial/src/main/resources/word.txt");

        // map
        // flatMap
        SingleOutputStreamOperator<WordWithCount> mappedStream = stream
                // 传递一个匿名类，第一个泛型是输入的泛型，第二个泛型是输出的泛型
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String in, Collector<WordWithCount> out) throws Exception {
                        String[] arr = in.split(" ");
                        for (String word : arr) {
                            // 使用collect方法向下游发送数据
                            out.collect(new WordWithCount(word, 1));
                        }
                    }
                });

        // shuffle
        // 将相同word的数据发送到同一个逻辑分区
        KeyedStream<WordWithCount, String> keyedStream = mappedStream
                // 匿名类
                // 第一个参数：输入的泛型
                // 第二个参数：key的泛型
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount in) throws Exception {
                        return in.word;
                    }
                });

        // reduce
        SingleOutputStreamOperator<WordWithCount> result = keyedStream
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });

        // 输出
        result.print();

        // 提交任务
        env.execute();
    }

    // POJO class定义
    // 1. 必须是public类
    // 2. 所有字段必须是public
    // 3. 必须有空构造器
    public static class WordWithCount {
        public String word;
        public Integer count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
