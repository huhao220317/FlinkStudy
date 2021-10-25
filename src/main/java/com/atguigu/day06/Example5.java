package com.atguigu.day06;

import com.atguigu.day02.Example2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

// 查询流
// 查询结果是某个用户的pv数据流
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 我们在端口输入要查询的用户名：`Mary`
        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999);

        // 点击流
        DataStreamSource<Example2.Event> clickStream = env
                .addSource(new Example2.ClickSource());

        // 点击流使用url进行分流
        clickStream.keyBy(r -> r.url)
                // 查询流将并行度设置为1，然后广播到所有任务槽
                // 为什么要将并行度设置为1？因为需要将数据按顺序广播出去
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new CoFlatMapFunction<Example2.Event, String, Example2.Event>() {
                    private String queryString = "";
                    @Override
                    public void flatMap1(Example2.Event value, Collector<Example2.Event> out) throws Exception {
                        // 当第一条流的数据到达flatMap算子时，触发flatMap1的调用
                        if (value.user.equals(queryString)) out.collect(value);
                    }

                    @Override
                    public void flatMap2(String value, Collector<Example2.Event> out) throws Exception {
                        // 当第二条流的数据到达flatMap算子时，触发flatMap2的调用
                        queryString = value;
                    }
                })
                .print();

        env.execute();
    }
}
