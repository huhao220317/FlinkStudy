package com.atguigu.day06;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// SELECT * FROM a INNER JOIN b WHERE a.id = b.id;
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("b", 4)
                );

        DataStreamSource<Tuple2<String, Integer>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 5),
                        Tuple2.of("a", 6),
                        Tuple2.of("b", 7),
                        Tuple2.of("b", 8)
                );

        stream1.keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                // 进入到process方法的数据，key是相同的
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    // list1用来保存来自第一条流的数据
                    private ListState<Tuple2<String, Integer>> list1;
                    // list2用来保存来自第二条流的数据
                    private ListState<Tuple2<String, Integer>> list2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        list1 = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple2<String, Integer>>("list1", Types.TUPLE(Types.STRING, Types.INT))
                        );
                        list2 = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple2<String, Integer>>("list2", Types.TUPLE(Types.STRING, Types.INT))
                        );
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> left, Context ctx, Collector<String> out) throws Exception {
                        list1.add(left);
                        // 每来一条来自第一条流的数据，就将数据和第二条流已经保存下来的数据进行join
                        for (Tuple2<String, Integer> right : list2.get()) {
                            out.collect(left + " => " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> right, Context ctx, Collector<String> out) throws Exception {
                        list2.add(right);
                        // 每来一条来自第二条流的数据，就将数据和第一条流已经保存下来的数据进行join
                        for (Tuple2<String, Integer> left : list1.get()) {
                            out.collect(left + " => " + right);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
