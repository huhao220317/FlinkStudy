package com.atguigu.day01.ready;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 9999);
        SingleOutputStreamOperator<String> process = ds1.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {
                    private ValueState<Tuple2<String, Integer>> wcStat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        wcStat = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, Integer>>(
                                "stat-wc",
                                Types.TUPLE(Types.STRING,Types.INT)

                        ));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<String> collector) throws Exception {
                        //Tuple2<String, Integer> res = wcStat.value();
                        if (wcStat.value() == null){
                            wcStat.update(stringIntegerTuple2);
                        }else {

                            wcStat.update(Tuple2.of(stringIntegerTuple2.f0,wcStat.value().f1+stringIntegerTuple2.f1));
                        }
                        //s输出时使用状态获取值，如果使用res，会出现NullPointException 可能原因是状态更新后res指向并未发生变化
                        collector.collect(wcStat.value().f0 + " " +  + wcStat.value().f1);
                    }
                });
        process.print();
        env.execute();
    }
}
