package com.atguigu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

// flink cep 实现 连续三次登录失败
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user-1", "fail", 1000L),
                        new LoginEvent("user-2", "success", 2000L),
                        new LoginEvent("user-1", "fail", 3000L),
                        new LoginEvent("user-1", "fail", 4000L),
                        new LoginEvent("user-1", "fail", 5000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        // 定义模板
        // flink cep 的底层将模板翻译成有限状态机
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("login-fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.type.equals("fail");
                    }
                })
                .times(3) // 要求出现三次
                .consecutive(); // 要求连续

        // 在流上匹配模板
        // patternStream是匹配到的数据构成的流
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                stream.keyBy(r -> r.userId),
                pattern
        );

        // 将匹配到的数据从流上取出
        patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                        // {
                        //   "login-fail": [LoginEvent, LoginEvent, LoginEvent]
                        // }
                        LoginEvent first = pattern.get("login-fail").get(0);
                        LoginEvent second = pattern.get("login-fail").get(1);
                        LoginEvent third = pattern.get("login-fail").get(2);
                        String result = "用户：" + first.userId + "在时间：" +
                                "" + first.ts + ";" +
                                "" + second.ts + ";" +
                                "" + third.ts + ";" +
                                "连续三次登录失败";
                        return result;
                    }
                })
                .print();

        env.execute();
    }

    public static class LoginEvent {
        public String userId;
        public String type;
        public Long ts;

        public LoginEvent() {
        }

        public LoginEvent(String userId, String type, Long ts) {
            this.userId = userId;
            this.type = type;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "LoginEvent{" +
                    "userId='" + userId + '\'' +
                    ", type='" + type + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }
}
