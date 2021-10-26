package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

// 自定义数据源
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .print();

        env.execute();
    }

    // 注意SourceFunction的泛型
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz", "John"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random();

        // 程序启动时，触发run的执行
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                // 使用了collect方法来向下游发送数据
                ctx.collect(Event.of(
                        userArr[random.nextInt(userArr.length)],
                        urlArr[random.nextInt(urlArr.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                Thread.sleep(100L);
            }
        }

        // 取消任务时，触发cancel的执行
        // ./flink cancel JobID
        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public String user;
        public String url;
        public Long ts;

        public Event() {
        }

        public Event(String user, String url, Long ts) {
            this.user = user;
            this.url = url;
            this.ts = ts;
        }

        public static Event of(String user, String url, Long ts) {
            return new Event(user, url, ts);
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", ts=" + new Timestamp(ts) +
                    '}';
        }
    }
}
