package com.atguigu.day02.ready;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class MySource {

    public static class ClickEvent implements SourceFunction<JSONObject> {
        private boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz", "John"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<JSONObject> ctx) throws Exception {
            JSONObject jsonobj = new JSONObject();
            int useri = 0;
            int urli = 0;
            String source = "";
            while (running) {
                useri = random.nextInt(5);
                urli = random.nextInt(5);
                jsonobj.put("user", userArr[useri]);
                jsonobj.put("url", userArr[urli]);
                jsonobj.put("ts", Calendar.getInstance().getTimeInMillis());
                ctx.collect(jsonobj);
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
