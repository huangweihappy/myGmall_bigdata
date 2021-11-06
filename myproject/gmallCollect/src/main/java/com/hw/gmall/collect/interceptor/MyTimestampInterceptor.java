package com.hw.gmall.collect.interceptor;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class MyTimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        JSONObject jsonObject = JSON.parseObject(body);
        String  ts = jsonObject.getString("ts");
        event.getHeaders().put("timestamp",ts);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            intercept(iterator.next());
        }
        return list;
    }

    public static class myBuilder implements Builder{


        @Override
        public Interceptor build() {
            return new MyTimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
    @Override
    public void close() {

    }
}
