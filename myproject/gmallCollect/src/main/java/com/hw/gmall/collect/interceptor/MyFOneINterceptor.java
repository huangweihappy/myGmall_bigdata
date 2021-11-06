package com.hw.gmall.collect.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 判断日志数据是否合法的拦截器
 */
public class MyFOneINterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        //判断数据是不是合法的json串

        try {
            JSON.parseObject(body);
        }catch (JSONException e){
            return null;
        }


        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        /*
        迭代判断list中的event是否合法，如果不合法就把它remove 使用迭代器，不能使用增强for
         */
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            Event event = iterator.next();
            Event result = intercept(event);
            if (result == null){
                iterator.remove();
            }


        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class myBuilder implements Builder{


        @Override
        public Interceptor build() {
            return new MyFOneINterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
