package com.hp.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        System.out.println("执行LogETLInterceptor");
        // etl
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        System.out.println("获取log");
        System.out.println(log);
        // 校验
        if (log.contains("start")){
            if (LogUtils.validateStart(log)){
                System.out.println("validateStart");
                return event;
            }
        }else {
            if (LogUtils.validateEvent(log)){
                System.out.println("validateEvent");
                return event;
            }
        }
        System.out.println("返回null");
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            if (intercept1 != null){
                interceptors.add(event);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
