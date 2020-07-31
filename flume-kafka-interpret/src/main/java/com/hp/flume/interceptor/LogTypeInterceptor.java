package com.hp.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        System.out.println("执行LogTypeInterceptor");
        // 将body里的数据根据类型，写到header

        // 1 获取body数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        System.out.println("获取log");
        System.out.println(log);
        // 2 获取header
        Map<String, String> headers = event.getHeaders();

        // 3 判断
        if (log.contains("start")){
            headers.put("topic","topic-start");
            System.out.println("header is topic-start");
        }else {
            headers.put("topic","topic-event");
            System.out.println("header is topic-event");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept = intercept(event);

            interceptors.add(intercept);
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class  Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
