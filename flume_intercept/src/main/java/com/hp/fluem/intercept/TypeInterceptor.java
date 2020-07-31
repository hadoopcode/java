package com.hp.fluem.intercept;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {
    private ArrayList<Event> arrayList = new ArrayList<Event>();;

    public void initialize() {
    }

    public Event intercept(Event event) {
        String body = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();
        if (body.contains("hello")) {
            headers.put("topic", "first");
        } else {
            headers.put("topic", "second");
        }
        event.setHeaders(headers);
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        arrayList.clear();
        for (Event event : list) {
            Event intercept = this.intercept(event);
            arrayList.add(intercept);
        }
        return arrayList;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }

}
