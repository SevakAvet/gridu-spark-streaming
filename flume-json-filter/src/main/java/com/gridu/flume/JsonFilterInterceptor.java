package com.gridu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.stream.Collectors;

public class JsonFilterInterceptor implements Interceptor {

    public Event intercept(Event event) {
        byte[] eventBody = event.getBody();

        String body = new String(eventBody);
        body = filter(body);

        event.setBody(body.getBytes());
        return event;
    }

    private String filter(String body) {
        if (body == null || body.isEmpty()) {
            return body;
        }

        int startIndex = 0;
        int endIndex = body.length();

        char firstChar = body.charAt(0);
        char lastChar = body.charAt(body.length() - 1);

        if (firstChar == '[') {
            startIndex++;
        }

        if (lastChar == ',' || lastChar == ']') {
            endIndex--;
        }

        if (startIndex == 0 && endIndex == body.length()) {
            return body;
        }

        return body.substring(startIndex, endIndex);
    }

    public void initialize() {

    }

    public void close() {

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return events.stream().map(this::intercept).collect(Collectors.toList());
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public void configure(Context context) {
            // TODO Auto-generated method stub
        }

        @Override
        public Interceptor build() {
            return new JsonFilterInterceptor();
        }
    }
}
