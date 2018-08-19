package com.gridu.flume;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestJsonFilterInterceptor {
    private JsonFilterInterceptor jsonFilterInterceptor;

    private String input;
    private String intercepted;

    public TestJsonFilterInterceptor(String input, String intercepted) {
        this.input = input;
        this.intercepted = intercepted;
    }

    @Before
    public void setUp() {
        this.jsonFilterInterceptor = new JsonFilterInterceptor();
    }

    @Parameters
    public static Collection<Object[]> data() {
        String jsonObject = "{\"key\": \"value\"}";

        return Arrays.asList(new Object[][]{
                {"[" + jsonObject, jsonObject},
                {"[" + jsonObject + ",", jsonObject},
                {jsonObject + ",", jsonObject},
                {jsonObject + "]", jsonObject},
                {jsonObject, jsonObject}
        });
    }

    @Test
    public void testIntercept() {
        Charset charset = Charset.forName("UTF-8");

        Event event = new JSONEvent();
        event.setBody(input.getBytes(charset));
        Event interceptedEvent = jsonFilterInterceptor.intercept(event);

        String interceptedEventBody = new String(interceptedEvent.getBody(), charset);
        Assert.assertEquals(this.intercepted, interceptedEventBody);
    }
}
