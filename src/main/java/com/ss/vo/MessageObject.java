package com.ss.vo;

import io.netty.handler.codec.http.HttpRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yousheng on 15/3/17.
 */
public class MessageObject {

    private HttpRequest httpMessage;

    public Map<String, Object> attribute = new HashMap<>();


    public HttpRequest getHttpMessage() {
        return httpMessage;
    }

    public void setHttpMessage(HttpRequest httpMessage) {
        this.httpMessage = httpMessage;
    }

    public void add(String key, Object value) {
        if (key == null)
            return;

        attribute.put(key, value);
    }

    public Map<String, Object> getAttribute() {
        return attribute;
    }
}
