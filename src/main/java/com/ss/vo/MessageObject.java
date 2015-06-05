package com.ss.vo;

import com.ss.main.Constants;
import io.netty.handler.codec.http.HttpRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yousheng on 15/3/17.
 */
public class MessageObject implements Constants {

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


    public MessageObject method(String method) {
        attribute.put(METHOD, method);
        return this;
    }

    public MessageObject version(String version) {
        attribute.put(VERSION, version);
        return this;
    }

    public MessageObject remote(String remote) {
        attribute.put(REMOTE, remote);
        return this;
    }

    public MessageObject kv(String key, String value) {
        attribute.put(key, value);
        return this;
    }

}
