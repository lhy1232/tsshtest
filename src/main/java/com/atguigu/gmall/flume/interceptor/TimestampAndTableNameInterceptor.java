package com.atguigu.gmall.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * @Author:goat
 * @Date:2022/10/19 0:38
 * @Descrption:
 */
public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取body里面的时间戳
        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        String s = new String(body, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(s);
        String ts = jsonObject.getString("ts");
        String table = jsonObject.getString("table");
        headers.put("timestamp",ts+"000");
        headers.put("tableName", table);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
