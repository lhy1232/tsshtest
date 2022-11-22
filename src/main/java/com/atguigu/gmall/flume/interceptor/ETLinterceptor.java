package com.atguigu.gmall.flume.interceptor;

import com.atguigu.gmall.utils.JSONUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @Author:goat
 * @Date:2022/10/15 15:42
 * @Descrption:
 */
public class ETLinterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    //校验json格式是否完整
    @Override
    public Event intercept(Event event) {
        //获取body，查看json格式
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        //校验json格式是否完整
        if (JSONUtils.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        //hasnext方法判断迭代器当前索引处是否有元素
        while(iterator.hasNext()){
            //迭代器取出当前位置元素进行处理，然后指针后移
            Event next = iterator.next();
            //调用上面的方法进行判断，
            if(intercept(next) == null){
                iterator.remove();
            }
        }
        return list;

    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLinterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
