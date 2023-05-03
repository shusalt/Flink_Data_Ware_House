package com.liweidao.apps.functions;

import com.alibaba.fastjson.JSONObject;
import com.liweidao.utils.DimUtil;
import com.liweidao.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

//维度异步查询的函数类
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    //声明线程池
    private static ThreadPoolExecutor threadPoolExecutor;
    //声明属性
    private String tableName;

    //构造器
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }
    //初始化线程池

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //获取查询条件
                String key = getKey(t);
                //查询维度数据
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
                //关联到事实数据上
                if (dimInfo != null || dimInfo.size() > 0) {
                    try {
                        join(t, dimInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //继续向下游传输
                resultFuture.complete(Collections.singletonList(t));
            }
        });
    }
}
