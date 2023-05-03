package com.liweidao.apps.functions;

import com.alibaba.fastjson.JSONObject;

//自定义维度数据查询接口
public interface DimJoinFunction<T> {
    //获取数据中的所要关联维度的主键
    String getKey(T input);

    //关联事实数据与维度数据
    void join(T input, JSONObject diminfo) throws Exception;
}
