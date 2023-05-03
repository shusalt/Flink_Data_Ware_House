package com.liweidao.apps.functions;

import com.alibaba.fastjson.JSONObject;
import com.liweidao.common.ConstantConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化phoenix连接
        Class.forName(ConstantConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(ConstantConfig.PHOENIX_SERVER);
    }

    //修改DimSink的invoke方法
    //如果维度数据发生了变化，同时失效该数据对应的Redis中的缓存
    @Override
    public void invoke(JSONObject value, Context context) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            //获取数据中的Key以及Value
            JSONObject data = value.getJSONObject("data");
            Set<String> keySets = data.keySet();
            Collection<Object> values = data.values();

            //获取表名
            String table = value.getString("sink_table");

            //创建插入数据的SQL
            //自定义一个upsertSQL函数用于创建插入数据SQL
            String upsertSQL = insertDataQL(table, keySets, values);
            System.out.println("插入数据SQL:" + upsertSQL);
            preparedStatement = connection.prepareStatement(upsertSQL);
            preparedStatement.execute();
            connection.commit();
            //判断如果为更新数据,则删除Redis中数据
//            if ("update".equals(value.getString("operation"))){
//                String source_table1 = value.getString("sink_table");
//                String redis_value = value.getJSONObject("data").getString("id");
//                String key=source_table1+":"+redis_value;
//                DimUtil.deleteCached(key);
//            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入Phoenix数据失败！");
        } finally {
            if (preparedStatement == null) {
                preparedStatement.close();
            }
        }
    }

    //创建插入数据的SQL
    private String insertDataQL(String table, Set<String> columns, Collection<Object> values) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("upsert into ").append(ConstantConfig.HBASE_SCHEMA).append(".").append(table);
        stringBuffer.append("(").append(StringUtils.join(columns, ",")).append(")");
        stringBuffer.append(" values").append("('").append(StringUtils.join(values, "','")).append("')");
        return stringBuffer.toString();
    }
}
