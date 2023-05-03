package com.liweidao.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;


//DIM数据查询工具

/**
 * select * from t where id='19' and name='zhangsan';
 * <p>
 * Redis:
 * 1.存什么数据？         维度数据   JsonStr
 * 2.用什么类型？         String  Set  Hash
 * 3.RedisKey的设计？     String：tableName+id  Set:tableName  Hash:tableName
 * t:19:zhangsan
 * <p>
 * 集合方式排除,原因在于我们需要对每条独立的维度数据设置过期时间
 */
//redis key---->tableName+条件值
public class DimUtil {
    //旧方法
//    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {
//        if (columnValues.length <= 0) {
//            throw new RuntimeException("查询维度数据时,请至少设置一个查询条件！");
//        }
//        //创建phoenix where子句
//        StringBuffer whereSql = new StringBuffer(" where ");
//        //遍历查询条件并赋值whereSql
//        for (int i = 0; i < columnValues.length; i++) {
//            Tuple2<String, String> columnValue = columnValues[i];
//            String column = columnValue.f0;
//            String value = columnValue.f1;
//            whereSql.append(column).append("=").append("'").append(value).append("'");
//            //如果不是最好一个条件，则添加and
//            if (i<columnValues.length-1){
//                whereSql.append(" and ");
//            }
//        }
//        //拼接SQL
//        String querySQL="select * from "+tableName+whereSql.toString();
//        System.out.println(querySQL);
//        List<JSONObject> jsonObjects = PhoenixUtil.queryList(querySQL, JSONObject.class);
//        JSONObject jsonObject = jsonObjects.get(0);
//        return jsonObject;
//    }
    //重构方法
    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    //加入cache_aside_pattern
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnvalues) {
        if (columnvalues.length <= 0) {
            throw new RuntimeException("查询维度数据时,请至少设置一个查询条件！");
        }
        //创建phoenix where子句
        StringBuffer whereSQl = new StringBuffer(" where ");
        //创建Redis---Key
        StringBuffer redisKey = new StringBuffer(tableName).append(":");
        for (int i = 0; i < columnvalues.length; i++) {
            Tuple2<String, String> columnvalue = columnvalues[i];
            String column = columnvalue.f0;
            String value = columnvalue.f1;
            whereSQl.append(column).append("=").append("'").append(value).append("'");
            redisKey.append(value);
            //如果不是最后一个判断条件，则加上and
            if (i < columnvalues.length - 1) {
                whereSQl.append(" and ");
                redisKey.append(":");
            }
        }
        System.out.println("redisKye>>>>" + redisKey.toString());
        //获取redis连接
        Jedis jedis = RedisUtil.getJedis();
        String dimJsonStr = jedis.get(redisKey.toString());
        //判断是否从Redis中查询数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            jedis.close();
            return JSON.parseObject(dimJsonStr);
        }
        //否则
        //拼接querySQl
        String querySQL = "select * from " + tableName + whereSQl.toString();
        System.out.println("querySQL>>>>" + querySQL);
        //查询Phoenix中的维度数据
        List<JSONObject> query_list = PhoenixUtil.queryList(querySQL, JSONObject.class);
        JSONObject dim_data = query_list.get(0);
        //将数据写入redis
        jedis.set(redisKey.toString(), dim_data.toString());
        jedis.expire(redisKey.toString(), 24 * 60 * 60);//设置redis的key失效时间
        jedis.close();
        return dim_data;
    }

    //维度数据变化时要使缓存失效
    //根据key让Redis中的缓存失效
    public static void deleteCached(String key) {
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(getDimInfo("dim_base_province", "1"));
    }
}