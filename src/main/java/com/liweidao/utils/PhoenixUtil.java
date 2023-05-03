package com.liweidao.utils;

import com.alibaba.fastjson.JSONObject;
import com.liweidao.common.ConstantConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//Phoenix连接工具
public class PhoenixUtil {
    //声明
    private static Connection connection;

    //初始化连接
    private static Connection inti() {
        try {
            Class.forName(ConstantConfig.PHOENIX_DRIVER);
            connection = DriverManager.getConnection(ConstantConfig.PHOENIX_SERVER);
            connection.setSchema(ConstantConfig.HBASE_SCHEMA);
            System.out.println("init成功");
            return connection;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix连接失败");
        }
    }

    //查询
    public static <T> List<T> queryList(String sql, Class<T> cls) {
        if (connection == null) {
            Connection inti = inti();
        }
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            ArrayList<T> list = new ArrayList<>();
            while (resultSet.next()) {
                T t = cls.newInstance();
                for (int i = 1; i < columnCount + 1; i++) {
                    BeanUtils.setProperty(t, metaData.getColumnName(i), resultSet.getObject(i));
                }
                list.add(t);

            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询维度信息失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }
//
//    //测试
//    public static void main(String[] args) {
//        System.out.println(queryList("select * from base_trademark",JSONObject.class));
//    }

}
