package com.liweidao.apps.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liweidao.bean.TableProcessBean;
import com.liweidao.common.ConstantConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class tableProcess extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcessBean> mapStateDescriptor;

    public tableProcess(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcessBean> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //定义phoenix连接
    private Connection connection = null;

    //初始化连接,在测试中初始没有响应,在DriverManager.getConnection()长时间运行,没有连接phoenix
    // 导致后面处理函数没有执行，初始化有问题,
    //最好在window下的window/system/driver/etc/hosts 做linux服务器的 ip 主机名映射,然后就成功解决问题
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("开始初始化");
        Class.forName(ConstantConfig.PHOENIX_DRIVER);
        System.out.println("实例化驱动类成功");
        connection = DriverManager.getConnection(ConstantConfig.PHOENIX_SERVER);
        System.out.println("初始化结束");
    }


    //处理广播流
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        //获取状态
        BroadcastState<String, TableProcessBean> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //将配置信息流中的数据转换为JSON对象{"database":"","table":"","type","","data":{"":""}}
        JSONObject jsonObject = JSON.parseObject(s);
//        System.out.println("处理广播流函数中"+jsonObject.toString());
        //取出数据中的表名以及操作类型封装key,获取data
        JSONObject data = jsonObject.getJSONObject("data");
        String table = data.getString("source_table");
        String type = data.getString("operate_type");
        String key = table + ":" + type;
        //将jsonobject对象解析为TableProcess bean对象
        TableProcessBean tableProcess22 = JSON.parseObject(data.toString(), TableProcessBean.class);
        //检验表功能，检验该表在phoenix中是否存在，如果不存在则在phoenix中建该表
        if (TableProcessBean.SINK_TYPE_HBASE.equals(tableProcess22.getSinkType())) {
            CheckTable(tableProcess22.getSinkTable(), tableProcess22.getSinkColumns(), tableProcess22.getSinkPk(), tableProcess22.getSinkExtend());
        }
//        System.out.println("Key:"+key+","+tableProcess22);
        broadcastState.put(key, tableProcess22);
    }

    //检验表__method
//        /**
//         * Phoenix建表
//         *
//         * @param sinkTable   表名       test
//         * @param sinkColumns 表名字段   id,name,sex
//         * @param sinkPk      表主键     id
//         * @param sinkExtend  表扩展字段 ""
//         *                    create table if not exists mydb.test(id varchar primary key,name varchar,sex varchar) ...
//         */
    private void CheckTable(String sinkTable, String sinkColumns, String sinkKey, String sinkExtend) {
        if (sinkKey == null) {
            sinkKey = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //封装建表语句SQL
        StringBuffer createSQl = new StringBuffer("create table if not exists ");
        createSQl.append(ConstantConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");
        //遍历字段
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            //判断字段是否为主键
            if (sinkKey.equals(column)) {
                createSQl.append(column).append(" varchar primary key ");
            } else {
                createSQl.append(column).append(" varchar ");
            }
            //判断当前字段是否为最好一个字段
            if (i < columns.length - 1) {
                createSQl.append(",");
            }
        }
        createSQl.append(")");
        createSQl.append(sinkExtend);
        System.out.println("建表SQL:" + createSQl.toString());
        //执行建表语句
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSQl.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表:" + sinkTable + " fail");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException s) {
                    s.printStackTrace();
                }
            }
        }
    }


    //处理主流 核心处理方法，根据MySQL配置表的信息为每条数据打标签，走Kafka还是HBase
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //获取状态
        ReadOnlyBroadcastState<String, TableProcessBean> readOnlyBroadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        //获取主流信息的表名与操作类型
        String table = jsonObject.getString("table");
        String operate = jsonObject.getString("operation");
        String key = table + ":" + operate;

        //根据key获取配置信息broadcast流中TableProcess数据
        TableProcessBean tableProcess = readOnlyBroadcastState.get(key);
        if (tableProcess != null) {
            //先主流追加数据
            jsonObject.put("sink_table", tableProcess.getSinkTable());

            //根据配置信息中提供的字段做数据过滤,"校验字段,过滤掉多余的字段",自定义一个函数
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());

            //判断当前配置信息流的数据的sink_type为kafka还是HBASE
            if (TableProcessBean.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //kafka数据，写入到主流中
                collector.collect(jsonObject);
            } else if (TableProcessBean.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //HBASE数据写到sideoutput中
                readOnlyContext.output(outputTag, jsonObject);
            }
        } else {
            System.out.println("key" + key + "ON In mysql 配置信息表");
        }
    }

    //根据配置信息数据进行校验字段，(过滤主流中多余字段)
    private void filterColumn(JSONObject data, String sinkColumns) {
        //保留的数据字段
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
//            Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        //        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!fieldList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
        entries.removeIf(next -> !fieldList.contains(next.getKey()));
    }
}
