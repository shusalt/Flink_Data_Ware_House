package com.liweidao.apps.DWS;

import com.alibaba.fastjson.JSON;
import com.liweidao.bean.OrderWide;
import com.liweidao.bean.ProvinceStats;
import com.liweidao.utils.ClickHouseUtil;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Desc: FlinkSQL实现地区主题宽表计算
 */
//地区主题
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new FsStateBackend("hdfs://liweidao/FlinkRtHd"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L*6);
        // 定义流表（StreamTable）环境
        //-----------
        //测试FLinkSQL 业务线行不通,改用streamAPI
        //-----------
//        EnvironmentSettings set = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, set);
//        //2 把数据源定义为动态表
//        String groupid="province_stats20";
//        String orderWide_topic="dwm_order_wide20";
//        String DDL="CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
//                "province_name STRING,province_area_code STRING," +
//                "province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
//                "total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
//                "WATERMARK FOR rowtime AS rowtime)" +
//                " WITH (" + MyKafkaUtils.getKafkaDDL(orderWide_topic, groupid) + ")";
//        tableEnv.executeSql(DDL);
//        Table table1 = tableEnv.sqlQuery("select * from ORDER_WIDE");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table1, Row.class);
//        rowDataStream.print("province>>>>");
//        //聚合计算
//        String juheSQL="select " +
//                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
//                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
//                " province_id,province_name,province_area_code," +
//                "province_iso_code,province_3166_2_code," +
//                "COUNT( DISTINCT order_id) order_count, sum(total_amount) order_amount," +
//                "UNIX_TIMESTAMP()*1000 ts "+
//                " from ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
//                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ";
//        Table table = tableEnv.sqlQuery(juheSQL);
//        DataStream<Row> provinceStatsDataStream1= tableEnv.toAppendStream(table, Row.class);
//        provinceStatsDataStream1.print("provinceStatsRow>>>>");
//        DataStream<ProvinceStats> provinceStatsDataStream2 = tableEnv.toAppendStream(table, ProvinceStats.class);
//        provinceStatsDataStream2.print("provinceStats>>>>");
//        provinceStatsDataStream2.addSink(ClickHouseUtil.<ProvinceStats>getJdbcSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));
        //-------
        //Stream API业务线
        //-------
        String groupid = "province_stats20";
        String orderWide_topic = "dwm_order_wide20";
        //1 读取kafka order_wide topic数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.FlinkSource(orderWide_topic, groupid));
        SingleOutputStreamOperator<ProvinceStats> provinceDs = dataStreamSource.map(json -> {
            OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
            return new ProvinceStats(orderWide);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ProvinceStats>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<ProvinceStats>() {
                    @Override
                    public long extractTimestamp(ProvinceStats provinceStats, long l) {
                        return provinceStats.getTs();
                    }
                }));
        KeyedStream<ProvinceStats, String> key_province_stream = provinceDs.keyBy(new KeySelector<ProvinceStats, String>() {
            @Override
            public String getKey(ProvinceStats provinceStats) throws Exception {
                return provinceStats.getProvince_name();
            }
        });
        SingleOutputStreamOperator<ProvinceStats> reduce = key_province_stream
                .window(TumblingEventTimeWindows.of(Time.seconds(1L)))
                .reduce(new reduce_ft(), new wf());
        reduce.print("aaaa>>>>");
        reduce.addSink(ClickHouseUtil.<ProvinceStats>getJdbcSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }

    //-------
    //自定义reducefunction函数
    //-------
    private static class reduce_ft implements ReduceFunction<ProvinceStats> {
        @Override
        public ProvinceStats reduce(ProvinceStats t1, ProvinceStats t2) throws Exception {
            t1.setOrder_count(t1.getOrder_count() + t2.getOrder_count());
            t1.setOrder_amount(t1.getOrder_amount().add(t2.getOrder_amount()));
            return t1;
        }
    }

    //-------
    //自定义window function函数
    //-------
    private static class wf implements WindowFunction<ProvinceStats, ProvinceStats, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<ProvinceStats> iterable, Collector<ProvinceStats> collector) throws Exception {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd MM:hh:ss");
            for (ProvinceStats province : iterable) {
                province.setStt(simpleDateFormat.format(new Date(timeWindow.getStart())));
                province.setEdt(simpleDateFormat.format(new Date(timeWindow.getEnd())));
                collector.collect(province);
            }
        }
    }
}
