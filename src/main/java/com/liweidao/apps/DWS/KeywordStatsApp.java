package com.liweidao.apps.DWS;

import com.liweidao.apps.UTF.KeywordUDTF;
import com.liweidao.bean.KeywordStats;
import com.liweidao.common.diansConstant;
import com.liweidao.utils.ClickHouseUtil;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.setStateBackend(new FsStateBackend("hdfs://liweidao/FlinkRtDh"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L*6);
        //2 定义流表环境
        EnvironmentSettings set = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, set);
        //3 注册udf函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //4 将数据源定义为动态表
        String groupid = "keyword_stats_app";
        String pageViewTopic = "dwd_page_log";

        String DDL = "CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtils.getKafkaDDL(pageViewTopic, groupid) + ")";
        tableEnv.executeSql(DDL);
        String select_sql1 = "select page['item'] fullword ," +
                "rowtime from page_view  " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ";
        Table table1 = tableEnv.sqlQuery(select_sql1);
        String select_sql2 = "select keyword,rowtime from " + table1 + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)";
        Table table2 = tableEnv.sqlQuery(select_sql2);
        //窗口聚合
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + diansConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   " + table2
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);
        keywordStatsDataStream.print("keywordStats>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.<KeywordStats>getJdbcSink("insert into " +
                "keyword_stats2(keyword,ct,source,stt,edt,ts) " +
                "values(?,?,?,?,?,?)"));
        env.execute();
    }
}
