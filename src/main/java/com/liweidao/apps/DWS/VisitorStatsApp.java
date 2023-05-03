package com.liweidao.apps.DWS;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liweidao.bean.VisitorStats;
import com.liweidao.utils.ClickHouseUtil;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Desc: 访客主题宽表计算
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性，所以先进行维度聚合
 * 进行关联  这是一个fulljoin
 * 可以考虑使用flinksql 完成
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.setStateBackend(new FsStateBackend("hdfs://liweidao/FlinkRtDh"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L*6);
        //2 获取kafka主题数据
        String groupId = "visitor_stats_app";
        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> page_stream = env.addSource(MyKafkaUtils.FlinkSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uv_stream = env.addSource(MyKafkaUtils.FlinkSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> user_jump_stream = env.addSource(MyKafkaUtils.FlinkSource(userJumpDetailSourceTopic, groupId));
        //3 合并数据流,利用union算子,合并之前，要求各个流的格式一致，首先对其进行转换
        //3.1 转换pv流
        SingleOutputStreamOperator<VisitorStats> page_map_stream = page_stream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new VisitorStats("", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 1L, 0L, 0L, jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts"));
            }
        });
        //3.2 转换sv流
        SingleOutputStreamOperator<VisitorStats> sessionVisDstream = page_stream.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String s, Context context, Collector<VisitorStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String lastPageId = jsonObject.getJSONObject("common").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    VisitorStats visitorStats = new VisitorStats("", "",
                            jsonObject.getJSONObject("common").getString("vc"),
                            jsonObject.getJSONObject("common").getString("ch"),
                            jsonObject.getJSONObject("common").getString("ar"),
                            jsonObject.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L, 0L,
                            jsonObject.getLong("ts"));
                    collector.collect(visitorStats);
                }
            }
        });
        //3.3 转换uv流
        SingleOutputStreamOperator<VisitorStats> uv_map_stream = uv_stream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new VisitorStats("", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        1L, 0L, 0L, 0L, 0L,
                        jsonObject.getLong("ts"));
            }
        });

        //3.4 转换跳转流
        SingleOutputStreamOperator<VisitorStats> user_jump_map_stream = user_jump_stream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new VisitorStats("", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 0L, 0L, 1L, 0L,
                        jsonObject.getLong("ts"));
            }
        });
        //4 利用union,合并四个流,并进行提取时间戳并生成watermark
        SingleOutputStreamOperator<VisitorStats> union_stream = uv_map_stream.union(page_map_stream, sessionVisDstream, user_jump_map_stream)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        }));
        //5 对数据进行keyby
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> key_data_stream = union_stream.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {

                return new Tuple4<>(visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new());
            }
        });
        //6 进行窗口聚合
        SingleOutputStreamOperator<VisitorStats> reduce_stream = key_data_stream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> iterable,
                                        Collector<VisitorStats> collector) throws Exception {
                        //补时间字段
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : iterable) {

                            String startDate = simpleDateFormat.format(new Date(context.window().getStart()));
                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));

                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            collector.collect(visitorStats);
                        }
                    }
                });
        //7 将数据写入clickHouse中
        reduce_stream.print();
        reduce_stream.addSink(ClickHouseUtil.<VisitorStats>getJdbcSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute("visitorstats");
    }
}
