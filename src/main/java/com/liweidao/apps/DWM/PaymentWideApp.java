package com.liweidao.apps.DWM;

import com.alibaba.fastjson.JSON;
import com.liweidao.bean.OrderWide;
import com.liweidao.bean.PaymentInfo;
import com.liweidao.bean.PaymentWide;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Mock -> Mysql(binLog) -> MaxWell -> Kafka(ods_base_db_m) -> BaseDBApp(修改配置,Phoenix)
 * -> Kafka(dwd_order_info,dwd_order_detail) -> OrderWideApp(关联维度,Redis) -> Kafka(dwm_order_wide)
 * -> PaymentWideApp -> Kafka(dwm_payment_wide)
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //修改用户名
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //2.读取Kafka主题数据  dwd_payment_info  dwm_order_wide
        String groupId = "payment_wide_group20";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide20";
        String paymentWideSinkTopic = "dwm_payment_wide20";
        DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtils.FlinkSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtils.FlinkSource(orderWideSourceTopic, groupId));
        //3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS
                .map(new MapFunction<String, PaymentInfo>() {
                    @Override
                    public PaymentInfo map(String s) throws Exception {
                        ArrayList<String> arrayList = new ArrayList<>();
                        arrayList.add("String");
                        arrayList.add("Long");
                        arrayList.add("BigDecimal");
                        PaymentInfo data = JSON.parseObject(s, PaymentInfo.class);
                        for (Field field : data.getClass().getDeclaredFields()) {
                            field.setAccessible(true);
                            if (field.get(data) == null) {
                                if (arrayList.get(0).equals(field.getType().getSimpleName())) {
                                    field.set(data, "0");
                                } else if (arrayList.get(1).equals(field.getType().getSimpleName())) {
                                    field.set(data, 0L);
                                } else if (arrayList.get(2).equals(field.getType().getSimpleName())) {
                                    field.set(data, BigDecimal.valueOf(0));
                                }
                            }
                        }
                        return data;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS
                .map(new MapFunction<String, OrderWide>() {

                    @Override
                    public OrderWide map(String s) throws Exception {
                        ArrayList<String> arrayList = new ArrayList<>();
                        arrayList.add("String");
                        arrayList.add("Long");
                        arrayList.add("Integer");
                        arrayList.add("BigDecimal");
                        OrderWide data = JSON.parseObject(s, OrderWide.class);
                        for (Field field : data.getClass().getDeclaredFields()) {
                            field.setAccessible(true);
                            if (field.get(data) == null) {
                                if (arrayList.get(0).equals(field.getType().getSimpleName())) {
                                    field.set(data, "0");
                                } else if (arrayList.get(1).equals(field.getType().getSimpleName())) {
                                    field.set(data, 0L);
                                } else if (arrayList.get(2).equals(field.getType().getSimpleName())) {
                                    field.set(data, 0);
                                } else if (arrayList.get(3).equals(field.getType().getSimpleName())) {
                                    field.set(data, BigDecimal.valueOf(0));
                                }
                            }
                        }
                        return data;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));
//        orderWideDS.print("order_wide>>>>>");
        //4.按照OrderID分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream = orderWideDS.keyBy(OrderWide::getOrder_id);

        //5.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //打印测试
        paymentWideDS.print(">>>>>>>>>>");
        //6.将数据写入Kafka  dwm_payment_wide
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtils.FlinkSink(paymentWideSinkTopic));
        //7.启动任务
        env.execute();

    }
}
