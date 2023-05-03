package com.liweidao.apps.DWD;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.text.SimpleDateFormat;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://192.168.31.105:8082/liweidao/FlinkRtDh"));
        //开启ck
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //设置系统属性
//        System.setProperty("HADOOP_USER_NAME","root");

        //2 读取Kafka "ods_base_log" topic数据
        String source_topic = "ods_base_log1";
        String group_id = "ods_dwd_base_log_app";
        String sink_topic_page = "dwd_page_log";
        String sink_topic_display = "dwd_display_log";
        String sink_topic_start = "dwd_start_log";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtils.FlinkSource(source_topic, group_id);
        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);

        //3 将每一行数据转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonStreamData = dataStreamSource.map(JSONObject::parseObject);

        //4 按照Mid分组
        KeyedStream<JSONObject, String> json_keyedStream = jsonStreamData.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        //5 使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> StreamData1 = json_keyedStream.map(new usercheck());

        //6 分流,使用processFunction将ODS数据拆分成启动，曝光，页面数据,其中启动日志与曝光日志输出到测输出流中
        OutputTag<String> start_out = new OutputTag<String>("start_log") {
        };
        OutputTag<String> display_out = new OutputTag<String>("display_log") {
        };
        SingleOutputStreamOperator<String> pageStream = StreamData1.process(new splitStream(start_out, display_out));

        //7 获取侧输出流数据 ,并将3个流分别写入各自kafka topic
        DataStream<String> startStream = pageStream.getSideOutput(start_out);
        DataStream<String> displayStream = pageStream.getSideOutput(display_out);
        pageStream.addSink(MyKafkaUtils.FlinkSink(sink_topic_page));
        displayStream.addSink(MyKafkaUtils.FlinkSink(sink_topic_display));
        startStream.addSink(MyKafkaUtils.FlinkSink(sink_topic_start));
//        pageStream.print("pageStream:");
//        displayStream.print("displayStream:");
//        startStream.print("startStream");

        //8 执行程序
        env.execute();

    }


    //------
    //定义一个RichMapfunction,做新老用户校验
    //------
    private static class usercheck extends RichMapFunction<JSONObject, JSONObject> {
        //声明状态用于表示当前Mid是否已经访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleDateFormat;

        //初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject jsonObject) throws Exception {
            //取出新用户标识
            String biaoji = jsonObject.getJSONObject("common").getString("is_new");
            if ("1".equals(biaoji)) {
                //取出状态值
                String state_value = firstVisitDateState.value();
                Long ts = jsonObject.getLong("ts");
                if (state_value != null) {
                    //修复数据
                    jsonObject.getJSONObject("common").put("is_new", 0);
                } else {
                    //否则更新状态值
                    firstVisitDateState.update(simpleDateFormat.format(ts));
                }
            }
            return jsonObject;
        }
    }

    //------
    //定义一个processfunction,进行分流处理
    //------
    private static class splitStream extends ProcessFunction<JSONObject, String> {
        private OutputTag<String> start_out;
        private OutputTag<String> display_out;

        public splitStream(OutputTag<String> start_out, OutputTag<String> display_out) {
            this.start_out = start_out;
            this.display_out = display_out;
        }

        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
            //在log_json数据中,start_log与display,page是不同时存在的,在这里page数据主流输出包含display数据
            //获取启动日志
            String start_log = jsonObject.getString("start");
            if (start_log != null && start_log.length() > 0) {
                //输出start数据到start_out侧输出流中
                context.output(start_out, start_log);
            } else {
                //输出page数据到主流中
                collector.collect(jsonObject.toString());
                //获取曝光数据
                JSONArray display_log = jsonObject.getJSONArray("displays");
                if (display_log != null && display_log.size() > 0) {
                    for (int i = 0; i < display_log.size(); i++) {
                        JSONObject display_log_value = display_log.getJSONObject(i);
                        display_log_value.put("page_id", jsonObject.getJSONObject("page").getString("page_id"));
                        //输出display数据到display_out侧输出流中
                        context.output(display_out, display_log_value.toString());
                    }
                }
            }
        }

    }
}

