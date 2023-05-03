package com.liweidao.apps.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

//计算user跳出行为
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new FsStateBackend("hdfs://liweidao//FlinkRtDh"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(6*10000L);
        //2 读取kafka ”dwd_page_log“ topic 数据
        String source_topic = "dwd_page_log";
        String groupId = "UserJumpDetailApp";
        String sink_topic = "dwm_user_jump_detail";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.FlinkSource(source_topic, groupId));

        //3 将数据转换为JSONObject格式
        SingleOutputStreamOperator<JSONObject> dataStream = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, s);
                }
            }
        });
        //4 提取时间戳并生成watermark
        SingleOutputStreamOperator<JSONObject> dataStream2 = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }));
        //5 按照mid进行keyby
        KeyedStream<JSONObject, String> keyedStream = dataStream2.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
        //6利用FLink CEP模块，进行复杂事件处理，匹配出跳出行为
        //6.1 模型定义
        Pattern<JSONObject, JSONObject> Pattern1 = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String last_page = jsonObject.getJSONObject("page").getString("last_page_id");
                        return last_page == null || last_page.length() <= 0;
                    }
                }).times(2)
                .consecutive()
                .within(Time.seconds(10));
        //6.2 模式应用
        PatternStream<JSONObject> pattern_Stream = CEP.pattern(keyedStream, Pattern1);
        //6。3 模式检测,检测与提取匹配事件
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("TimeOut") {
        };
        SingleOutputStreamOperator<JSONObject> pattern_select_stream = pattern_Stream
                .select(outputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        List<JSONObject> jsonObjects = map.get("start");
                        return jsonObjects.get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        List<JSONObject> jsonObjects = map.get("start");
                        return jsonObjects.get(0);
                    }
                });
        //7 将数据写入kafka
        DataStream<JSONObject> sideOutput_stream = pattern_select_stream.getSideOutput(outputTag);
        DataStream<JSONObject> union_stream = pattern_select_stream.union(sideOutput_stream);
        union_stream.print(">>>>>");
        union_stream.map(JSONAware::toJSONString).addSink(MyKafkaUtils.FlinkSink(sink_topic));
        env.execute();
    }
}
