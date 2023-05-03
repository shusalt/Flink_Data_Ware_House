package com.liweidao.apps.DWM;

import com.alibaba.fastjson.JSONObject;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//独立访客
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //1执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new FsStateBackend("hdfs:/liweidao/FilnkRtDh"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //2读取kafka dwd_page_logz主题数据流
        String source_topic = "dwd_page_log";
        String group_id = "unique_visit_app";
        String sink_topic = "dwm_unique_visit";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.FlinkSource(source_topic, group_id));
        //3 将每行数据转换为JSONObject数据
        SingleOutputStreamOperator<JSONObject> processData = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, s);
                }
            }
        });
        //4 按mid分组
        KeyedStream<JSONObject, String> keyedStream = processData.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
        //5 利用状态实现过滤，过滤掉不是第一次访问的用户
        SingleOutputStreamOperator<JSONObject> filterStream = keyedStream.filter(new userUniquefile());
        filterStream.print("filter_Stream");
        filterStream.map(JSONObject::toString).addSink(MyKafkaUtils.FlinkSink(sink_topic));
        env.execute();
    }

    private static class userUniquefile extends RichFilterFunction<JSONObject> {
        private ValueState<String> valueState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化时间格式
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            //初始化状态
            ValueStateDescriptor<String> State_des = new ValueStateDescriptor<>("user_state", String.class);
            //创建state TTL
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            State_des.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(State_des);
        }

        @Override
        public boolean filter(JSONObject jsonObject) throws Exception {
            //从page_log取出上一次访问的page_id
            String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
            //判断上一次访问的page_id是否存在
            if (lastPage == null || lastPage.length() <= 0) {
                //取出状态
                String value = valueState.value();
                //取出时间戳
                Long ts = jsonObject.getLong("ts");
                String format = simpleDateFormat.format(ts);
                if (value == null || !value.equals(format)) {
                    valueState.update(format);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
}
