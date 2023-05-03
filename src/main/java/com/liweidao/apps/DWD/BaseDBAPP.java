package com.liweidao.apps.DWD;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.liweidao.apps.functions.DimSink;
import com.liweidao.apps.functions.tableProcess;
import com.liweidao.bean.TableProcessBean;

import com.liweidao.utils.MyKafkaUtils;

import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;


public class BaseDBAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new FsStateBackend("hdfs://192.168.31.105:8082/liweidao/FlinkRtDh"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        System.setProperty("HADOOP_USER_NAME","root");
        String source_topic = "ods_base_db20";
        String group_id = "ods_db_group20";
        //2 读取kafka数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.FlinkSource(source_topic, group_id));
        //3 转换数据为kafka
        SingleOutputStreamOperator<JSONObject> db_json_data = dataStreamSource.map(JSONObject::parseObject);

        //4 过滤数据
        SingleOutputStreamOperator<JSONObject> filter_data = db_json_data.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String data = jsonObject.getString("data");
                return data != null && data.length() > 0;
            }
        });

        //5 利用Flink_CDC读取mysql中的用于分流的配置表
        DebeziumSourceFunction<String> table_process_CDC = MySQLSource.<String>builder()
                .hostname("192.168.31.145")
                .port(3306)
                .username("user1")
                .password("liwei135646")
                .databaseList("realtime")
                .tableList("realtime.table_process")
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    //反序列化方法
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        //库名&表名
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db = split[1];
                        String table = split[2];
                        //获取数据
                        Struct value = (Struct) sourceRecord.value();
                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        //获取操作类型
                        Envelope.Operation oo = Envelope.operationFor(sourceRecord);
                        String operation = oo.toString().toLowerCase();
                        if ("create".equals(operation)) {
                            operation = "insert";
                        }
                        //创建JSON用于存放最终的结果
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", table);
                        result.put("operation", operation);
                        result.put("data", data);

                        collector.collect(result.toJSONString());
                    }

                    //定义数据类型
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        DataStreamSource<String> table_process_data = env.addSource(table_process_CDC);
        table_process_data.print("cdc:");
//        filter_data.print("主流1:");

        //6 将配置信息流作为广播流
        MapStateDescriptor<String, TableProcessBean> mapStateDescriptor = new MapStateDescriptor<String, TableProcessBean>("table_process_state", String.class,
                TableProcessBean.class);
        BroadcastStream<String> broadcastStream = table_process_data.broadcast(mapStateDescriptor);

        //7 将主流与广播流进行连接
        BroadcastConnectedStream<JSONObject, String> connectStream = filter_data.connect(broadcastStream);

        //8 分流处理
        OutputTag<JSONObject> talbeProcess_tag = new OutputTag<JSONObject>("table_process") {
        };
        SingleOutputStreamOperator<JSONObject> process = connectStream.process(new tableProcess(talbeProcess_tag, mapStateDescriptor));
        //8.1 获取侧输出流
        DataStream<JSONObject> sideOutput = process.getSideOutput(talbeProcess_tag);

        //9 分流保存到各自对应的sink中
//        process.print("输入到kafka的数据:");
//        sideOutput.print("输入到HBASE的数据:");
        sideOutput.addSink(new DimSink());
        FlinkKafkaProducer<JSONObject> kafka_sink = MyKafkaUtils.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) {
                System.out.println("开始初始化kafka数据");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sink_table"),
                        jsonObject.getString("data").getBytes());
            }
        });
        process.addSink(kafka_sink);
        //10 执行程序
        env.execute();
    }
}
