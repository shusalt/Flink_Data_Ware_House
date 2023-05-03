package com.liweidao.apps.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liweidao.apps.functions.DimAsyncFunction;
import com.liweidao.bean.OrderDetail;
import com.liweidao.bean.OrderInfo;
import com.liweidao.bean.OrderWide;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //1 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new FsStateBackend("hdfs://liweidao/FlinkRtDh"));
//        env.enableCheckpointing(6*10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //2 读取Kafka订单和订单明细 topic数据 dwd_order_info  dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide20";
        String groupId = "order_wide_group20";
        DataStreamSource<String> orderinfor_source = env.addSource(MyKafkaUtils.FlinkSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderdetail_source = env.addSource(MyKafkaUtils.FlinkSource(orderDetailSourceTopic, groupId));
        orderinfor_source.print("dwd_order_info>>>>");
        orderdetail_source.print("dwd_order_detail>>>>");
        //3 对数据进进行转换，转换成javabean, 提取时间时间戳并生成watermark，按照id进行keyby
        SingleOutputStreamOperator<OrderInfo> orderinfo_stream = orderinfor_source.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String s) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                //取出创建时间字段
                String create_time = orderInfo.getCreate_time();
                String[] split = create_time.split(" ");
                orderInfo.setCreate_date(split[0]);
                orderInfo.setCreate_hour(split[1]);
                orderInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                return orderInfo;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));


        SingleOutputStreamOperator<OrderDetail> orderdetail_stream = orderdetail_source.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String s) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);
                orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                }));
        KeyedStream<OrderInfo, Long> orderinfo_key_stream = orderinfo_stream.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderdetail_key_stream = orderdetail_stream.keyBy(OrderDetail::getOrder_id);
//        orderinfo_stream.print();
//        orderdetail_stream.print();

        //4 双流join
        SingleOutputStreamOperator<OrderWide> join_stream = orderinfo_key_stream.intervalJoin(orderdetail_key_stream)
                .between(Time.seconds(-5), Time.seconds(5))//生产环境,为了不丢数据,设置时间为最大网络延迟
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        join_stream.print();
        //5 关联维度数据
        //5.1 关联用户维度数据
        SingleOutputStreamOperator<OrderWide> orderwide1 = AsyncDataStream.unorderedWait(join_stream, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

            @Override
            public String getKey(OrderWide input) {
                return input.getUser_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject diminfo) throws ParseException {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                //取出用户维度中的生日
                String birthday = diminfo.getString("BIRTHDAY");
                long curretTS = System.currentTimeMillis();
                long ts = simpleDateFormat.parse(birthday).getTime();
                //将生日字段处理成年纪
                Long ageLong = (curretTS - ts) / 1000L / 60 / 60 / 24 / 365;
                input.setUser_age(ageLong.intValue());
                //取出用户维度中的性别
                String gender = diminfo.getString("GENDER");
                input.setUser_gender(gender);
            }
        }, 60, TimeUnit.SECONDS);
        //5.2 关联省市维度数据
        SingleOutputStreamOperator<OrderWide> orderwide2 = AsyncDataStream.unorderedWait(orderwide1, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject diminfo) throws Exception {
                //提取维度信息并设置进orderwide
                input.setProvince_name(diminfo.getString("NAME"));
                input.setProvince_area_code(diminfo.getString("AREA_CODE"));
                input.setProvince_iso_code(diminfo.getString("ISO_CODE"));
                input.setProvince_3166_2_code(diminfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);
        //5.3 关联SKU维度数据
        SingleOutputStreamOperator<OrderWide> orderwide3 = AsyncDataStream.unorderedWait(orderwide2, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getSku_id());
            }

            @Override
            public void join(OrderWide input, JSONObject diminfo) throws Exception {
                input.setSku_name(diminfo.getString("SKU_NAME"));
                input.setCategory3_id(diminfo.getLong("CATEGORY3_ID"));
                input.setSpu_id(diminfo.getLong("SPU_ID"));
                input.setTm_id(diminfo.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);
        //5.4D.关联SPU维度数据
        SingleOutputStreamOperator<OrderWide> orderwide4 = AsyncDataStream.unorderedWait(orderwide3, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getSpu_id());
            }

            @Override
            public void join(OrderWide input, JSONObject diminfo) throws Exception {
                input.setSpu_name(diminfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);
        //5.5 关联品牌维度数据
        SingleOutputStreamOperator<OrderWide> orderwide5 = AsyncDataStream.unorderedWait(orderwide4, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getTm_id());
            }

            @Override
            public void join(OrderWide input, JSONObject diminfo) throws Exception {
                input.setTm_name(diminfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);
        //5.6 关联品类维度数据
        SingleOutputStreamOperator<OrderWide> orderwide6 = AsyncDataStream.unorderedWait(orderwide5, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getCategory3_id());
            }

            @Override
            public void join(OrderWide input, JSONObject diminfo) throws Exception {
                input.setCategory3_name(diminfo.getString("NAME"));
            }
        }, 60, TimeUnit.SECONDS);
        //输出到kafka中
        orderwide6.print("order_wide>>>>>>");
        orderwide6.map(JSON::toJSONString).addSink(MyKafkaUtils.FlinkSink(orderWideSinkTopic));
        env.execute();
    }
}
