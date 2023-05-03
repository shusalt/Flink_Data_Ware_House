package com.liweidao.apps.DWS;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.liweidao.apps.functions.DimAsyncFunction;
import com.liweidao.bean.OrderWide;
import com.liweidao.bean.PaymentWide;
import com.liweidao.bean.ProductStats;
import com.liweidao.common.diansConstant;
import com.liweidao.utils.ClickHouseUtil;
import com.liweidao.utils.DateTimeUtil;
import com.liweidao.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 */
//商品主题表
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.setStateBackend(new FsStateBackend("hdfs://liweidao/FlinkRtDh"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L*6);
        //2 获取kafka数据
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> page_source_data = env.addSource(MyKafkaUtils.FlinkSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> orderwide_source_data = env.addSource(MyKafkaUtils.FlinkSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paywide_source_data = env.addSource(MyKafkaUtils.FlinkSource(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartinfo_source_data = env.addSource(MyKafkaUtils.FlinkSource(cartInfoSourceTopic, groupId));
        DataStreamSource<String> favorinfo_source_data = env.addSource(MyKafkaUtils.FlinkSource(favorInfoSourceTopic, groupId));
        DataStreamSource<String> order_refund_info = env.addSource(MyKafkaUtils.FlinkSource(refundInfoSourceTopic, groupId));
        DataStreamSource<String> comment_source_data = env.addSource(MyKafkaUtils.FlinkSource(commentInfoSourceTopic, groupId));
        //3 对数据进行转换，转换成统一的格式
        //3.1 转换曝光及页面流数据
        SingleOutputStreamOperator<ProductStats> page_process = page_source_data.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String s, Context context, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                if (page_id == null) {
                    System.out.println(jsonObject);
                }
                Long ts = jsonObject.getLong("ts");
                if (page_id.equals("good_detail")) {
                    Long item = page.getLong("item");
                    ProductStats build = ProductStats.builder().sku_id(item)
                            .click_ct(1L)
                            .ts(ts).build();
                    collector.collect(build);
                }
                JSONArray displays = jsonObject.getJSONArray("display");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if (display.getString("item_type").equals("sku_id")) {
                            Long item = display.getLong("item");
                            ProductStats.builder()
                                    .sku_id(item)
                                    .display_ct(1L)
                                    .ts(ts).build();
                        }
                    }
                }
            }
        });
        //3.2 转换下单流数据
        SingleOutputStreamOperator<ProductStats> orderwide_map_stream = orderwide_source_data.map(json -> {
            OrderWide orderwide = JSON.parseObject(json, OrderWide.class);
            System.out.println("orderwide:====" + orderwide);
            String create_time = orderwide.getCreate_time();
            Long ts = DateTimeUtil.toTs(create_time);
            return ProductStats.builder()
                    .sku_id(orderwide.getSku_id())
                    .orderIdSet(new HashSet(Collections.singleton(orderwide.getOrder_id())))
                    .order_sku_num(orderwide.getSku_num())
                    .order_amount(orderwide.getSplit_activity_amount()).ts(ts).build();
        });
        //3.3 转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favor_map_stream = favorinfo_source_data.map(json -> {
            JSONObject favorInfo = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(favorInfo.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(favorInfo.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(ts)
                    .build();
        });
        //3.4 转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cartinfo_map_stream = cartinfo_source_data.map(json -> {
            JSONObject cartinfo = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(cartinfo.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(cartinfo.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(ts)
                    .build();
        });
        //3.5 转换支付信息流数据
        SingleOutputStreamOperator<ProductStats> payment_map_stream = paywide_source_data.map(json -> {
            PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
            Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
            return ProductStats
                    .builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                    .ts(ts)
                    .build();
        });
        //3.6 转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundinfo_map_stream = order_refund_info.map(json -> {
            JSONObject refund_info = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(refund_info.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(refund_info.getLong("sku_id"))
                    .refund_amount(refund_info.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(new HashSet(Collections.singleton(refund_info.getLong("order_id"))))
                    .ts(ts)
                    .build();
        });
        //3.7 转换评价流数据
        SingleOutputStreamOperator<ProductStats> comment_map_stream = comment_source_data.map(json -> {
            JSONObject comment_data = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(comment_data.getString("create_time"));
            Long GoodCt = diansConstant.APPRAISE_GOOD.equals(comment_data.getString("appraise")) ? 0L : 1L;
            return ProductStats.builder()
                    .sku_id(comment_data.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(GoodCt)
                    .ts(ts)
                    .build();
        });
        //4 对各个流进行合并
        SingleOutputStreamOperator<ProductStats> union_stream_data = page_process.union(cartinfo_map_stream, comment_map_stream, favor_map_stream,
                orderwide_map_stream, payment_map_stream, refundinfo_map_stream)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats productStats, long l) {
                                return productStats.getTs();
                            }
                        }));
        //5 按照sku_id进行keyby，利用时间窗口进行聚合
        SingleOutputStreamOperator<ProductStats> product_reduce_stream = union_stream_data.keyBy(new KeySelector<ProductStats, Long>() {
            @Override
            public Long getKey(ProductStats productStats) throws Exception {
                return productStats.getSku_id();
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new union_stream_reduce(), new union_stream_wf());
        //6 补充维度数据
        //6.1 补充sku维度数据
        SingleOutputStreamOperator<ProductStats> buchong_stream = AsyncDataStream.unorderedWait(product_reduce_stream, new
                DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getSku_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                        productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                        productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                        productStats.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);
        //6.2 补充spu维度数据
        SingleOutputStreamOperator<ProductStats> buchong_stream2 = AsyncDataStream.unorderedWait(buchong_stream, new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return String.valueOf(input.getSpu_id());
            }

            @Override
            public void join(ProductStats input, JSONObject diminfo) throws Exception {
                input.setSpu_name(diminfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);
        //6.3 补充品类维度数据
        SingleOutputStreamOperator<ProductStats> buchong_stream3 = AsyncDataStream.unorderedWait(buchong_stream2, new
                DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getCategory3_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject diminfo) throws Exception {
                        input.setCategory3_name(diminfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);
        //6.4 补充商品品牌维度数据
        SingleOutputStreamOperator<ProductStats> buchong_stream4 = AsyncDataStream.unorderedWait(buchong_stream3, new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(ProductStats input) {
                return String.valueOf(input.getTm_id());
            }

            @Override
            public void join(ProductStats input, JSONObject diminfo) throws Exception {
                input.setTm_name(diminfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);
        // 写入click house中
        buchong_stream4.print();
        buchong_stream4.addSink(ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?)"));
        env.execute();
    }

    private static class union_stream_reduce implements ReduceFunction<ProductStats> {
        @Override
        public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
            //曝光数 点击数 收藏数 加入购物车数
            stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
            stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
            stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
            stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
            //下单商品金额 下单数  下单商品数
            stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
            stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
            stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
            stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
            //支付金额
            stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
            //退单数 退单商品金额
            stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
            stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
            stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
            //支付订单数
            stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
            stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
            //好评数评
            stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
            stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
            return stats1;
        }
    }

    private static class union_stream_wf implements WindowFunction<ProductStats, ProductStats, Long, TimeWindow> {
        @Override
        public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (ProductStats productStats : iterable) {
                productStats.setStt(simpleDateFormat.format(timeWindow.getStart()));
                productStats.setEdt(simpleDateFormat.format(timeWindow.getEnd()));
                productStats.setTs(new Date().getTime());
                collector.collect(productStats);
            }
        }
    }
}