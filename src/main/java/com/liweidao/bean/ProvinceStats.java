package com.liweidao.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

//地区主题bean
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private Long province_id;
    private String province_name;
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;
    private BigDecimal order_amount;
    private Long order_count;
    private Long ts;

    public ProvinceStats(OrderWide orderWide) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.stt = "";
        this.edt = "";
        this.province_id = orderWide.getProvince_id();
        this.order_amount = orderWide.getSplit_total_amount();
        this.province_name = orderWide.getProvince_name();
        this.province_area_code = orderWide.getProvince_area_code();
        this.province_iso_code = orderWide.getProvince_iso_code();
        this.province_3166_2_code = orderWide.getProvince_3166_2_code();

        this.order_count = 1L;
        this.ts = simpleDateFormat.parse(orderWide.getCreate_time()).getTime();
    }
}
