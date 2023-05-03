package com.liweidao.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateTimeUtil {
    public static Long toTs(String time) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time1 = simpleDateFormat.parse(time).getTime();
        return time1;
    }
}
