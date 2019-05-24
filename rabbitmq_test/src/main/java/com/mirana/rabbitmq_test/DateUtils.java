package com.mirana.rabbitmq_test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    public static String format() {
        return format(new Date());
    }

    public static String format(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    }
}
