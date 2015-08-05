package com.boco.od.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class DateUtils {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
    private static final long dayMs = 24*60*60*1000;

    public static long convertString2Time(String date){
        long error = -1;
        if(date!=null&&date.length()==14){
            try {
                return sdf.parse(date).getTime();
            } catch (ParseException e) {
                return error;
            }
        }else {
            return error;
        }
    }
    public static String convertTime2FullString(long time){
        Date date = new Date(time);
        return sdf.format(date);
    }
    public static String getNextDay(long time){
        //当前时间毫秒数加上一天的毫秒数
        long ntime = time +dayMs;
        return convertTime2FullString(ntime).substring(0,8);
    }
}
