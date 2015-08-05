package com.boco.od.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by mars on 2015/7/30.
 */
public class Util {
    private static SimpleDateFormat drd = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat drh = new SimpleDateFormat("yyyyMMddHH");
    private static SimpleDateFormat drs = new SimpleDateFormat("yyyyMMddHHmmss");

    public static long ParseDatebysec(String date) {
        try {
            return drs.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0L;
    }
    public static long calcTime(String small,String big) {
        try {
            return drs.parse(big).getTime()-drs.parse(small).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0L;
    }


    public static boolean isTheSameDay(String dt1,String dt2){
       return dt1.substring(0,8).equals(dt2.substring(0,8));
    }


    public static boolean isStartOfDay(String dt){
        return "000000".equals(dt.substring(8, 14));
    }
    public static boolean isEndOfDay(String dt){
        return "235959".equals(dt.substring(8, 14));
    }


    public static void main(String[] args) {
        String dt1 = "20150722021230";
        String dt2 = "20150722020510";
//        System.out.println(isTheSameDay(dt1, dt2));
        System.out.println(dt1.substring(8, 14));


//        System.out.println(ParseDatebysec(dt1));
//        System.out.println(ParseDatebysec(dt2));
    }

}
