package com.boco.od.utils;

import java.math.BigDecimal;

/**
 * Created by mars on 2015/8/5.
 */
public class DistanceUtils {
/*
    public static Double calcDistance(String start_lng,String start_lat,String end_lng,String end_lat){
        int AVERAGE_RADIUS_OF_EARTH = 6371;

        double dstart_lng = Double.valueOf(start_lat);
        double dstart_lat = Double.valueOf(start_lat);
        double dend_lng = Double.valueOf(end_lng);
        double dend_lat = Double.valueOf(end_lat);

        double latDistance = Math.toRadians(dstart_lat - dend_lng);
        double lngDistance = Math.toRadians(dstart_lng - dend_lat);

        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(Math.toRadians(dstart_lat)) * Math.cos(Math.toRadians(dend_lat)) *
                        Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        BigDecimal b = new BigDecimal(AVERAGE_RADIUS_OF_EARTH * c);

        return b.setScale(2, BigDecimal.ROUND_HALF_EVEN).doubleValue();
    }

*/

    private static  double EARTH_RADIUS = 6378.137;//地球半径
    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }

    public static double getDistance(String slat1, String slng1, String slat2, String slng2)
    {
        double lng1 = Double.valueOf(slng1);
        double lat1 = Double.valueOf(slat1);
        double lng2 = Double.valueOf(slng2);
        double lat2 = Double.valueOf(slat2);

        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);

        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
                Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2),2)));
        s = s * EARTH_RADIUS;
//        s = Math.round(s * 10000) / 10000;
        BigDecimal d = new BigDecimal(s);
        return d.setScale(2, BigDecimal.ROUND_HALF_EVEN).doubleValue();
    }



    public static void main(String[] args) {
//        System.out.println(DistanceUtils.calcDistance("116.321265", "39.938416", "116.321211", "39.941763"));
        System.out.println(DistanceUtils.getDistance("116.321265", "39.938416", "116.321211", "39.941763"));
    }

}
