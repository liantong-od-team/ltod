package com.boco.od.common;

/**
 * Created by ranhualin on 2015/8/3.
 */
public class DimensionLrc implements Comparable<DimensionLrc>{

    private String province;
    private String city;
    private String month;
    private String day;
    public DimensionLrc(){

    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public DimensionLrc(String province,String city,String month,String day){
        this.province=province;
        this.city=city;
        this.month=month;
        this.day=day;
    }
    @Override
    public int compareTo(DimensionLrc o) {
        if (!month.equals(o.getMonth())) {
            return  month.compareTo(o.getMonth());
        } else if (day != o.getDay()) {
            return day.compareTo(o.getDay());
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "DimensionLrc{" +
                "province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", month='" + month + '\'' +
                ", day='" + day + '\'' +
                '}';
    }
}
