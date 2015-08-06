package com.boco.od.ods;

import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class OdTraceRecord implements Writable,Cloneable {

    private String dateDay;
//    private String msisdn;
    private long time;
    private String cellId;
    private String lac;
    private String longitude;
    private String latitude;
    private String cell_province;
    private String cell_city;
    private String cell_county;
    private String area_id;
    private String lrc_province;
    private String lrc_city;

    @Override
    public void readFields(DataInput in) throws IOException {
        dateDay= in.readUTF();
//        msisdn = in.readUTF();
        time = in.readLong();
        cellId= in.readUTF();
        lac= in.readUTF();
        longitude=in.readUTF();
        latitude=in.readUTF();
        cell_province= in.readUTF();
        cell_city= in.readUTF();
        cell_county= in.readUTF();
        area_id= in.readUTF();
        lrc_province= in.readUTF();
        lrc_city= in.readUTF();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(dateDay);
//        out.writeUTF(msisdn);
        out.writeLong(time);
        out.writeUTF(cellId);
        out.writeUTF(lac);
        out.writeUTF(longitude);
        out.writeUTF(latitude);
        out.writeUTF(cell_province);
        out.writeUTF(cell_city);
        out.writeUTF(cell_county);
        out.writeUTF(area_id);
        out.writeUTF(lrc_province);
        out.writeUTF(lrc_city);
    }
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return (OdTraceRecord)super.clone();
    }
//    public String getMsisdn() {
//        return msisdn;
//    }
//
//    public void setMsisdn(String msisdn) {
//        this.msisdn = msisdn;
//    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public String getLac() {
        return lac;
    }

    public void setLac(String lac) {
        this.lac = lac;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getCell_province() {
        return cell_province;
    }

    public void setCell_province(String cell_province) {
        this.cell_province = cell_province;
    }

    public String getCell_city() {
        return cell_city;
    }

    public void setCell_city(String cell_city) {
        this.cell_city = cell_city;
    }

    public String getCell_county() {
        return cell_county;
    }

    public void setCell_county(String cell_county) {
        this.cell_county = cell_county;
    }

    public String getArea_id() {
        return area_id;
    }

    public void setArea_id(String area_id) {
        this.area_id = area_id;
    }

    public String getLrc_province() {
        return lrc_province;
    }

    public void setLrc_province(String lrc_province) {
        this.lrc_province = lrc_province;
    }

    public String getLrc_city() {
        return lrc_city;
    }

    public void setLrc_city(String lrc_city) {
        this.lrc_city = lrc_city;
    }

    public String getDateDay() {
        return dateDay;
    }

    public void setDateDay(String dateDay) {
        this.dateDay = dateDay;
    }
}
