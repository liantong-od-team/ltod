package com.boco.od.livework;

import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by tianweiqi on 2015/8/5.
 */
public class LiveWorkRecord implements Writable,Cloneable {

    private String dateDay;

    private String msisdn;
    private String action_type;
    private String longitude;
    private String latitude;


    @Override
    public void readFields(DataInput in) throws IOException {
        dateDay= in.readUTF();
       msisdn = in.readUTF();
        action_type= in.readUTF();
        longitude= in.readUTF();
        latitude=in.readUTF();


    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(dateDay);
     out.writeUTF(msisdn);
        out.writeUTF(action_type);
        out.writeUTF(longitude);
        out.writeUTF(latitude);

    }
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }




    public String getAction_type() {
        return action_type;
    }
    public void setAction_type(String action_type) {
        this.action_type = action_type;
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



    public String getDateDay() {
        return dateDay;
    }

    public void setDateDay(String dateDay) {
        this.dateDay = dateDay;
    }
}
