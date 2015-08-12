package com.boco.od.livework;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by tianweiqi on 2015/8/5
 */
public  class LiveWorkPair implements WritableComparable<LiveWorkPair> {

    private String msisdn;
    private int day;

    public void set(String msisdn, int day) {
        this.msisdn = msisdn;
        this.day = day;
    }
    public String getMsisdn() {
        return msisdn;
    }
    public int getTime() {
        return day;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        msisdn = in.readUTF();
        day = in.readInt();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(msisdn);
        out.writeInt(day);
    }
    @Override
    //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
    public int hashCode()
    {
        return msisdn.hashCode() * 157 + (day+"").hashCode();
    }
    @Override
    public boolean equals(Object right) {
        if (right instanceof LiveWorkPair) {
            LiveWorkPair r = (LiveWorkPair) right;
            return r.getMsisdn().equals(msisdn) && r.getTime() == day;
        } else {
            return false;
        }
    }
    //key排序时，调用的就是这个compareTo方法
    @Override
    public int compareTo(LiveWorkPair o) {
        if (!msisdn.equals(o.getMsisdn())) {
            return  msisdn.compareTo(o.getMsisdn());
        } else if (day != o.getTime()) {
            return (day - o.day)>0?1:-1;
        } else {
            return 0;
        }
    }
}
