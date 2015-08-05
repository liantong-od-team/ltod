package com.boco.od.ods;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ranhualin on 2015/7/30.
 */
public  class OdTracePair implements WritableComparable<OdTracePair> {

    private String msisdn;
    private long time;

    public void set(String msisdn, long time) {
        this.msisdn = msisdn;
        this.time = time;
    }
    public String getMsisdn() {
        return msisdn;
    }
    public long getTime() {
        return time;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        msisdn = in.readUTF();
        time = in.readLong();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(msisdn);
        out.writeLong(time);
    }
    @Override
    //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
    public int hashCode()
    {
        return msisdn.hashCode() * 157 + (time+"").hashCode();
    }
    @Override
    public boolean equals(Object right) {
        if (right instanceof OdTracePair) {
            OdTracePair r = (OdTracePair) right;
            return r.getMsisdn().equals(msisdn) && r.getTime() == time;
        } else {
            return false;
        }
    }
    //key排序时，调用的就是这个compareTo方法
    @Override
    public int compareTo(OdTracePair o) {
        if (!msisdn.equals(o.getMsisdn())) {
            return  msisdn.compareTo(o.getMsisdn());
        } else if (time != o.getTime()) {
            return time - o.time>0?1:-1;
        } else {
            return 0;
        }
    }
}
