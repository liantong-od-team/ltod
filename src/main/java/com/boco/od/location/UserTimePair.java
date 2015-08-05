package com.boco.od.location;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mars on 2015/7/30.
 */
public class UserTimePair implements WritableComparable<UserTimePair> {

    String user;
    long time;

    public void set(String user, long time) {
        this.user = user;
        this.time = time;
    }

    public String getFirst() {
        return this.user;
    }

    public long getSecond() {
        return this.time;
    }

    @Override
    public int compareTo(UserTimePair o) {
        if (!user.equals( o.user)) {
            return user.compareTo(o.user);
        } else if (time != o.time) {
            return time < o.time ? -1 : 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.user);
        dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user = dataInput.readUTF();
        this.time = dataInput.readLong();
    }

    public int hashCode() {
        return this.user.hashCode() * 157 + (time + "").hashCode();
    }

    @Override
    public boolean equals(Object right) {
        if (right == null)
            return false;
        if (this == right)
            return true;
        if (right instanceof UserTimePair) {
            UserTimePair r = (UserTimePair) right;
            return r.user.equals(user) && r.time == time;
        } else {
            return false;
        }
    }
}
