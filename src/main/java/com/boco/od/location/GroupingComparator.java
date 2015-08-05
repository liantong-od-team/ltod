package com.boco.od.location;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by mars on 2015/7/30.
 */
public class GroupingComparator extends WritableComparator {
    protected GroupingComparator() {
        super(UserTimePair.class, true);
    }

    @Override
    //Compare two WritableComparables.
    public int compare(WritableComparable w1, WritableComparable w2) {
        UserTimePair ip1 = (UserTimePair) w1;
        UserTimePair ip2 = (UserTimePair) w2;
        String l = ip1.getFirst();
        String r = ip2.getFirst();
        return l.compareTo(r);
    }
}