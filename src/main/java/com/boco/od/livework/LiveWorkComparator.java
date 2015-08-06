package com.boco.od.livework;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by tianweiqi on 2015/8/5
 */
public class LiveWorkComparator  implements RawComparator<LiveWorkPair> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2, s2, Integer.SIZE / 8);
    }

    @Override
    public int compare(LiveWorkPair o1, LiveWorkPair o2) {
        return  o1.getMsisdn().compareTo(o2.getMsisdn());
    }
}
