package com.boco.od.ods;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class OdTraceComparator  implements RawComparator<OdTracePair> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2, s2, Integer.SIZE / 8);
    }

    @Override
    public int compare(OdTracePair o1, OdTracePair o2) {
        return  o1.getMsisdn().compareTo(o2.getMsisdn());
    }
}
