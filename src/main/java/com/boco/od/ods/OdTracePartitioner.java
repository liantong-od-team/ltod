package com.boco.od.ods;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class OdTracePartitioner extends Partitioner<OdTracePair, OdTraceRecord>
{
    @Override
    public int getPartition(OdTracePair key, OdTraceRecord value,int numPartitions)
    {
        return (key.getMsisdn().hashCode()&Integer.MAX_VALUE)%numPartitions;
    }
}
