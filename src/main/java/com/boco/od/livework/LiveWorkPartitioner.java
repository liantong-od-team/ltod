package com.boco.od.livework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by tianweiqi on 2015/8/5
 */
public class LiveWorkPartitioner extends Partitioner<LiveWorkPair, LiveWorkRecord>
{
    @Override
    public int getPartition(LiveWorkPair key, LiveWorkRecord value,int numPartitions)
    {
        return (key.getMsisdn().hashCode()&Integer.MAX_VALUE)%numPartitions;
    }
}
