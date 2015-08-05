package com.boco.od.location;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by mars on 2015/7/30.
 */
public class FirstPartitioner extends Partitioner<UserTimePair, Text> {
    @Override
    public int getPartition(UserTimePair key, Text value, int numPartitions) {
        return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}