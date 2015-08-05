package com.boco.od.ods;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by ranhualin on 2015/7/30.
 */
public class CellOdTraceDriver  extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf(
                    "Usage: %s [generic options]<input dir> <output dir>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        String inpath = args[0];
        String outpath = args[1];

        Job job = new Job(getConf());
        job.setJarByClass(CellOdTraceDriver.class);

        job.setJobName("CellOdTraceDriver");
        job.setMapperClass(CellOdTraceMapper.class);
        job.setReducerClass(CellOdTraceReducer.class);


        job.setPartitionerClass(OdTracePartitioner.class); //设置自定义分区策略
        job.setGroupingComparatorClass(OdTraceComparator.class); //设置自定义分组策略
        job.setMapOutputKeyClass(OdTracePair.class);
        job.setMapOutputValueClass(OdTraceRecord.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inpath));
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

//    hadoop jar od-assembly-1.0.jar com.boco.od.ods.CellOdTraceDriver ranhl/odin ranhl/odout8
    public static void main(String[] args) {
        try {
            int ret = ToolRunner.run(new CellOdTraceDriver(), args);
            System.exit(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

