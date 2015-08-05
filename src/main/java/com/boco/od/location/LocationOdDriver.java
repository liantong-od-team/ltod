package com.boco.od.location;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by mars on 2015/7/30.
 */
public class LocationOdDriver extends Configured implements Tool {
    String prefix = "hdfs://10.12.2.80:8020/user/boco/";
    String inpath = /*prefix +*/ "fy/odin3";
    String outpath = /*prefix +*/ "fy/odtest3";

    @Override
    public int run(String[] args) throws Exception {

        System.out.println("======= command line params =========");
        for(int i=0;i<args.length;i++){
            System.out.printf("arg[%d] : %s \n", i,args[i]);
        }
        System.out.println();
        String inpath = args[0];
        String outpath = args[1];
        String level = args[2];
        String charset="";

        if(args.length>3){
            charset =args[3];
        }

        Job job = new Job(getConf());
        job.getConfiguration().set("level",level);
        job.getConfiguration().set("charset",charset);

        job.setJarByClass(LocationOdDriver.class);
        job.setJobName("LocationOdDriver");
        job.setMapperClass(LocationOdMapper.class);

        job.setReducerClass(LocationOdReducer3.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(UserTimePair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths((JobConf) job.getConfiguration(), inpath);
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

// hadoop jar od-assembly-1.0.jar com.boco.od.location.LocationOdDriver fy/odin fy/odout region gbk
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Tools\\runtime\\hadoop_home\\hadoop-2.5.2");
        try {
            if (args.length < 3) {
                System.out.printf(
                        "Usage: %s [generic options]<input dir> <output dir> <region level> <charset>\n",
                        LocationOdDriver.class.getSimpleName());
                ToolRunner.printGenericCommandUsage(System.out);
                return ;
            }
            int exitCode = ToolRunner.run(new LocationOdDriver(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
