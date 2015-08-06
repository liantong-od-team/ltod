package com.boco.od.livework;


import com.boco.od.common.Metadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by tianweiqi on 2015/8/5
 */
public class LiveWorkDriver  extends Configured implements Tool {

    private Metadata cellMeta;
    private Metadata lrcMeta;


    public int dayNum=0;
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.printf(
                    "Usage: %s [generic options]<input dir> <output dir> <cell dir> <lrc dir>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
          //  return -1;
        }
        System.out.printf("args.length="+args.length);
        for(int i=0;i<args.length;i++)
        {
            System.out.printf("I="+i+"args:"+args[i]);

        }
        String inpath = args[0];
        String outpath = args[1];
        dayNum = Integer.parseInt(args[2]);
        //String cellpath = args[2];
       // String numberpath=args[3];

        Configuration conf=getConf();
//       conf.set("hadoop.tmp.dir", "D:\\odtest\\tmp\\");


        Job job = new Job(conf);
        println(conf);
        job.setJarByClass(LiveWorkDriver.class);

        job.setJobName("LiveWork");
        job.setMapperClass(LiveWorkMapper.class);
        job.setReducerClass(LiveWorkReducer.class);


        job.setPartitionerClass(LiveWorkPartitioner.class); //设置自定义分区策略
        job.setGroupingComparatorClass(LiveWorkComparator.class); //设置自定义分组策略
        job.setMapOutputKeyClass(LiveWorkPair.class);
        job.setMapOutputValueClass(LiveWorkRecord.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inpath));
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

//    hadoop jar od-assembly-1.0.jar com.boco.od.ods.CellOdTraceDriver ranhl/odin ranhl/odout8
    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");
//        String in = "D:\\odtest\\in\\*\\";
//        String out = "D:\\odtest\\out90\\";
//        String cell="D:/odtest/dim_all_station_cellid_new/";
//        String lrc="D:/odtest/zb_d_bsdwal/";
//        String [] testargs = new String[]{in,out,cell,lrc};
        try {
            int ret = ToolRunner.run(new LiveWorkDriver(), args);
            System.exit(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void println(Configuration conf){
    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while(it.hasNext()){
        Map.Entry<String, String> s = it.next();
        System.out.println(s.getKey()+"="+s.getValue());
    }
}
}

