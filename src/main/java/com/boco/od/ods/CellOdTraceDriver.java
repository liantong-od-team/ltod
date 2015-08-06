package com.boco.od.ods;

import com.boco.od.common.Metadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
 * Created by ranhualin on 2015/7/30.
 */
public class CellOdTraceDriver extends Configured implements Tool {

    private Metadata cellMeta;
    private Metadata lrcMeta;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.printf(
                    "Usage: %s [generic options]<input dir> <output dir> <cell dir> <lrc dir>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        String inpath = args[0];
        String outpath = args[1];
        String cellpath = args[2];
        String numberpath=args[3];

        Configuration conf=getConf();
//       conf.set("hadoop.tmp.dir", "D:\\odtest\\tmp\\");
        if(args.length>5){
            initRelationConf(cellpath,numberpath,conf,args[4]);//加载缓存数据
        }else{
            initRelationConf(cellpath,numberpath,conf,null);//加载缓存数据
        }
        Job job = new Job(conf);

//        println(conf);
        job.setJarByClass(CellOdTraceDriver.class);

        job.setJobName("CellOdTrace");
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
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");
//        String in = "D:\\odtest\\in\\*\\";
//        String out = "D:\\odtest\\out65\\";
//        String cell="D:/odtest/dim_all_station_cellid_new/";
//        String lrc="D:/odtest/zb_d_bsdwal/";

//        String [] testargs = new String[]{in,out,cell,lrc};
        try {
            int ret = ToolRunner.run(new CellOdTraceDriver(), args);
            System.exit(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void initRelationConf(String cellpath,String numberpath, Configuration conf,String hdfs) throws URISyntaxException, IOException {
//        DistributedCache.createSymlink(conf);
        disCache(cellpath, conf,hdfs);//为该job添加缓存文件
        disCache(numberpath, conf,hdfs);//为该job添加缓存文件
        URI[] distributePaths = DistributedCache.getCacheFiles(conf);
        System.out.println("#distributePaths#" + distributePaths);
        String info = null;
        for (URI  uri: distributePaths) {
            System.out.println("#URI#" + uri.getPath());
        }
        Path[] dpaths = DistributedCache.getLocalCacheFiles(conf);
        System.out.println("#Local#" + dpaths);
        if(dpaths!=null){
            System.out.println("#Local#" + dpaths.length);
            for (Path  p: dpaths) {
                System.out.println("#Local#" + p.toString());
            }
        }

    }
    public static void disCache(String dimDir, Configuration conf,String hdfs) throws IOException {
        FileSystem fs = null;
        if(hdfs!=null){
            fs=  FileSystem.get(URI.create(hdfs), conf);
        }else{
            fs=  FileSystem.get(conf);
        }

        FileStatus[] fileDir = fs.listStatus(new Path(dimDir));
        for (FileStatus file : fileDir) {
            if(file.isDirectory()){
                disCache(file.getPath().toString(),conf,hdfs);
            }else {
                DistributedCache.addCacheFile(URI.create(file.getPath().toString()), conf);
            }
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

