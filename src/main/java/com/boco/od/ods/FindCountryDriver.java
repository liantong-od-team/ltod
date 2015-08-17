package com.boco.od.ods;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mars on 2015/8/13.
 */
public class FindCountryDriver extends Configured implements Tool {
//    String prefix = "file:///e:/";//"hdfs://10.12.2.80:8020/user/boco/";
//    String inpath = prefix + "cache/ods1";
//    String outpath = prefix + "cache/ods1out";

    @Override
    public int run(String[] args) throws Exception {

        System.out.println("======= command line params =========");
        for (int i = 0; i < args.length; i++) {
            System.out.printf("arg[%d] : %s \n", i, args[i]);
        }
        System.out.println();
        String inpath = args[0];
        String outpath = args[1];
        String cellpath = args[2];
        String cell_linkName = args[3];

        Configuration conf = getConf();
        conf.set("cell_fileName", cell_linkName);

        //add cache path
        initRelationConf(cellpath,cell_linkName,conf);
        Job job = new Job(conf);

        job.setJarByClass(FindCountryDriver.class);
        job.setJobName("FindCountryDriver");
        job.setMapperClass(FindCountryMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths((JobConf) job.getConfiguration(), inpath);
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // hadoop jar od-assembly-1.0.jar com.boco.od.location.FindCountryDriver fy/odin fy/odout cell_link
    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "D:\\Tools\\runtime\\hadoop_home\\hadoop-2.5.2");
        try {
            if (args.length < 4) {
                System.out.printf(
                        "Usage: %s [generic options]<input dir> <output dir> <dim_cell_linkName>\n",
                        FindCountryDriver.class.getSimpleName());
                ToolRunner.printGenericCommandUsage(System.out);
                return;
            }
            int exitCode = ToolRunner.run(new FindCountryDriver(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void initRelationConf(String cellpath, String linkName, Configuration conf) throws URISyntaxException, IOException {
        int file_idx=0;
//        String linkName="dim_all_station_cellid_new";
        DistributedCache.createSymlink(conf);
        disCache(cellpath, file_idx,linkName,conf);//为该job添加缓存文件
        URI[] distributePaths = DistributedCache.getCacheFiles(conf);
        System.out.println("#distributePaths#" + distributePaths);
        /*String info = null;
        for (URI uri : distributePaths) {
            System.out.println("#URI#" + uri.getPath());
        }*/
        Path[] dpaths = DistributedCache.getLocalCacheFiles(conf);
        System.out.println("#Local#" + dpaths);
       /* if(dpaths!=null){
            System.out.println("#Local#" + dpaths.length);
            for (Path  p: dpaths) {
                System.out.println("#Local#" + p.toString());
            }
        }*/
    }

    public static void disCache(String dimDir, int idx,String linkname,Configuration conf) {
        FileSystem fs = null;
        String uri_with_link="";
        try {
            fs = FileSystem.get(conf);
            FileStatus[] fileDir = fs.listStatus(new Path(dimDir));
            for (FileStatus file : fileDir) {
                if (file.isDirectory()) {
                    disCache(file.getPath().toString(),idx,linkname, conf);
                } else {
                    if (file.getLen() > 0) {
                        uri_with_link=file.getPath().toString()+"#"+linkname+(++idx);
                        DistributedCache.addCacheFile(URI.create(uri_with_link), conf);
                    }
                }
            }
        } catch (IOException e) {
        }


    }
}