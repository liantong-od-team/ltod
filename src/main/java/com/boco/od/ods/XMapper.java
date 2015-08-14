package com.boco.od.ods;

import com.boco.od.common.COUNTER;
import com.boco.od.utils.DsFactory;
import com.boco.od.utils.geo.GisTool;
import com.google.common.base.Joiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/8/13.
 */
public class XMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static Pattern pattern = Pattern.compile(",");
    private final int COLSIZE = 22;
    private static String charset = "utf-8";
    private Text tv = new Text();
    private String join_type = "";

    private int start_country_idx = 15, end_country_idx = 18, start_lng_idx = 9, start_lat_idx = 10, end_lng_idx = 11, end_lat_idx = 12;

    @Override
    protected void setup(Context ctx) {
        if (ctx.getConfiguration().get("charset") != null && !"".equals(ctx.getConfiguration().get("charset"))) {
            charset = ctx.getConfiguration().get("charset");
        }
        System.out.println("charset = " + charset);

        if (ctx.getConfiguration().get("join_type") != null && !"".equals(ctx.getConfiguration().get("join_type"))) {
            join_type = ctx.getConfiguration().get("join_type");
        }


        try {
            GisTool.getInstance().load(GisTool.COUNTRY_FILE_PATH);
            ctx.getCounter("LOADDATA", "GISDATA_SUCC").increment(1);
            System.out.println("GISDATA_SUCC");
        } catch (Exception ex) {
            ctx.getCounter("LOADDATA", "GISDATA_ERROR").increment(1);
            System.out.println("GISDATA_ERROR");
        }


    }

    @Override
    public void map(LongWritable offset, Text value, Context ctx)
            throws IOException, InterruptedException {
        ctx.getCounter(COUNTER.MapperInput).increment(1);
        String[] cols = pattern.split(value.toString(), -1);
//        System.out.println("cols.length=" + cols.length);
//        System.out.println("COLSIZE=" + COLSIZE);
        if (COLSIZE != cols.length) {
            //记录非法数据
            ctx.getCounter(COUNTER.Illegal).increment(1);
            return;
        }


        if ("oracle".equalsIgnoreCase(join_type)) {
            cols[start_country_idx] = DsFactory.getinstance().queryCountry(cols[start_lng_idx], cols[start_lat_idx]);
            cols[end_country_idx] = DsFactory.getinstance().queryCountry(cols[end_lng_idx], cols[end_lat_idx]);
            ctx.getCounter("LOADDATA", "GET_ORACLE_GIS").increment(1);
            System.out.println("GET_ORACLE_GIS");
        } else {
            cols[start_country_idx] = GisTool.getInstance().getCountry(cols[start_lng_idx], cols[start_lat_idx]);
            cols[end_country_idx] = GisTool.getInstance().getCountry(cols[end_lng_idx], cols[end_lat_idx]);
            ctx.getCounter("LOADDATA", "GET_LOCAL_GIS").increment(1);
            System.out.println("GET_LOCAL_GIS");
        }


        tv.set(Joiner.on(",").join(cols));
        ctx.write(NullWritable.get(), tv);
        ctx.getCounter(COUNTER.MapperOutput).increment(1);
    }

}
