package com.boco.od.location;

import com.boco.od.common.COUNTER;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.boco.od.common.Util;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/7/30.
 */
public class LocationOdMapper extends Mapper<LongWritable, Text, UserTimePair, Text> {
    private static Pattern pattern = Pattern.compile(",");
    private final int COLSIZE = 22;
    private static String charset = "utf-8";

    String msisdn;
    String start_date;

    private final UserTimePair utPair = new UserTimePair();
    private final Text tv = new Text();

    protected void setup(Context ctx) throws InterruptedException,
            IOException {
        if (ctx.getConfiguration().get("charset") != null && !"".equals(ctx.getConfiguration().get("charset"))) {
            charset = ctx.getConfiguration().get("charset");
        }
        System.out.println("charset = "+charset);
    }

    protected void cleanup(Context ctx) throws IOException,
            InterruptedException {
    }


    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        ctx.getCounter(COUNTER.MapperInput).increment(1);
        String[] cols = pattern.split(value.toString());
        if (COLSIZE != cols.length) {
            //记录非法数据
            ctx.getCounter(COUNTER.Illegal).increment(1);
            return;
        }

        start_date = cols[1];
        msisdn = cols[3];

        utPair.set(msisdn, Util.ParseDatebysec(start_date));
        tv.set(new String(value.getBytes(), 0, value.getLength(), charset));
        ctx.write(utPair, tv);
        ctx.getCounter(COUNTER.MapperOutput).increment(1);



        /*
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int left = 0;
        int right = 0;
        if (tokenizer.hasMoreTokens())
        {
            left = Integer.parseInt(tokenizer.nextToken());
            if (tokenizer.hasMoreTokens())
                right = Integer.parseInt(tokenizer.nextToken());
            intkey.set(left, right);
            intvalue.set(right);
            ctx.write(intkey, intvalue);
        }
        */
    }
}
