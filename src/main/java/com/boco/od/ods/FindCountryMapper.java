package com.boco.od.ods;

import com.boco.od.common.COUNTER;
import com.boco.od.common.FindCountryDimReader;
import com.google.common.base.Joiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/8/13.
 */
public class FindCountryMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static Pattern pattern = Pattern.compile(",");
    private final int COLSIZE = 22;
    private static String charset = "utf-8";
    private Text tv = new Text();
    private Map<String, String[]> cellMap;
    String[] dim;
    String start_lac_ci, end_lac_ci;

    private int start_country_idx = 15, end_country_idx = 18, start_lac_idx = 6, start_ci_idx = 5, end_lac_idx = 8, end_ci_idx = 7;


    @Override
    protected void setup(Context ctx) {
        FindCountryDimReader reader = new FindCountryDimReader(ctx.getConfiguration());
        cellMap = reader.getCellMap();
        ctx.getCounter("DIM_CHECK","DIM_COLUMN_SIZE").increment(reader.getCell_get_dim_cell_col_size());
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
        start_lac_ci = cols[start_lac_idx] + "-" + cols[start_ci_idx];
        end_lac_ci = cols[end_lac_idx] + "-" + cols[end_ci_idx];
        if (cellMap.containsKey(start_lac_ci)) {
            dim = cellMap.get(start_lac_ci);
            cols[start_country_idx] = dim[4];
        }
        if (cellMap.containsKey(end_lac_ci)) {
            dim = cellMap.get(end_lac_ci);
            cols[end_country_idx] = dim[4];
        }

        tv.set(Joiner.on(",").join(cols));
        ctx.write(NullWritable.get(), tv);
        ctx.getCounter(COUNTER.MapperOutput).increment(1);
    }

}
