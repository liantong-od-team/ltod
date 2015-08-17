package com.boco.od.dim;

import com.boco.od.common.COUNTER;
import com.boco.od.common.Constants;
import com.boco.od.common.Metadata;
import com.boco.od.utils.DsFactory;
import com.boco.od.utils.geo.GisTool;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/8/13.
 */
public class DimMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static Pattern pattern ;
//    private static String charset = "utf-8";
    private Text tv = new Text();
    private String join_type = "";

    private Configuration conf;
    private int CELL_INDEX_LAC;
    private int CELL_INDEX_CELL_ID;
    private int CELL_INDEX_LONGITUDE;
    private int CELL_INDEX_LATITUDE;
    private int CELL_INDEX_PROVINCE;
    private int CELL_INDEX_CITY;
    private String cell_fileName;
    private String cell_delimiterIn;
    private int cell_columnSize;
    private String country;
    private String province;
    @Override
    protected void setup(Context ctx) {
//        if (ctx.getConfiguration().get("charset") != null && !"".equals(ctx.getConfiguration().get("charset"))) {
//            charset = ctx.getConfiguration().get("charset");
//        }

//        System.out.println("charset = " + charset);

        this.conf = ctx.getConfiguration();
        Metadata cellMeta = new Metadata(Constants.CELL_PROP_PATH);
        cell_fileName = conf.get("cell_fileName", cellMeta.getValue("fileName"));
        join_type = conf.get("join_type","");
        //TODO
        cell_delimiterIn = /*",";*/cellMeta.getValue("delimiterIn");
        cell_columnSize = cellMeta.getIntValue("column.size");
        CELL_INDEX_LAC = cellMeta.getIntValue("LAC");
        CELL_INDEX_CELL_ID = cellMeta.getIntValue("CELL_ID");
        CELL_INDEX_LONGITUDE = cellMeta.getIntValue("LONGITUDE");
        CELL_INDEX_LATITUDE = cellMeta.getIntValue("LATITUDE");
        CELL_INDEX_PROVINCE = cellMeta.getIntValue("PROVINCE");
        CELL_INDEX_CITY = cellMeta.getIntValue("CITY");

        pattern= Pattern.compile(cell_delimiterIn);

        try {
            GisTool.getInstance().load(GisTool.COUNTRY_FILE_PATH);
            ctx.getCounter("LOAD_FILE", "GISDATA_SUCC").increment(1);
        } catch (Exception ex) {
            ctx.getCounter("LOAD_FILE", "GISDATA_ERROR").increment(1);
        }


    }

    @Override
    public void map(LongWritable offset, Text value, Context ctx)
            throws IOException, InterruptedException {
        ctx.getCounter(COUNTER.MapperInput).increment(1);
        String[] cols = pattern.split(value.toString(), -1);
//        System.out.println("cols.length=" + cols.length);
//        System.out.println("cell_columnSize=" + cell_columnSize);
        if (cell_columnSize != cols.length) {
            //记录非法数据
            ctx.getCounter(COUNTER.Illegal).increment(1);
            return;
        }

        province = cols[CELL_INDEX_PROVINCE];
        if (province != null && (province.trim().equals("011") || province.trim().equals("013") || province.trim().equals("018"))) {
            if ("oracle".equalsIgnoreCase(join_type)) {
                country = DsFactory.getinstance().queryCountry(cols[CELL_INDEX_LONGITUDE], cols[CELL_INDEX_LATITUDE]);
                ctx.getCounter("LOAD_DATA", "GET_ORACLE_GIS").increment(1);
            } else {
                country = GisTool.getInstance().getCountry(cols[CELL_INDEX_LONGITUDE], cols[CELL_INDEX_LATITUDE]);
                ctx.getCounter("LOAD_DATA", "GET_LOCAL_GIS").increment(1);
            }
            ctx.getCounter("DIM_DATA","BELONG_TARGET_PROV").increment(1);
        }else {
            ctx.getCounter("DIM_DATA","NOT_BELONG_TARGET_PROV").increment(1);
        }
        tv.set(value.toString().concat(cell_delimiterIn+country.trim()));
        ctx.write(NullWritable.get(), tv);
        ctx.getCounter(COUNTER.MapperOutput).increment(1);
    }

}
