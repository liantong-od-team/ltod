package com.boco.od.livework;

import com.boco.od.common.*;
import java.io.FileWriter;
import java.io.IOException;
import com.boco.od.utils.DateUtils;
import com.boco.od.utils.DsFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
/**
 * Created by tianweiqi on 2015/8/5
 */
public class LiveWorkMapper  extends Mapper<LongWritable, Text, LiveWorkPair, LiveWorkRecord>{

    static Logger log4j = Logger.getLogger(LiveWorkMapper.class.getClass());

  /*  public LiveWorkMapper(){
        System.out.println("hello, I am HMain");

        printLog();
    }*/

    private void printLog(){
        BasicConfigurator.configure();
        PropertyConfigurator.configure("/home/hadoop/hadoop-2.4.0/etc/hadoop/log4j.properties");

        DOMConfigurator.configure("");

        log4j.debug("log4j debug");
        log4j.info("log4j info");
        log4j.warn("log4j warn");
        log4j.error("log4j error");
        log4j.fatal("log4j fatal");
    }

    private int date_day_INDEX;
    private int msisdn_INDEX;
    private int action_type_INDEX;
    private int longitude_INDEX;
    private int latitude_INDEX;
    //private int LAC_INDEX;
   // private int CELL_ID_INDEX;

    private int columnSize;
    private String delimiterIn;
    private Pattern pattern ;

  protected LiveWorkPair rskey = new LiveWorkPair();
   // protected Text rskey = new Text();
    protected LiveWorkRecord rsval = new LiveWorkRecord();

    private String date_day_VAL;
    private String msisdn_VAL;
    private String action_type_VAL;
    private String longitude_VAL;
    private String latitude_VAL;


    private String msisdn;
    private long time;

    public LiveWorkMapper() {
    }

    @Override
    protected void setup(Context context) {
        BasicConfigurator.configure();
        PropertyConfigurator.configure("/home/hadoop/hadoop-2.4.0/etc/hadoop/log4j.properties");

        DOMConfigurator.configure("");

        log4j.debug("log4j debug");
        log4j.info("log4j info");
        log4j.warn("log4j warn");
        log4j.error("log4j error");
        log4j.fatal("log4j fatal");

        //delimiterIn=\u0001;
        pattern = Pattern.compile(",");


    }

    @Override
    public void map(LongWritable offset, Text value, Context context)
            throws IOException {
        context.getCounter(COUNTER.MapperInput).increment(1);
        String val = value.toString();
        String[] vals =pattern.split(val,-1);
        if(vals.length!=columnSize){
            context.getCounter(COUNTER.ErrorColumnSize).increment(1);
            return;
        }
        log4j.fatal("date_day_INDEX fatal"+date_day_INDEX);
        date_day_VAL=vals[date_day_INDEX];
        msisdn_VAL=vals[msisdn_INDEX];
        action_type_VAL=vals[action_type_INDEX];
        longitude_VAL=vals[longitude_INDEX];
        latitude_VAL=vals[latitude_INDEX];
        log4j.fatal("latitude_VAL fatal"+latitude_VAL);

        time = DateUtils.convertString2Time(date_day_VAL);
        //时间非法
        if(time<0){
            context.getCounter(COUNTER.ErrorDate).increment(1);
            return;
        }

//         逻辑需确定？
//        "00：主叫呼出话单
//        01：被叫呼入话单
//        02：呼叫前转话单
//        03：呼转拆分的话单
//        10：短消息发送话单(MO)
//        11：短消息接收话单(MT)
//        12：短消息转发MO-F
//        13：短消息转发MT-F
//        18：国际漫游主叫短信
//        19：国际漫游被叫短信
//        20：国际漫游语音附加业务
//        EE：尾记录（后续系统可不用处理）
//        FF：未定义业务话单"

        //号码为空
        if(msisdn==null||"".equals(msisdn.trim())){
            context.getCounter(COUNTER.Null).increment(1);
            return;
        }
      //  msisdn = msisdn.trim();
        //rskey.set(msisdn);
        msisdn = msisdn.trim();
        rskey.set(msisdn,Integer.parseInt(date_day_VAL));

        rsval.setDateDay(date_day_VAL);
      rsval.setMsisdn(msisdn);
        rsval.setAction_type(action_type_VAL);
        rsval.setLongitude(longitude_VAL);
        rsval.setLatitude(latitude_VAL);

        context.getCounter("RELATIONS","CELL_SUCC").increment(1);

        System.out.print("rsval:"+rsval);
        System.out.print("rskey:"+rskey);

        String str="rsval:"+rsval;
        FileWriter writer;
        try {
            writer = new FileWriter("rsval.txt");
            writer.write(str);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            context.write(rskey,rsval);
            context.getCounter(COUNTER.MapperOutput).increment(1);
        } catch (InterruptedException e) {
            context.getCounter(COUNTER.Illegal).increment(1);
        }
    }
    protected void cleanup(Context context) throws IOException,
            InterruptedException {

    }
}
