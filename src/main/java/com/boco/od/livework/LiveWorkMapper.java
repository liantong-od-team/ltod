package com.boco.od.livework;

import com.boco.od.common.*;
import java.io.FileWriter;
import java.io.IOException;
import com.boco.od.utils.DateUtils;
import com.boco.od.utils.DsFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Random;
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
public class LiveWorkMapper  extends Mapper<Object, Text, LiveWorkPair, LiveWorkRecord>{

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
   // 20150810,13721323533,工作,115.60033602821871,39.196659996472664
    private int date_day_INDEX=0;
    private int msisdn_INDEX=1;
    private int action_type_INDEX=2;
    private int longitude_INDEX=3;
    private int latitude_INDEX=4;
    //private int LAC_INDEX;
   // private int CELL_ID_INDEX;

    private int columnSize;
    private String delimiterIn;
    private Pattern pattern ;

  protected LiveWorkPair rskey = new LiveWorkPair();
   // protected Text rskey = new Text();
    protected LiveWorkRecord rsval = new LiveWorkRecord();

    private String date_day_VAL;

    private String action_type_VAL;
    private String longitude_VAL;
    private String latitude_VAL;


    private String msisdn;
    private long time;

    public LiveWorkMapper() {
    }

    @Override
    protected void setup(Context context) {
  /*      BasicConfigurator.configure();
        PropertyConfigurator.configure("/home/hadoop/hadoop-2.4.0/etc/hadoop/log4j.properties");

        DOMConfigurator.configure("");

        log4j.debug("log4j debug");
        log4j.info("log4j info");
        log4j.warn("log4j warn");
        log4j.error("log4j error");
        log4j.fatal("log4j fatal");*/

        //delimiterIn=\u0001;
        pattern = Pattern.compile(",");


    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException,InterruptedException {
        context.getCounter(COUNTER.MapperInput).increment(1);
   //   String val = value.toString();
     //   String[] vals =pattern.split(val,-1);
     //  String[] vals = val.split(",");
       // String words[] = val.toString().split(" |\t");
      //  int x[]=new int[12];
        // String[] vals = val.split(",");
//val:com.boco.od.livework.LiveWorkRecord@493b0ccd


     //    val="20150802,13810623623,xiuxi,116.297334,39.925338";

        //System.out.println(vals.length);
        //System.out.println(vals[0]);


      String vals[] =       value.toString().split(",");


        if (vals.length!=5){
            context.getCounter(COUNTER.ErrorColumnSize).increment(1);
          return;
        }




     //   log4j.fatal("date_day_INDEX fatal"+date_day_INDEX);
     date_day_VAL=vals[date_day_INDEX];
 msisdn=vals[msisdn_INDEX];


       action_type_VAL=vals[action_type_INDEX];
     longitude_VAL=vals[longitude_INDEX];
      latitude_VAL=vals[latitude_INDEX];

        if(longitude_VAL.length()<8){
            context.getCounter(COUNTER.Null).increment(1);
            return;
        }

        //log4j.fatal("latitude_VAL fatal"+latitude_VAL);

        time = DateUtils.convertString2Time(date_day_VAL);
        //时间非法
        if(time<0){
            context.getCounter(COUNTER.ErrorDate).increment(1);
           // return;
        }
      /*  Random random = new Random();
*//*        int incr=0;

        for(int i = 0; i < 100;i++) {

            incr=Math.abs(random.nextInt())%18;
            System.out.println(incr);
        }*//*


        Random random = new Random();
        int incr=0;

        for(int i = 0; i < 20;i++) {

            incr1=Math.abs(random.nextInt())%10000;

            Double b = (double) (incr/5000.0);
            System.out.println(b);
        }

        int tmpmdate_day_VAL=Integer.parseInt(date_day_VAL);
        tmpmdate_day_VAL=tmpmdate_day_VAL+Math.abs(random.nextInt())%18;


        date_day_VAL=tmpmdate_day_VAL+"";*/

        //号码为空
        if(msisdn==null||"".equals(msisdn.trim())||msisdn.length()>11){
            context.getCounter(COUNTER.Null).increment(1);
          return;
        }
      //  msisdn = msisdn.trim();
        //rskey.set(msisdn);
        msisdn = msisdn.trim();
       rskey.set(msisdn,Integer.parseInt(date_day_VAL));
  // rskey.set(msisdn,123);
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
