package com.boco.od.livework;

import com.boco.od.common.COUNTER;
import com.boco.od.common.Constants;
import com.boco.od.common.Metadata;
import  com.boco.od.utils.DsFactory;
import com.boco.od.utils.DateUtils;
import com.boco.od.utils.geo.GisTool;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Random;

/**
 * Created by tianweiqi on 2015/8/5
 */
public class LiveWorkReducer extends Reducer<LiveWorkPair, LiveWorkRecord, NullWritable, Text> {

  //  private static Metadata meta = new Metadata(Constants.STAGE_PROP_PATH);
  //  private String delimiterOut = meta.getValue("delimiterOut");
    private String delimiterOut =",";
    private Text rsval = new Text();
    private String lastMsisdn = null;
    private int linenum = 0;
    private String dayNum;
    private String dbflag;

    private  int liveArrayNum=0;
    private  int workArrayNum=0;
    private String lastRecordDay = null;
    private LiveWorkRecord lastRecord = null;

    private String firstDay = null;
    LiveWorkRecord liveArray[] = new LiveWorkRecord[1000];
    LiveWorkRecord workArray[] = new LiveWorkRecord[1000];




    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
         dayNum = conf.get("dayNum");
        dbflag = conf.get("dbflag");
    }
    @Override
    public void reduce(LiveWorkPair key, Iterable<LiveWorkRecord> values, Context context)
            throws IOException, InterruptedException {
        for (LiveWorkRecord val : values) {
            linenum++;
            String msisdn = key.getMsisdn();
           // context.getCounter(COUNTER.ReducerOutput).increment(1); //
          // rsval.set("reduceval:" + val );
          // context.write(NullWritable.get(), rsval);

            if ((lastMsisdn == null) && (lastRecord == null)) {//第一条数据
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;

             //  rsval.set("lastRecordDay:" + lastRecordDay );
               //  context.write(NullWritable.get(), rsval);
              //  rsval.set("lastMsisdn:" + lastMsisdn );
              //  context.write(NullWritable.get(), rsval);
            } else if (!msisdn.equals(lastMsisdn)) {//用户号码已经变化，下一个用户的第一条数据
               // if (!checkLastSecond(lastRecord.getDateDay(), lastRecord.getTime())) {//判断上一条数据是否是235959，如不是
                //    writeRecord(context, getOutLine(lastMsisdn, lastRecord, getDayLastRecord(lastRecord)));
               //}


            //    rsval.set("liveArrayNum:" + liveArrayNum );
              //   context.write(NullWritable.get(), rsval);
               //  rsval.set("workArrayNum:" + workArrayNum );
             //   context.write(NullWritable.get(), rsval);
               reduceRecord(context, lastMsisdn ,"LIVE",liveArray,liveArrayNum);
                reduceRecord(context, lastMsisdn ,"WORK",workArray,workArrayNum);
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
              //  LiveWorkRecord[] intArray;
                linenum=0;
               // liveArray=null;
              //  workArray=null;
                liveArrayNum=0;
                workArrayNum=0;
            } else {//某号码中间数据，
                //有没有跨天
               // if(workArrayNum>50||liveArrayNum>50)return;
                if(val.getAction_type().equalsIgnoreCase("work"))
                {
                 // rsval.set("DateDay:" + val.getDateDay() );
                  //  context.write(NullWritable.get(), rsval);
                  //  rsval.set("Msisdn:" + val.getMsisdn() );
                 //   context.write(NullWritable.get(), rsval);
                    workArray[workArrayNum]= new LiveWorkRecord();
                    workArray[workArrayNum].setAction_type(val.getAction_type());
                    workArray[workArrayNum].setDateDay(val.getDateDay());
                    workArray[workArrayNum].setLatitude(val.getLatitude());
                    workArray[workArrayNum].setLongitude(val.getLongitude());
                    workArray[workArrayNum].setMsisdn(val.getMsisdn());
                     //cloneRecord(val);
                    workArrayNum++;
                }

                else
                {
                    liveArray[liveArrayNum]= new LiveWorkRecord();
                    liveArray[liveArrayNum].setAction_type(val.getAction_type());
                    liveArray[liveArrayNum].setDateDay(val.getDateDay());
                    liveArray[liveArrayNum].setLatitude(val.getLatitude());
                    liveArray[liveArrayNum].setLongitude(val.getLongitude());
                    liveArray[liveArrayNum].setMsisdn(val.getMsisdn());
                    //cloneRecord(val);
                    liveArrayNum++;
                }
              //  lastRecordDay = val.getDateDay();
            //    lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
              //  lastMsisdn = msisdn;

            }

        }

    }
    private LiveWorkRecord getDayFirstRecord(LiveWorkRecord record) {
        LiveWorkRecord newRecord = cloneRecord(record);
      //  newRecord.setDateDay(DateUtils.convertString2Time(record.getDateDay() + "000000"));
        newRecord.setDateDay("000000");
        return newRecord;
    }


    private void reduceRecord(Context context,String msisdn ,String action_type,LiveWorkRecord[] record,int arrynum)  throws IOException, InterruptedException {



        int recordlenth=arrynum;

        int cycle=Integer.parseInt(dayNum);

        int dbuse=Integer.parseInt(dbflag);


        int daynum=0;
        int duration=cycle;
        int currentday;
        try {
            GisTool.getInstance().load(GisTool.COUNTRY_FILE_PATH);
           // ctx.getCounter("LOAD_FILE", "GISDATA_SUCC").increment(1);
        } catch (Exception ex) {
           // ctx.getCounter("LOAD_FILE", "GISDATA_ERROR").increment(1);
        }
        if(action_type.equals("WORK"))
        {
            for(int i=0;i<recordlenth;i++)
            {
                currentday=Integer.parseInt(workArray[i].getDateDay());
                //  if((recordlenth-i)<15)break;
                // for(int j=i;j<Math.min(i+15,recordlenth);j++)
                double longitude=0.0;
                double latitude=0.0;
                for(int j=i;j<recordlenth;j++)
                {

    /*                rsval.set(String.valueOf(currentday));
                    context.write(NullWritable.get(), rsval);
                    rsval.set(workArray[j].getDateDay());
                    context.write(NullWritable.get(), rsval);*/

                    if((Integer.parseInt(workArray[j].getDateDay())-currentday)>=(cycle-1))
                    {


                            daynum++;
                            longitude+=+Double.parseDouble(workArray[j].getLongitude());
                            latitude+=+Double.parseDouble(workArray[j].getLatitude());

                        longitude=longitude/((double) daynum);
                        //   double theMean = (((double) length) / ((double) count));
                        latitude=latitude/((double) daynum);



                       /*rsval.set(String.valueOf(currentday));
                       context.write(NullWritable.get(), rsval);
                       rsval.set(workArray[j].getDateDay());
                       context.write(NullWritable.get(), rsval);*/
                        StringBuffer sb = new StringBuffer(String.valueOf(currentday)).append(delimiterOut);
                        //     sb.append(DateUtils.convertTime2FullString(last.getTime())).append(delimiterOut);
                        //    sb.append(DateUtils.convertTime2FullString(curr.getTime())).append(delimiterOut);


                        sb.append(workArray[j].getDateDay()).append(delimiterOut);
                        sb.append(String.valueOf(duration)).append(delimiterOut);
                        sb.append(msisdn).append(delimiterOut);
                        sb.append(action_type).append(delimiterOut);

                        int strlen=    String.valueOf(longitude).length();
                        sb.append(String.valueOf(longitude).substring(0,Math.min(strlen,11))).append(delimiterOut);
                    int    strlen1= String.valueOf(latitude).length();
                        sb.append(String.valueOf(latitude).substring(0,Math.min(strlen1,10))).append(delimiterOut);
                        String prov;
                        if(dbuse==1)
                        {
                            prov = GisTool.getInstance().getProvince(String.valueOf(longitude),String.valueOf(latitude));
                        }

                else    if(dbuse==2)
                    {



                         prov=DsFactory.getinstance().queryProvince(String.valueOf(longitude),String.valueOf(latitude));

                    }
                        else
                    {
                         prov= String.valueOf(longitude).substring(0,Math.min(strlen,3))+"-"+String.valueOf(latitude).substring(0,Math.min(strlen1,2));//DsFactory.getinstance().queryProvince(String.valueOf(longitude),String.valueOf(latitude));

                    }

                        sb.append(prov).append(delimiterOut);
                        String county;


                        if(dbuse==1)
                        {
                            county = GisTool.getInstance().getCountry(String.valueOf(longitude),String.valueOf(latitude));
                        }
                   else     if(dbuse==2)
                        {
                            // sb.append(latitude).append(delimiterOut);
                            county = DsFactory.getinstance().queryCountry(String.valueOf(longitude),String.valueOf(latitude));

                        }
               else
                      county = String.valueOf(longitude).substring(0,Math.min(strlen,5))+"-"+String.valueOf(latitude).substring(0,Math.min(strlen1,4));//DsFactory.getinstance().queryCountry(String.valueOf(longitude),String.valueOf(latitude));

                        sb.append(county);
                        //  sb.append(msisdn).append(delimiterOut);

                     rsval.set(sb.toString());
                      context.write(NullWritable.get(), rsval);

                        context.getCounter(COUNTER.ReducerOutput).increment(1); //
                        break;
                    }
                    else
                    {

                        longitude=0.0;
                        latitude=0.0;
                        daynum=0;
                        for(int k=i;k<j;k++)
                        {
                            daynum++;
                            longitude+=+Double.parseDouble(workArray[k].getLongitude());
                            latitude+=+Double.parseDouble(workArray[k].getLatitude());
                        }
                      //  longitude=longitude/((double) daynum);
                        //   double theMean = (((double) length) / ((double) count));
                       // latitude=latitude/((double) daynum);

                    }

                }
            }
        }
        if(action_type.equals("LIVE"))
        {
            for(int i=0;i<recordlenth;i++)
            {
                currentday=Integer.parseInt(liveArray[i].getDateDay());
                //  if((recordlenth-i)<15)break;
                // for(int j=i;j<Math.min(i+15,recordlenth);j++)
                for(int j=i;j<recordlenth;j++)
                {
                    double longitude=0.0;
                    double latitude=0.0;

                    if((Integer.parseInt(liveArray[j].getDateDay())-currentday)>=(cycle-1))
                    {
                        daynum++;
                        longitude+=+Double.parseDouble(liveArray[j].getLongitude());
                        latitude+=+Double.parseDouble(liveArray[j].getLatitude());

                        longitude=longitude/((double) daynum);
                        //   double theMean = (((double) length) / ((double) count));
                        latitude=latitude/((double) daynum);
                        StringBuffer sb = new StringBuffer(String.valueOf(currentday)).append(delimiterOut);
                        //     sb.append(DateUtils.convertTime2FullString(last.getTime())).append(delimiterOut);
                        //    sb.append(DateUtils.convertTime2FullString(curr.getTime())).append(delimiterOut);


                        sb.append(liveArray[j].getDateDay()).append(delimiterOut);
                        sb.append(String.valueOf(duration)).append(delimiterOut);
                        sb.append(msisdn).append(delimiterOut);
                        sb.append(action_type).append(delimiterOut);
                        int strlen=    String.valueOf(longitude).length();
                        sb.append(String.valueOf(longitude).substring(0,Math.min(strlen,11))).append(delimiterOut);
                        int   strlen1= String.valueOf(latitude).length();
                        sb.append(String.valueOf(latitude).substring(0,Math.min(strlen1,10))).append(delimiterOut);
                          String prov;
                        if(dbuse==1)
                        {
                            prov = GisTool.getInstance().getProvince(String.valueOf(longitude), String.valueOf(latitude));
                        }

                        else    if(dbuse==2)
                        {

                            prov=DsFactory.getinstance().queryProvince(String.valueOf(longitude),String.valueOf(latitude));
                        }

                    else
                            prov= String.valueOf(longitude).substring(0,Math.min(strlen,3))+"-"+String.valueOf(latitude).substring(0,Math.min(strlen1,2));;//DsFactory.getinstance().queryProvince(String.valueOf(longitude),String.valueOf(latitude));
                        sb.append(prov).append(delimiterOut);
                        // sb.append(latitude).append(delimiterOut);
                        String county;
                        if(dbuse==1)
                        {
                            county = GisTool.getInstance().getCountry(String.valueOf(longitude), String.valueOf(latitude));
                        }
                        else                  if(dbuse==2)

                        {
                          county = DsFactory.getinstance().queryCountry(String.valueOf(longitude),String.valueOf(latitude));

                        }
            else
                            county =  String.valueOf(longitude).substring(0,Math.min(strlen,5))+"-"+String.valueOf(latitude).substring(0,Math.min(strlen1,4));//DsFactory.getinstance().queryCountry(String.valueOf(longitude),String.valueOf(latitude));

                        sb.append(county);
                        //  sb.append(msisdn).append(delimiterOut);

                        rsval.set(sb.toString());
                        try {
                            try {
                                context.write(NullWritable.get(), rsval);
                            }catch (InterruptedException  e)
                            {}
                        }catch (IOException  e)
                        {}


                        context.getCounter(COUNTER.ReducerOutput).increment(1); //

                        break;

                    }
                    else
                    {

                        longitude=0.0;
                        latitude=0.0;
                        daynum=0;
                        for(int k=i;k<j;k++)
                        {
                            daynum++;
                            longitude+=+Double.parseDouble(liveArray[k].getLongitude());
                            latitude+=+Double.parseDouble(liveArray[k].getLatitude());
                        }
                     //   longitude=longitude/((double) daynum);
                        //   double theMean = (((double) length) / ((double) count));
                       // latitude=latitude/((double) daynum);

                    }

                }
            }
        }
    //  rsval.set("reduceRecord msisdn:" + msisdn + "val.recordlenth:"+recordlenth);
      // context.write(NullWritable.get(), rsval);



      //  return newRecord;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    }


    private void writeRecord(Context context, String line) throws IOException, InterruptedException {
        rsval.set(line);
        context.write(NullWritable.get(), rsval);
        context.getCounter(COUNTER.ReducerOutput).increment(1); //
    }



    //克隆OdTraceRecord对象
    private LiveWorkRecord cloneRecord(LiveWorkRecord in) {
        try {
            return (LiveWorkRecord) in.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            return null;
        }
    }

    //小区发生变化，合并记录
    private String getOutLine(String msisdn, LiveWorkRecord last, LiveWorkRecord curr) {
        StringBuffer sb = new StringBuffer(last.getDateDay()).append(delimiterOut);
   //     sb.append(DateUtils.convertTime2FullString(last.getTime())).append(delimiterOut);
    //    sb.append(DateUtils.convertTime2FullString(curr.getTime())).append(delimiterOut);
        sb.append(msisdn).append(delimiterOut);

        sb.append(last.getLongitude()).append(delimiterOut);
        sb.append(last.getLatitude()).append(delimiterOut);

        sb.append(curr.getLongitude()).append(delimiterOut);
        sb.append(curr.getLatitude()).append(delimiterOut);


        return sb.toString();
    }
}

