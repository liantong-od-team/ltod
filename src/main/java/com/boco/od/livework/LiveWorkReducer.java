package com.boco.od.livework;

import com.boco.od.common.COUNTER;
import com.boco.od.common.Constants;
import com.boco.od.common.Metadata;
import  com.boco.od.utils.DsFactory;
import com.boco.od.utils.DateUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
    private String lastRecordDay = null;
    private LiveWorkRecord lastRecord = null;

    private String firstDay = null;


    @Override
    public void setup(Context context) throws IOException, InterruptedException {

    }
    @Override
    public void reduce(LiveWorkPair key, Iterable<LiveWorkRecord> values, Context context)
            throws IOException, InterruptedException {
        for (LiveWorkRecord val : values) {
            linenum++;
            String msisdn = key.getMsisdn();

            LiveWorkRecord liveArray[] = new LiveWorkRecord[100];
            LiveWorkRecord workArray[] = new LiveWorkRecord[100];
            if ((lastMsisdn == null) && (lastRecord == null)) {//第一条数据
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
            } else if (!msisdn.equals(lastMsisdn)) {//用户号码已经变化，下一个用户的第一条数据
               // if (!checkLastSecond(lastRecord.getDateDay(), lastRecord.getTime())) {//判断上一条数据是否是235959，如不是
                //    writeRecord(context, getOutLine(lastMsisdn, lastRecord, getDayLastRecord(lastRecord)));
               //}


                reduceRecord(context, lastMsisdn ,"LIVE",liveArray);
                reduceRecord(context, lastMsisdn ,"WORK",workArray);
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
              //  LiveWorkRecord[] intArray;
                linenum=0;
                liveArray=null;
                workArray=null;
            } else {//某号码中间数据，
                //有没有跨天
                if(val.getAction_type().equals("Live"))
                liveArray[linenum]=val;
                else
                workArray[linenum]=val;
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


    private void reduceRecord(Context context,String msisdn ,String action_type,LiveWorkRecord record[]) {

        int recordlenth=record.length;

        int k=5;
        int currentday;
        for(int i=0;i<recordlenth;i++)
        {
            currentday=Integer.parseInt(record[i].getDateDay());
          //  if((recordlenth-i)<15)break;
           // for(int j=i;j<Math.min(i+15,recordlenth);j++)
            for(int j=i;j<recordlenth;j++)
            {
                double longitude=0.0;
                double latitude=0.0;
                int daynum=0;
                if((Integer.parseInt(record[j].getDateDay())-currentday)>=(k-1))
                {

                    StringBuffer sb = new StringBuffer(String.valueOf(currentday)).append(delimiterOut);
                    //     sb.append(DateUtils.convertTime2FullString(last.getTime())).append(delimiterOut);
                    //    sb.append(DateUtils.convertTime2FullString(curr.getTime())).append(delimiterOut);


                    sb.append(record[j].getDateDay()).append(delimiterOut);
                    sb.append(k).append(delimiterOut);
                    sb.append(msisdn).append(delimiterOut);
                    sb.append(action_type).append(delimiterOut);
                    sb.append(longitude).append(delimiterOut);
                    sb.append(latitude).append(delimiterOut);

                    String prov=DsFactory.getinstance().queryProvince(String.valueOf(longitude),String.valueOf(latitude));
                    sb.append(prov).append(delimiterOut);
                   // sb.append(latitude).append(delimiterOut);
                    String county = DsFactory.getinstance().queryCountry(String.valueOf(longitude),String.valueOf(latitude));
                    sb.append(county).append(delimiterOut);
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

                }
                else
                {

                     longitude=0.0;
                     latitude=0.0;
                     daynum=0;
                    for(k=i;k<j;k++)
                    {
                        daynum++;
                        longitude+=+Double.parseDouble(record[k].getLongitude());
                        latitude+=+Double.parseDouble(record[k].getLatitude());
                    }
                    longitude=longitude/((double) daynum);
                 //   double theMean = (((double) length) / ((double) count));
                    latitude=latitude/((double) daynum);

                }

            }
        }

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

