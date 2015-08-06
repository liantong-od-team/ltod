package com.boco.od.livework;

import com.boco.od.common.COUNTER;
import com.boco.od.common.Constants;
import com.boco.od.common.Metadata;
import com.boco.od.ods.OdTraceRecord;
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

    private static Metadata meta = new Metadata(Constants.STAGE_PROP_PATH);
    private String delimiterOut = meta.getValue("delimiterOut");
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

            if ((lastMsisdn == null) && (lastRecord == null)) {//第一条数据
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
            } else if (!msisdn.equals(lastMsisdn)) {//用户号码已经变化，下一个用户的第一条数据
               // if (!checkLastSecond(lastRecord.getDateDay(), lastRecord.getTime())) {//判断上一条数据是否是235959，如不是
                //    writeRecord(context, getOutLine(lastMsisdn, lastRecord, getDayLastRecord(lastRecord)));
               //}
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
            } else {//某号码中间数据，
                //有没有跨天


                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;

            }

        }

    }
    private LiveWorkRecord getDayFirstRecord(LiveWorkRecord record) {
        LiveWorkRecord newRecord = cloneRecord(record);
      //  newRecord.setDateDay(DateUtils.convertString2Time(record.getDateDay() + "000000"));
        newRecord.setDateDay("000000");
        return newRecord;
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

