package com.boco.od.ods;

import com.boco.od.common.COUNTER;
import com.boco.od.common.Constants;
import com.boco.od.common.Metadata;
import com.boco.od.utils.DateUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class CellOdTraceReducer extends Reducer<OdTracePair, OdTraceRecord, NullWritable, Text> {

    private static Metadata meta = new Metadata(Constants.STAGE_PROP_PATH);
    private String delimiterOut = meta.getValue("delimiterOut");
    private Text rsval = new Text();
    private String lastMsisdn = null;
    private String lastRecordDay = null;
    private OdTraceRecord lastRecord = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

    }
    @Override
    public void reduce(OdTracePair key, Iterable<OdTraceRecord> values, Context context)
            throws IOException, InterruptedException {
        for (OdTraceRecord val : values) {
            String msisdn = key.getMsisdn();
            if ((lastMsisdn == null) && (lastRecord == null)) {//第一条数据
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
            } else if (!msisdn.equals(lastMsisdn)) {//用户号码已经变化，下一个用户的第一条数据
                if (!checkLastSecond(lastRecord.getDateDay(), lastRecord.getTime())) {//判断上一条数据是否是235959，如不是
                    writeRecord(context, getOutLine(lastMsisdn, lastRecord, getDayLastRecord(lastRecord)));
                }
                lastRecordDay = val.getDateDay();
                lastRecord = getDayFirstRecord(val);//第一条数据必须是000000
                lastMsisdn = msisdn;
            } else {//某号码中间数据，需要判断是否跨天、跨小区
                //有没有跨天
                if (!val.getDateDay().equals(lastRecordDay)) {//跨了天
                    fillDayRecord(context, msisdn, lastRecord, val);
                    lastRecordDay = val.getDateDay();
                    lastRecord = cloneRecord(val);
                } else {//没有跨天
                    //小区是否有变化
                    if (!checkCellLac(lastRecord, val)) {//有变化
                        writeRecord(context, getOutLine(msisdn, lastRecord, val));
                        lastRecordDay = val.getDateDay();
                        lastRecord = cloneRecord(val);
                    }
                }
            }
        }
        //最后一条数据是否是该天的"235959"
        if (!checkLastSecond(lastRecord.getDateDay(), lastRecord.getTime())) {//不是
            writeRecord(context, getOutLine(lastMsisdn, lastRecord, getDayLastRecord(lastRecord)));
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    }

    //填充跨天数据
    private void fillDayRecord(Context context, String msisdn, OdTraceRecord last, OdTraceRecord curr)
            throws IOException, InterruptedException {
        String lastNext = DateUtils.getNextDay(last.getTime());
        writeRecord(context, getOutLine(msisdn, last, getDayLastRecord(last)));
        last = getNextDayFirstRecord(last);
        if (lastNext.equals(curr.getDateDay())) { //跨了一天
             return;
        } else {//跨了多天,递归
            fillDayRecord(context, msisdn, last, curr);
        }
    }

    private void writeRecord(Context context, String line) throws IOException, InterruptedException {
        rsval.set(line);
        context.write(NullWritable.get(), rsval);
        context.getCounter(COUNTER.ReducerOutput).increment(1); //
    }
    //判断小区和lac是否一样
    private boolean checkCellLac(OdTraceRecord last, OdTraceRecord curr) {
        return last.getCellId().equals(curr.getCellId()) && last.getLac().equals(curr.getLac());
    }

    //判断是否是该天的第00秒
    private boolean checkFirstSecond(String day, long time) {
        return DateUtils.convertString2Time(day + "000000") == time;
    }

    //判断是否是改天的最后一秒
    private boolean checkLastSecond(String day, long time) {
        return DateUtils.convertString2Time(day + "235959") == time;
    }
    //克隆OdTraceRecord对象
    private OdTraceRecord cloneRecord(OdTraceRecord in) {
        try {
            return (OdTraceRecord) in.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            return null;
        }
    }

    //每个号码的第一条记录不是当天的"00"秒时，需要补上开头
    private OdTraceRecord getDayFirstRecord(OdTraceRecord record) {
        OdTraceRecord newRecord = record;
        newRecord = cloneRecord(record);
        newRecord.setTime(DateUtils.convertString2Time(record.getDateDay() + "000000"));
        return newRecord;
    }

    //跨天时,结尾取23:59计算
    private OdTraceRecord getDayLastRecord(OdTraceRecord record) {
        OdTraceRecord newRecord = record;
        newRecord = cloneRecord(record);
        newRecord.setTime(DateUtils.convertString2Time(record.getDateDay() + "235959"));
        return newRecord;
    }

    //跨天时,第二天头一条开头取00:00,
    private OdTraceRecord getNextDayFirstRecord(OdTraceRecord record) {
        OdTraceRecord newRecord = record;
        String nextDay = DateUtils.getNextDay(record.getTime());
        newRecord = cloneRecord(record);
        newRecord.setDateDay(nextDay);
        newRecord.setTime(DateUtils.convertString2Time(nextDay + "000000"));
        return newRecord;
    }

    //小区发生变化，合并记录
    private String getOutLine(String msisdn, OdTraceRecord last, OdTraceRecord curr) {
        StringBuffer sb = new StringBuffer(last.getDateDay()).append(delimiterOut);
        sb.append(DateUtils.convertTime2FullString(last.getTime())).append(delimiterOut);
        sb.append(DateUtils.convertTime2FullString(curr.getTime())).append(delimiterOut);
        sb.append(msisdn).append(delimiterOut);
        sb.append((curr.getTime() - last.getTime()) / 1000).append(delimiterOut);//毫秒转换为秒
        sb.append(last.getCellId()).append(delimiterOut);
        sb.append(last.getLac()).append(delimiterOut);
        sb.append(curr.getCellId()).append(delimiterOut);
        sb.append(curr.getLac()).append(delimiterOut);
        sb.append(last.getLongitude()).append(delimiterOut);
        sb.append(last.getLatitude()).append(delimiterOut);
        sb.append(curr.getLongitude()).append(delimiterOut);
        sb.append(curr.getLatitude()).append(delimiterOut);
        sb.append(last.getCell_province()).append(delimiterOut);
        sb.append(last.getCell_city()).append(delimiterOut);
        sb.append(last.getCell_county()).append(delimiterOut);
        sb.append(curr.getCell_province()).append(delimiterOut);
        sb.append(curr.getCell_city()).append(delimiterOut);
        sb.append(curr.getCell_county()).append(delimiterOut);
        sb.append(last.getArea_id()).append(delimiterOut);
        sb.append(last.getLrc_province()).append(delimiterOut);
        sb.append(last.getLrc_city()).append(delimiterOut);
        sb.append(last.getLrc_county()).append(delimiterOut);
        sb.append(curr.getLrc_province()).append(delimiterOut);
        sb.append(curr.getLrc_city()).append(delimiterOut);
        sb.append(curr.getLrc_county());
        return sb.toString();
    }
}

