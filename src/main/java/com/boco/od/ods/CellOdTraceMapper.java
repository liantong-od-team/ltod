package com.boco.od.ods;

import com.boco.od.common.*;

import com.boco.od.utils.DateUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class CellOdTraceMapper  extends Mapper<LongWritable, Text, OdTracePair, OdTraceRecord>{

    private static Metadata meta= new Metadata(Constants.STAGE_PROP_PATH);

    private int AREA_ID_INDEX;
    private int TICKET_TYPE_INDEX;
    private int CALL_NUMBER_INDEX;
    private int CALLED_NUMBER_INDEX;
    private int BEGIN_TIME_INDEX;
    private int LAC_INDEX;
    private int CELL_ID_INDEX;

    private int columnSize;
    private String delimiterIn;
    private Pattern pattern ;

    protected OdTracePair rskey = new OdTracePair();
    protected OdTraceRecord rsval = new OdTraceRecord();

    private String AREA_ID_VAL;
    private String TICKET_TYPE_VAL;
    private String CALL_NUMBER_VAL;
    private String CALLED_NUMBER_VAL;
    private String BEGIN_TIME_VAL;
    private String LAC_VAL;
    private String CELL_ID_VAL;

    private String msisdn;
    private long time;

    public CellOdTraceMapper() {
    }

    @Override
    protected void setup(Context context) {
        AREA_ID_INDEX = meta.getIntValue("AREA_ID");
        TICKET_TYPE_INDEX = meta.getIntValue("TICKET_TYPE");
        CALL_NUMBER_INDEX = meta.getIntValue("CALL_NUMBER");
        CALLED_NUMBER_INDEX = meta.getIntValue("CALLED_NUMBER");
        BEGIN_TIME_INDEX = meta.getIntValue("BEGIN_TIME");
        LAC_INDEX = meta.getIntValue("LAC");
        CELL_ID_INDEX = meta.getIntValue("CELL_ID");

        columnSize = meta.getIntValue("column.size");
        delimiterIn = meta.getValue("delimiterIn");
        pattern = Pattern.compile(delimiterIn);
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

        AREA_ID_VAL=vals[AREA_ID_INDEX];
        TICKET_TYPE_VAL=vals[TICKET_TYPE_INDEX];
        CALL_NUMBER_VAL=vals[CALL_NUMBER_INDEX];
        CALLED_NUMBER_VAL=vals[CALLED_NUMBER_INDEX];
        BEGIN_TIME_VAL=vals[BEGIN_TIME_INDEX];
        LAC_VAL=vals[LAC_INDEX];
        CELL_ID_VAL=vals[CELL_ID_INDEX];

        time = DateUtils.convertString2Time(BEGIN_TIME_VAL);
        //时间非法
        if(time<0){
            context.getCounter(COUNTER.ErrorDate).increment(1);
            return;
        }

//逻辑需确定？
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
        if("00".equals(TICKET_TYPE_VAL)||"10".equals(TICKET_TYPE_VAL)
                ||"12".equals(TICKET_TYPE_VAL)||"18".equals(TICKET_TYPE_VAL)){
            msisdn = CALL_NUMBER_VAL;
        }else{
            msisdn = CALLED_NUMBER_VAL;
        }
        //号码为空
        if(msisdn==null||"".equals(msisdn.trim())){
            context.getCounter(COUNTER.Null).increment(1);
            return;
        }
        rskey.set(msisdn,time);

//        private String msisdn;
//        private long time;
//        private String cellId;
//        private String lac;
//        private double longitude;
//        private double latitude;
//        private String cell_province;
//        private String cell_city;
//        private String cell_county;
//        private String area_id;
//        private String lrc_province;
//        private String lrc_city;
//        private String lrc_county;
        rsval.setDateDay(BEGIN_TIME_VAL.substring(0,8));
//        rsval.setMsisdn(msisdn);
        rsval.setTime(time);
        rsval.setCellId(CELL_ID_VAL);
        rsval.setLac(LAC_VAL);
        rsval.setArea_id(AREA_ID_VAL);
        //关联经纬度
        //TODO
        rsval.setLongitude(0);
        rsval.setLatitude(0);
        //关联地理位置
        //TODO
        rsval.setCell_province("cell_province");
        rsval.setCell_city("cell_city");
        rsval.setCell_county("cell_country");
        //关联归属地
        //TODO
        rsval.setLrc_province("lrc_province");
        rsval.setLrc_city("lrc_city");
        rsval.setLrc_county("lrc_country");
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
