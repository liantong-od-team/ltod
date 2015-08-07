package com.boco.od.ods;

import com.boco.od.common.*;
import com.boco.od.utils.DateUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by ranhualin on 2015/7/30.
 */
public class CellOdTraceMapper2 extends Mapper<LongWritable, Text, OdTracePair, OdTraceRecord>{

    private Metadata meta;
    private Map<String, String[]> cellMap;
    private Map<String, String[]> lrcMap;

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

    public CellOdTraceMapper2() {
    }

    @Override
    protected void setup(Context context) {

        meta= new Metadata(Constants.STAGE_PROP_PATH);
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

        DimensionReader2 reader = new DimensionReader2(context.getConfiguration());
        cellMap = reader.getCellMap();
        lrcMap = reader.getLrcMap();

        List<String> tes = reader.getErrorFile();
        for(String s:tes){
            context.getCounter("TEST",s).increment(1);
        }

//        cellMap  = new HashMap<String, String[]>();
//        lrcMap = new HashMap<String, DimensionLrc>();
        context.getCounter("RELATIONS","CELL_DIM_SUCC").increment(reader.getCell_succ());
        context.getCounter("RELATIONS","CELL_DIM_OTHER").increment(reader.getCell_other());
        context.getCounter("RELATIONS","CELL_DIM_ERROR").increment(reader.getCell_error());
        context.getCounter("RELATIONS","LRC_DIM_SUCC").increment(reader.getLrc_succ());
        context.getCounter("RELATIONS","LRC_DIM_ERROR").increment(reader.getLrc_error());
        context.getCounter("RELATIONS","ERROR_FILE").increment(reader.getFile_error());
        context.getCounter("RELATIONS","CELL_DIM").increment(cellMap.size());
        context.getCounter("RELATIONS","LRC_DIM").increment(lrcMap.size());
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
        if("00".equals(TICKET_TYPE_VAL)||"10".equals(TICKET_TYPE_VAL)){
            msisdn = CALL_NUMBER_VAL;
        }else if("01".equals(TICKET_TYPE_VAL)||"11".equals(TICKET_TYPE_VAL)){
            msisdn = CALLED_NUMBER_VAL;
        }else{
            msisdn=null;
        }
        //号码为空
        if(msisdn==null||"".equals(msisdn.trim())||msisdn.trim().length()<7){
            context.getCounter(COUNTER.Null).increment(1);
            return;
        }
        if(CELL_ID_VAL==null||"".equals(CELL_ID_VAL.trim())){
            context.getCounter(COUNTER.Null).increment(1);
            return;
        }
        if(LAC_VAL==null||"".equals(LAC_VAL.trim())){
            context.getCounter(COUNTER.Null).increment(1);
            return;
        }
        CELL_ID_VAL = CELL_ID_VAL.trim();
        LAC_VAL = LAC_VAL.trim();
        msisdn = msisdn.trim();
        rskey.set(msisdn,time);
        rsval.setDateDay(BEGIN_TIME_VAL.substring(0,8));
//        rsval.setMsisdn(msisdn);
        rsval.setTime(time);
        rsval.setCellId(CELL_ID_VAL);
        rsval.setLac(LAC_VAL);
        rsval.setArea_id(AREA_ID_VAL);
        //关联经纬度
        String lac_ci = LAC_VAL+"-"+CELL_ID_VAL;
        if(cellMap.containsKey(lac_ci)) {
            String[] dim = cellMap.get(lac_ci);
            rsval.setLongitude(dim[0]);
            rsval.setLatitude(dim[1]);
            rsval.setCell_province(dim[2]);
            rsval.setCell_city(dim[3]);
            rsval.setCell_county(dim[4]);

            context.getCounter("RELATIONS","CELL_SUCC").increment(1);
        }else{
            rsval.setLongitude("");
            rsval.setLatitude("");
            rsval.setCell_province("");
            rsval.setCell_city("");
            rsval.setCell_county("");
            context.getCounter("RELATIONS","CELL_FAIL").increment(1);
        }
        String tac = msisdn.substring(0,7);
        //关联归属地
        if(lrcMap.containsKey(tac)) {
            String[] dim = lrcMap.get(tac);
            rsval.setLrc_province(dim[0]);
            rsval.setLrc_city(dim[1]);
            context.getCounter("RELATIONS","LRC_SUCC").increment(1);
        }else{
            rsval.setLrc_province("");
            rsval.setLrc_city("");
            context.getCounter("RELATIONS","LRC_FAIL").increment(1);
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
        cellMap=null;
        lrcMap=null;
    }
}
