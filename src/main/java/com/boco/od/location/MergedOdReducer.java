package com.boco.od.location;

import com.boco.od.common.Util;
import com.boco.od.utils.DistanceUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/7/30.
 */
public class MergedOdReducer extends Reducer<UserTimePair, Text, NullWritable, Text> {

    private static Text tv = new Text();
    private static Pattern pattern = Pattern.compile(",");
    private static final String outputSep = ",";
    private String[] vArr;

    //current node
    String msisdn;
    String start_date;
    String end_date;
    String remain_times;
    String start_lng;
    String start_lat;
    String start_cell_province;
    String start_cell_city;
    String start_cell_county;
    String end_cell_province;
    String end_cell_city;
    String end_cell_county;
    String area_id = "";
    String lrc_province = "";
    String lrc_city = "";

    private double tmpDistance = 0d;
    private long tmpTimeCost = 0l;
    private double tmpSpeed = 0l;

    // prev node
    String prevStart_date = "";
    String prevEnd_date = "";
    String prevStart_cell_province = "";
    String prevStart_cell_city = "";
    String prevStart_cell_county = "";
    String prevArea_id = "";
    String prevLrc_province = "";
    String prevLrc_city = "";
    String prevStart_lng = "";
    String prevStart_lat = "";

    //origin point
    String originStart_date = "";
    String originStart_cell_province = "";
    String originStart_cell_city = "";
    String originStart_cell_county = "";
    String originStart_lng = "";
    String originStart_lat = "";
    String orginFlag = "";

    // od_line
    StringBuilder od_line_builder = new StringBuilder();
    // od_line 保存的内容  :  省|市|区县|距离|时间|速度
    String strTpl = "%s^%s^%s^%s^%s^%s";
    String pointSep = "|";

    int[] od_cityColIdx_in_RegionColIdx = {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 13, 14, 15, 16, 17};

    private static int level;
    private static int stayTime; //停留时间阈值
    private static int default_stayTime = 12; //停留时间阈值


    // dto
    String[] record = new String[18];
    StringBuffer sb = new StringBuffer();

    protected void setup(Context ctx) throws InterruptedException,
            IOException {
        //city=1 region=2
        level = ctx.getConfiguration().get("level").equalsIgnoreCase("city") ? 1 : 2;
        stayTime = ctx.getConfiguration().getInt("stayTime", default_stayTime);
        System.out.println("level=" + level);
    }

    public void reduce(UserTimePair key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        System.out.println(" reduce output...");
        this.resetEnv4User();
        this.travals(level, key, values, ctx);
    }

    /**
     * init prev param for per user
     */
    private void resetEnv4User() {
        prevStart_date = "";
        prevEnd_date = "";
        prevStart_cell_province = "";
        prevStart_cell_city = "";
        prevStart_cell_county = "";
        prevArea_id = "";
        prevLrc_province = "";
        prevLrc_city = "";
        prevStart_lng = "";
        prevStart_lat = "";

        originStart_date = "";
        originStart_cell_province = "";
        originStart_cell_city = "";
        originStart_cell_county = "";
        originStart_lng = "";
        originStart_lat = "";
        orginFlag = "";
        od_line_builder.setLength(0);
    }


    /**
     * @param level
     */
    private void travals(int level, UserTimePair key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        int len=0;
        for (Text val : values) {
            len++;
            // 0-21 strings
            vArr = pattern.split(val.toString(), -1);

            start_date = vArr[1];
            end_date = vArr[2];
            msisdn = vArr[3];
            remain_times = vArr[4];
            start_lng = vArr[9];
            start_lat = vArr[10];
            start_cell_province = vArr[13];
            start_cell_city = vArr[14];
            start_cell_county = vArr[15];
            end_cell_province = vArr[16];
            end_cell_city = vArr[17];
            end_cell_county = vArr[18];
            area_id = vArr[19];

            lrc_province = vArr[20];
            lrc_city = vArr[21];


            if ("".equals(prevStart_date)) {
                //第一条数据，保存至prev
                this.save2Prev(0);
                //将第一个点，标记为O点，属性为S(数据起始点)
                this.tagPrev2Origin("S");
                //将origin 点加入 od_line
                this.addOrigin2PassbyPoint();
            } else {
                //区县有变化
                if (!getComparatorKey(level, prevStart_cell_province, prevStart_cell_city, prevStart_cell_county).equals(getComparatorKey(level, start_cell_province, start_cell_city, start_cell_county))) {
                    //停留时间大于等于给定的阈值(默认为12小时) /*&& 前一个节点不是数据O点，即排除O->prev 只有一个点并且是数据开始点的情况*/
                    if (Util.isStayed(prevStart_date, start_date, stayTime)/*&&!originStart_date.equals(prevStart_date)*/) {
                        //1.生成O->prev 的OD记录，记录D点属性为N
                        this.genNormalRecord("O->prev", ctx, level);

                        //2.prev保存为新的O点，记录O点属性为N
                        this.tagPrev2Origin("N");
                        od_line_builder.setLength(0); // 经过点清空,从新的O点开始记录
                        //将origin 点加入 od_line
                        this.addOrigin2PassbyPoint();

                        //3.将cur点作为第一个经过点记录在od_line
                        this.tagPassbyPoint();

                        //4.将cur保存为prev点
                        this.save2Prev(0);

                    } else {
                        //小于停留时间阈值，记录为经过的点

                        //1.将cur点作为经过点记录在od_line
                        this.tagPassbyPoint();

                        //2.将cur保存为prev点
                        this.save2Prev(1);
                    }
                }else{
                    //区县无变化时，不需要更新时间，只需要更新区县内的基站间变化信息
                    this.save2Prev(1);

                }
            }
            //判断为最后一条数据 && 排除掉最后一个点是孤点的情况，即在上一步中根据最后一个点生成了o->prev 的od记录，此时的curr点是最后一个孤立点。
            if (!values.iterator().hasNext() &&len!=1 /*&& !prevStart_date.equals(start_date)*/) {

                /**
                 * 注释掉 将最后一个点加入经过的代码，因为在过程中，已经对它处理过了。
                 * 最后一个点有两种情况，1是与prev区县相同，2是不同。都已经在上一步中，生成od数据或记录了经过点，这里不需要再处理。
                 */
                //计算prev-> D点的距离/时间/速度，记录到od_line
//                this.tagPassbyPoint();

                //生成OD记录，O->curr记录，标记D点属性为E,这里要在O点属性上Append D点的属性
                this.genLastRecord("O->curr", ctx, level);
            }


        }// end of for

    }

    /**
     * 根据Origin -> prev 生成 od数据
     *
     * @param desc
     * @param ctx
     * @param level
     */
    private void genNormalRecord(String desc, Context ctx, int level) {
        //组织写入数据
        record[0] = this.originStart_date;  /*O点的时间*/
        record[1] = prevStart_date; /*D点的时间*/

        record[2] = "" + stayTime;/*停留时间阈值*/
        record[3] = msisdn;/*用户*/
        tmpTimeCost = Util.calcTime(record[0], record[1]) / 1000; //结果为秒
        record[4] = String.valueOf(tmpTimeCost); /*驻留时长*/

        //add 5-距离 ,6-速度
        tmpDistance = DistanceUtils.getDistance(originStart_lat.trim(), originStart_lng.trim(), prevStart_lat.trim(), prevStart_lng.trim());
        record[5] = String.valueOf(tmpDistance); /*开始-结束 扇区的距离 单位km*/
        if (tmpTimeCost != 0) {
            record[6] = String.valueOf(Util.round(tmpDistance * 3600 / tmpTimeCost, 2)); /*计算速度值*/
        } else {
            record[6] = "0";
        }

        record[7] = originStart_cell_province; /*开始扇区-省*/
        record[8] = originStart_cell_city; /*开始扇区-市*/
        record[9] = originStart_cell_county; /*开始扇区-区县*/
        record[10] = prevStart_cell_province; /*截止扇区-省*/
        record[11] = prevStart_cell_city;  /*截止扇区-市*/
        record[12] = prevStart_cell_county; /*截止扇区-区县*/
        record[13] = area_id; /*归属地区号*/
        record[14] = lrc_province; /*归属地-省*/
        record[15] = lrc_city; /*归属地-市*/
        record[16] = orginFlag + "N";  /*OD记录的属性标记, SN,SE,NN,NE*/
        record[17] = od_line_builder.toString(); /*经过的地区*/

        if("sn".equalsIgnoreCase(orginFlag)){
            record[1] = prevEnd_date; /*D点的时间*/
        }

        try {
            this.writeRecord(desc, record, level, ctx);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 根据Origin -> current 生成 od数据
     *
     * @param desc
     * @param ctx
     * @param level
     */
    private void genLastRecord(String desc, Context ctx, int level) {
        //组织写入数据
        record[0] = this.originStart_date;  /*O点的时间*/
        record[1] = prevStart_date; /*D点的时间*/
        record[2] = "" + stayTime;/*停留时间阈值*/
        record[3] = msisdn;/*用户*/
        tmpTimeCost = Util.calcTime(record[0], record[1]) / 1000; //结果为秒
        record[4] = String.valueOf(tmpTimeCost); /*驻留时长*/

        //add 5-距离 ,6-速度
        tmpDistance = DistanceUtils.getDistance(originStart_lat.trim(), originStart_lng.trim(), start_lat.trim(), start_lng.trim());
        record[5] = String.valueOf(tmpDistance); /*开始-结束 扇区的距离 单位km*/
        if (tmpTimeCost != 0) {
            record[6] = String.valueOf(Util.round(tmpDistance * 3600 / tmpTimeCost, 2)); /*计算速度值*/
        } else {
            record[6] = "0";
        }

        record[7] = originStart_cell_province; /*开始扇区-省*/
        record[8] = originStart_cell_city; /*开始扇区-市*/
        record[9] = originStart_cell_county; /*开始扇区-区县*/
        record[10] = start_cell_province; /*截止扇区-省*/
        record[11] = start_cell_city;  /*截止扇区-市*/
        record[12] = start_cell_county; /*截止扇区-区县*/
        record[13] = area_id; /*归属地区号*/
        record[14] = lrc_province; /*归属地-省*/
        record[15] = lrc_city; /*归属地-市*/
        record[16] = orginFlag + "E";  /*OD记录的属性标记, SN,SE,NN,NE*/
        record[17] = od_line_builder.toString(); /*经过的地区*/

        if("se".equalsIgnoreCase(record[16])){
            record[1] = this.prevEnd_date;
        }

        try {
            this.writeRecord(desc, record, level, ctx);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 记录上一条数据
     *
     */
    private void save2Prev(int type) {
        if(type==0){
            prevStart_date = start_date;
        }
        prevEnd_date = end_date;
        prevStart_cell_province = start_cell_province;
        prevStart_cell_city = start_cell_city;
        prevStart_cell_county = start_cell_county;
        prevArea_id = area_id;
        prevLrc_province = lrc_province;
        prevLrc_city = lrc_city;
        prevStart_lng = start_lng;
        prevStart_lat = start_lat;
    }

    /**
     * 记录O点(起始点)数据
     *
     * @param flag O点的属性，标识了O点或D点数据是真正的停留产生的，还是数据集的起始和截止点产生的
     *             正常数据集中一定存在S标识的虚拟O点数据，E标识的虚拟D点数据，以及N标识的真实的OD数据。
     */
    private void tagPrev2Origin(String flag) {
        originStart_date = prevStart_date;
        originStart_cell_province = prevStart_cell_province;
        originStart_cell_city = prevStart_cell_city;
        originStart_cell_county = prevStart_cell_county;
        originStart_lng = prevStart_lng;
        originStart_lat = prevStart_lat;
        orginFlag = flag;
    }


    /**
     * @param record
     * @param level
     * @param ctx
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeRecord(String desc, String[] record, int level, Context ctx) throws IOException, InterruptedException {
        System.out.println(desc);
//        StringBuffer sb = new StringBuffer();
        sb.setLength(0);
        if (level == 1) {
            // city
            for (int idx = 0; idx < od_cityColIdx_in_RegionColIdx.length; idx++) {
                if (idx != od_cityColIdx_in_RegionColIdx.length - 1) {
                    sb.append(record[od_cityColIdx_in_RegionColIdx[idx]]).append(outputSep);
                } else {
                    sb.append(record[od_cityColIdx_in_RegionColIdx[idx]]);
                }
            }

            tv.set(sb.toString());
            ctx.write(NullWritable.get(), tv);
            System.out.println(sb.toString());
        } else if (level == 2) {
            //region
            for (int i = 0; i < record.length; i++) {
                if (i != record.length - 1) {
                    sb.append(record[i]).append(outputSep);
                } else {
                    sb.append(record[i]);
                }
            }
            tv.set(sb.toString());
            ctx.write(NullWritable.get(), tv);
            System.out.println(sb.toString());
        } else {
            //noting to do
        }

    }

    /**
     * 根据比较级别，取待比较的省市区县字符串
     *
     * @param level
     * @param province
     * @param city
     * @param region
     * @return
     */
    private String getComparatorKey(int level, String province, String city, String region) {

        //city
        if (level == 1) {
            return province + city;

        } else {
            //region
            return province + city + region;
        }
    }

    /**
     * 将起始O点加入od_line，为了路径展示方便
     */
    private void addOrigin2PassbyPoint() {

        if (od_line_builder.length() > 0) {
            od_line_builder.append(pointSep);
        }
        od_line_builder.append(String.format(strTpl, originStart_cell_province, originStart_cell_city, originStart_cell_county, 0, 0, 0));
    }


    /**
     * 将当前点纳入经过的点序列，同时计算出prev->cur 的距离，时间，速度
     */
    private void tagPassbyPoint() {

        //开始-结束 扇区的距离 单位km
        tmpDistance = DistanceUtils.getDistance(prevStart_lat.trim(), prevStart_lng.trim(), start_lat.trim(), start_lng.trim());
        //时间，结果为秒
        tmpTimeCost = Util.calcTime(prevStart_date, start_date) / 1000;
        if (tmpTimeCost != 0) {
            tmpSpeed = Util.round(tmpDistance * 3600 / tmpTimeCost, 2); /*计算速度值*/
        } else {
            tmpSpeed = 0d;
        }

        if (od_line_builder.length() > 0) {
            od_line_builder.append(pointSep);
        }
        od_line_builder.append(String.format(strTpl, start_cell_province, start_cell_city, start_cell_county, tmpDistance, tmpTimeCost, tmpSpeed));

    }


}
