package com.boco.od.location;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.boco.od.common.Util;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/7/30.
 */
public class LocationOdReducer extends Reducer<UserTimePair, Text, NullWritable, Text> {

    private static Text tv = new Text();
    private static final Text SEPARATOR = new Text("------------------------------------------------");
    private static Pattern pattern = Pattern.compile(",");

    private static final String outputSep = ",";

    private String[] vArr;
    String msisdn;
    String date_day;
    String start_date;
    String end_date;
    String remain_times;
    String start_cell_province;
    String start_cell_city;
    String start_cell_county;
    String end_cell_province;
    String end_cell_city;
    String end_cell_county;
    String area_id = "";
    String lrc_province = "";
    String lrc_city = "";
    String lrc_county = "";


    int[] od_cityColIdx_in_RegionColIdx = {0, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14};

    /**
     * 需要补点数据的时间部分，起始补000000,结束补 235959
     */
    static String day_start_time = "000000";
    static String day_end_time = "235959";

    String[] record = new String[18];

    protected void setup(Context ctx) throws InterruptedException,
            IOException {

    }

    public void reduce(UserTimePair key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
       /* ctx.write(null, SEPARATOR);
        System.out.print("key=" + key.getFirst() + " | ");
//        left.set(Integer.toString(key.getFirst()));
        for (Text val : values) {
            System.out.print(val + " %%% ");
//            ctx.write(left, val);
        }
        System.out.println(" ");*/

        //city=1 region=2
        int level = ctx.getConfiguration().get("level").equalsIgnoreCase("city") ? 1 : 2;
        this.travals(level, key, values, ctx);
    }

    /**
     * @param level
     */
    private void travals(int level, UserTimePair key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        String prevDay = "";
        String prevStart_date = "";
        String prevStart_cell_province = "";
        String prevStart_cell_city = "";
        String prevStart_cell_county = "";
        String prevArea_id = "";
        String prevLrc_province = "";
        String prevLrc_city = "";
        String prevLrc_county = "";


        for (Text val : values) {
            // 0-22 strings
            vArr = pattern.split(val.toString());

            date_day = vArr[0];
            start_date = vArr[1];
            end_date = vArr[2];
            msisdn = vArr[3];
            remain_times = vArr[4];
            start_cell_province = vArr[13];
            start_cell_city = vArr[14];
            start_cell_county = vArr[15];
            end_cell_province = vArr[16];
            end_cell_city = vArr[17];
            end_cell_county = vArr[18];
            area_id = vArr[19];
            lrc_province = vArr[20];
            lrc_city = vArr[21];
            lrc_county = vArr[22];


            if ("".equals(prevDay) && !Util.isStartOfDay(start_date)) {
                /**
                 * 当处理第一条数据时，如果在别的处理步骤中没有补初始值，再进行补值。
                 *
                 */
                // 第一条数据时,补00:00:00点

                //组织写入数据
                record[0] = date_day;  /*归属日*/
                record[1] = date_day + day_start_time; /*开始时间*/
                record[2] = start_date;/*结束时间*/
                record[3] = msisdn;/*用户*/
                record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                record[5] = start_cell_province; /*开始扇区-省*/
                record[6] = start_cell_city; /*开始扇区-市*/
                record[7] = start_cell_county; /*开始扇区-区县*/
                record[8] = start_cell_province; /*截止扇区-省*/
                record[9] = start_cell_city;  /*截止扇区-市*/
                record[10] = start_cell_county; /*截止扇区-区县*/
                record[11] = area_id; /*归属地区号*/
                record[12] = lrc_province; /*归属地-省*/
                record[13] = lrc_city; /*归属地-市*/
                record[14] = lrc_county; /*归属地-区县*/
                record[15] = ""; /*高铁概率*/
                record[16] = ""; /*高速概率*/
                record[17] = ""; /*普通公路概率*/
                this.writeRecord(record, level, ctx);

                //更新上次数据
                prevDay = date_day;
                prevStart_date = start_date;
                prevStart_cell_province = start_cell_province;
                prevStart_cell_city = start_cell_city;
                prevStart_cell_county = start_cell_county;
                prevArea_id = area_id;
                prevLrc_province = lrc_province;
                prevLrc_city = lrc_city;
                prevLrc_county = lrc_county;

            } else {
                /**
                 * 在非第一条数据的场景下，需要判断当前数据与上一条数据是否满足以下条件：
                 * 1.是否为同一天的数据，如果不是，则需要生成一条新纪录: od关系为 prev -> prevDay 23:59:59(该条为虚拟数据点)。然后将当前数据赋值保存为prev。
                 *                     如果是同天的数据，比较区县或市是否相同，如相同则忽略，如不同则生成一条od：关系为 prev->current,然后将当前数据赋值保存为prev。
                 *
                 * 2.做完以上操作后，统一判断该条记录是否为分组（values）里的最后一条数据，如果是则生成最后一个数据的OD记录：关系为prev -> current (实际为current->current ，因为上面的操作已经将current值保存到了prev里.)
                 *
                 */

                //当同一天时
                if (Util.isTheSameDay(prevDay, date_day)) {
                    // 区域不相同，产生OD变化时
                    if (!getComparatorKey(level, prevStart_cell_province, prevStart_cell_city, prevStart_cell_county).equals(getComparatorKey(level, start_cell_province, start_cell_city, start_cell_county))) {

                        //组织写入数据
                        record[0] = date_day;  /*归属日*/
                        record[1] = prevStart_date; /*开始时间*/
                        record[2] = start_date;/*结束时间*/
                        record[3] = msisdn;/*用户*/
                        record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                        record[5] = prevStart_cell_province; /*开始扇区-省*/
                        record[6] = prevStart_cell_city; /*开始扇区-市*/
                        record[7] = prevStart_cell_county; /*开始扇区-区县*/
                        record[8] = start_cell_province; /*截止扇区-省*/
                        record[9] = start_cell_city;  /*截止扇区-市*/
                        record[10] = start_cell_county; /*截止扇区-区县*/
                        record[11] = area_id; /*归属地区号*/
                        record[12] = lrc_province; /*归属地-省*/
                        record[13] = lrc_city; /*归属地-市*/
                        record[14] = lrc_county; /*归属地-区县*/
                        record[15] = ""; /*高铁概率*/
                        record[16] = ""; /*高速概率*/
                        record[17] = ""; /*普通公路概率*/
                        this.writeRecord(record, level, ctx);
                        //更新上次数据
                        prevDay = date_day;
                        prevStart_date = start_date;
                        prevStart_cell_province = start_cell_province;
                        prevStart_cell_city = start_cell_city;
                        prevStart_cell_county = start_cell_county;
                        prevArea_id = area_id;
                        prevLrc_province = lrc_province;
                        prevLrc_city = lrc_city;
                        prevLrc_county = lrc_county;

                    }

                } else {// 当跨天时

                    //如果上一条不是235959的数据，则说明没有补当日的结束点，需要补结束点。
                    if(!Util.isEndOfDay(prevStart_date)) {
                        //组织写入数据
                        record[0] = prevDay;  /*归属日*/
                        record[1] = prevStart_date; /*开始时间*/
                        record[2] = prevDay + day_end_time;/*结束时间*/
                        record[3] = msisdn;/*用户*/
                        record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                        record[5] = prevStart_cell_province; /*开始扇区-省*/
                        record[6] = prevStart_cell_city; /*开始扇区-市*/
                        record[7] = prevStart_cell_county; /*开始扇区-区县*/
                        record[8] = prevStart_cell_province; /*截止扇区-省*/
                        record[9] = prevStart_cell_city;  /*截止扇区-市*/
                        record[10] = prevStart_cell_county; /*截止扇区-区县*/
                        record[11] = prevArea_id; /*归属地区号*/
                        record[12] = prevLrc_province; /*归属地-省*/
                        record[13] = prevLrc_city; /*归属地-市*/
                        record[14] = prevLrc_county; /*归属地-区县*/
                        record[15] = ""; /*高铁概率*/
                        record[16] = ""; /*高速概率*/
                        record[17] = ""; /*普通公路概率*/
                        this.writeRecord(record, level, ctx);

                        //更新上次数据
                        prevDay = record[0];
                        prevStart_date = record[1];
//                        prevStart_cell_province = start_cell_province;
//                        prevStart_cell_city = start_cell_city;
//                        prevStart_cell_county = start_cell_county;
//                        prevArea_id = area_id;
//                        prevLrc_province = lrc_province;
//                        prevLrc_city = lrc_city;
//                        prevLrc_county = lrc_county;
                    }
                    //如果当前条不是000000的数据，则说明没有补当日的开始点，需要补开始点。
                    if(!Util.isStartOfDay(start_date)){
                        //组织写入数据
                        record[0] = date_day;  /*归属日*/
                        record[1] = date_day+day_start_time; /*开始时间*/
                        record[2] = start_date;/*结束时间*/
                        record[3] = msisdn;/*用户*/
                        record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                        record[5] = prevStart_cell_province; /*开始扇区-省*/
                        record[6] = prevStart_cell_city; /*开始扇区-市*/
                        record[7] = prevStart_cell_county; /*开始扇区-区县*/
                        record[8] = start_cell_province; /*截止扇区-省*/
                        record[9] = start_cell_city;  /*截止扇区-市*/
                        record[10] = start_cell_county; /*截止扇区-区县*/
                        record[11] = area_id; /*归属地区号*/
                        record[12] = lrc_province; /*归属地-省*/
                        record[13] = lrc_city; /*归属地-市*/
                        record[14] = lrc_county; /*归属地-区县*/
                        record[15] = ""; /*高铁概率*/
                        record[16] = ""; /*高速概率*/
                        record[17] = ""; /*普通公路概率*/
                        this.writeRecord(record, level, ctx);

                        //更新上次数据
                        prevDay = date_day;
                        prevStart_date = start_date;
                        prevStart_cell_province = start_cell_province;
                        prevStart_cell_city = start_cell_city;
                        prevStart_cell_county = start_cell_county;
                        prevArea_id = area_id;
                        prevLrc_province = lrc_province;
                        prevLrc_city = lrc_city;
                        prevLrc_county = lrc_county;
                    }

                    //更新上次数据
                    prevDay = date_day;
                    prevStart_date = start_date;
                    prevStart_cell_province = start_cell_province;
                    prevStart_cell_city = start_cell_city;
                    prevStart_cell_county = start_cell_county;
                    prevArea_id = area_id;
                    prevLrc_province = lrc_province;
                    prevLrc_city = lrc_city;
                    prevLrc_county = lrc_county;

                }


                if (!values.iterator().hasNext()) {
                    //最后一条数据时,补充23:59:59点

                    //组织写入数据
                    record[0] = date_day;  /*归属日*/
                    record[1] = start_date; /*开始时间*/
                    record[2] = date_day + day_end_time;/*结束时间*/
                    record[3] = msisdn;/*用户*/
                    record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                    record[5] = start_cell_province; /*开始扇区-省*/
                    record[6] = start_cell_city; /*开始扇区-市*/
                    record[7] = start_cell_county; /*开始扇区-区县*/
                    record[8] = start_cell_province; /*截止扇区-省*/
                    record[9] = start_cell_city;  /*截止扇区-市*/
                    record[10] = start_cell_county; /*截止扇区-区县*/
                    record[11] = area_id; /*归属地区号*/
                    record[12] = lrc_province; /*归属地-省*/
                    record[13] = lrc_city; /*归属地-市*/
                    record[14] = lrc_county; /*归属地-区县*/
                    record[15] = ""; /*高铁概率*/
                    record[16] = ""; /*高速概率*/
                    record[17] = ""; /*普通公路概率*/
                    this.writeRecord(record, level, ctx);
                }
            }
            System.out.print(val + " , ");

        }

    }


    private void writeRecord(String[] record, int level, Context ctx) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        if (level == 1) {
            // city
            for (int idx = 0; idx < od_cityColIdx_in_RegionColIdx.length; idx++) {
                if (idx != record.length - 1) {
                    sb.append(record[od_cityColIdx_in_RegionColIdx[idx]]).append(outputSep);
                } else {
                    sb.append(record[od_cityColIdx_in_RegionColIdx[idx]]);
                }
            }

            tv.set(sb.toString());
            ctx.write(NullWritable.get(), tv);
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


}
