package com.boco.od.location;

import com.boco.od.common.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by mars on 2015/7/30.
 */
public class LocationOdReducer3 extends Reducer<UserTimePair, Text, NullWritable, Text> {

    private static Text tv = new Text();
    private static Pattern pattern = Pattern.compile(",");
    private static final String outputSep = ",";
    private String[] vArr;

    //current node
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

    // prev node
    String prevDay = "";
    String prevStart_date = "";
    String prevStart_cell_province = "";
    String prevStart_cell_city = "";
    String prevStart_cell_county = "";
    String prevArea_id = "";
    String prevLrc_province = "";
    String prevLrc_city = "";
    String prevLrc_county = "";

    int[] od_cityColIdx_in_RegionColIdx = {0, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14};

    /**
     * 需要补点数据的时间部分，起始补000000,结束补 235959
     */
    static String day_start_time = "000000";
    static String day_end_time = "235959";
    private static int level;
    // dto
    String[] record = new String[18];

    protected void setup(Context ctx) throws InterruptedException,
            IOException {
        //city=1 region=2
        level = ctx.getConfiguration().get("level").equalsIgnoreCase("city") ? 1 : 2;
        System.out.println("level="+level);
    }

    public void reduce(UserTimePair key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        System.out.println(" reduce output...");
        this.travals(level, key, values, ctx);
    }

    /**
     * @param level
     */
    private void travals(int level, UserTimePair key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

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

            //第一条数据
            if ("".equals(prevDay)) {
                if (!Util.isStartOfDay(start_date)) {
                    //补current node 的0点值
                    genRecord("第一条", ctx, level, "cur", day_start_time);

                }
                // 当前节点数据移动到prev
                this.save2Prev(0);

            } else {
                //跨天
                if (!Util.isTheSameDay(prevStart_date, start_date)) {
                    if (!Util.isEndOfDay(prevStart_date)) {
                        //补prev节点的235959值，因为补的是虚拟点，区域是没发生变化的。
                        genRecord("跨天 prev", ctx, level, "prev", day_end_time);
                    }
                    /*
                         移动prev指向虚拟点，此时 prev 指向小于cur 000000 时间点。
                          */
                    this.save2Prev(1);
                }

                //处理数据在同一天的情况
                if (Util.isStartOfDay(start_date) && Util.isStartOfDay(prevStart_date)) {
                    this.save2Prev(0);
                } else {
                    //区县有变化,插值
                    if (!getComparatorKey(level, prevStart_cell_province, prevStart_cell_city, prevStart_cell_county).equals(getComparatorKey(level, start_cell_province, start_cell_city, start_cell_county))) {
                        //插值
                        genNormalRecord("同一天", ctx, level);
                        //当前节点数据移动到prev
                        this.save2Prev(0);
                    }
                }
            }

            //最后一条数据
            if (!values.iterator().hasNext()) {
                if (!Util.isEndOfDay(prevStart_date)) {
                    //需要补 current 节点235959值
                    genRecord("最后一条", ctx, level, "prev", day_end_time);
                }
            }
        }
    }


    private void genNormalRecord(String desc, Context ctx, int level) {
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
        try {
            this.writeRecord(desc, record, level, ctx);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 补值的方法
     * 1.补当前节点000000点的场景只有两种，current为第一条数据或current是跨天的数据且不是000000点数据。
     * 2.补当前节点235959点的场景只有一种，current为最后一条数据。
     * 3.补前一个节点的235959点的场景只有一种，prev与current是跨天数据，并且prev不是235959点的数据。
     *
     * @param nodeType  cur 为当前节点，prev为前一个节点
     * @param valueType 000000 为起始时间，235959为结束时间
     */
    private void genRecord(String desc, Context ctx, int level, String nodeType, String valueType) {

        if ("cur".equalsIgnoreCase(nodeType)) {
            //补当前节点的000000点值
            if (day_start_time.equals(valueType)) {
                record[0] = date_day;  /*归属日*/
                record[1] = date_day + day_start_time; /*开始时间*/
                record[2] = start_date;/*结束时间*/
                record[3] = msisdn;/*用户*/
                record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                if ("".equals(prevDay)) {
                    record[5] = start_cell_province; /*开始扇区-省*/
                    record[6] = start_cell_city; /*开始扇区-市*/
                    record[7] = start_cell_county; /*开始扇区-区县*/

                } else {
                    record[5] = prevStart_cell_province; /*开始扇区-省*/
                    record[6] = prevStart_cell_city; /*开始扇区-市*/
                    record[7] = prevStart_cell_county; /*开始扇区-区县*/
                }
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
                try {
                    this.writeRecord(desc, record, level, ctx);
                } catch (IOException e) {
                    e.printStackTrace();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (day_end_time.equals(valueType)) {
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
                try {
                    this.writeRecord(desc, record, level, ctx);
                } catch (IOException e) {
                    e.printStackTrace();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {

                // 错误参数
                System.out.println("请检查参数，对cur节点 非 000000 进行非法不值");
            }

        } else if ("prev".equalsIgnoreCase(nodeType)) {
            //补prev节点的235959点值
            if (day_end_time.equals(valueType)) {
                record[0] = prevDay;  /*归属日*/
                record[1] = prevStart_date; /*开始时间*/
                record[2] = prevDay + day_end_time;
                ;/*结束时间*/
                record[3] = msisdn;/*用户*/
                record[4] = String.valueOf(Util.calcTime(record[1], record[2])); /*驻留时长*/
                record[5] = prevStart_cell_province; /*开始扇区-省*/
                record[6] = prevStart_cell_city; /*开始扇区-市*/
                record[7] = prevStart_cell_county; /*开始扇区-区县*/
                record[8] = prevStart_cell_province; /*截止扇区-省*/
                record[9] = prevStart_cell_city;  /*截止扇区-市*/
                record[10] = prevStart_cell_county; /*截止扇区-区县*/
                record[11] = area_id; /*归属地区号*/
                record[12] = lrc_province; /*归属地-省*/
                record[13] = lrc_city; /*归属地-市*/
                record[14] = lrc_county; /*归属地-区县*/
                record[15] = ""; /*高铁概率*/
                record[16] = ""; /*高速概率*/
                record[17] = ""; /*普通公路概率*/
                try {
                    this.writeRecord(desc, record, level, ctx);
                } catch (IOException e) {
                    e.printStackTrace();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 错误参数
                System.out.println("请检查参数，对prev节点 非235959 进行非法补值");
            }
        }
    }

    /**
     * 记录上一条数据
     *
     * @param type 1 ： 虚拟点 ， 其他 ：普通存值
     */
    private void save2Prev(int type) {
        // type == 1 是虚拟点
        if (type == 1) {
            prevDay = date_day;
            prevStart_date = date_day + day_start_time;
        } else {
            //其他情况下，做普通的current到prev的赋值
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

    }

    /**
     * @param record
     * @param level
     * @param ctx
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    private void writeRecord(String desc, String[] record, int level, Context ctx) throws IOException, InterruptedException {
        System.out.println(desc);
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


}
