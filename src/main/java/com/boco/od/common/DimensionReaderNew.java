package com.boco.od.common;


import com.boco.od.utils.DsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ranhualin on 2015/8/3.
 */
public class DimensionReaderNew implements Serializable {

    private Map<String, String[]> cellMap;
    private Map<String, String[]> lrcMap;
    private Configuration conf;

    private String cell_fileName;
    private String cell_delimiterIn;
    private int cell_columnSize;
    private String lrc_fileName;
    private String lrc_delimiterIn;
    private int lrc_columnSize;

    private int CELL_INDEX_LAC;
    private int CELL_INDEX_CELL_ID;
    private int CELL_INDEX_LONGITUDE;
    private int CELL_INDEX_LATITUDE;
    private int CELL_INDEX_PROVINCE;
    private int CELL_INDEX_CITY;

    private int LRC_INDEX_MSISDN;
    private int LRC_INDEX_PROVINCE;
    private int LRC_INDEX_CITY;
    // private int LRC_INDEX_COUNTRY;
//    private int LRC_INDEX_MONTH;
//    private int LRC_INDEX_DAY;
    private int cell_succ;
    private int cell_error;
    private int lrc_succ;
    private int lrc_error;
    private int file_error;

    private List<String> errorFile=new ArrayList<String>();

    public DimensionReaderNew(Configuration conf) {
        this.conf = conf;
        Metadata cellMeta = new Metadata(Constants.CELL_PROP_PATH);
        cell_fileName= conf.get("cell_fileName",cellMeta.getValue("fileName"));
        cell_delimiterIn = cellMeta.getValue("delimiterIn");
        cell_columnSize = cellMeta.getIntValue("column.size");
        CELL_INDEX_LAC = cellMeta.getIntValue("LAC");
        CELL_INDEX_CELL_ID = cellMeta.getIntValue("CELL_ID");
        CELL_INDEX_LONGITUDE = cellMeta.getIntValue("LONGITUDE");
        CELL_INDEX_LATITUDE = cellMeta.getIntValue("LATITUDE");
        CELL_INDEX_PROVINCE = cellMeta.getIntValue("PROVINCE");
        CELL_INDEX_CITY = cellMeta.getIntValue("CITY");

        Metadata lrcMeta = new Metadata(Constants.LRC_PROP_PATH);
        lrc_fileName = conf.get("lrc_fileName",lrcMeta.getValue("fileName"));
        lrc_delimiterIn = lrcMeta.getValue("delimiterIn");
        lrc_columnSize = lrcMeta.getIntValue("column.size");
        LRC_INDEX_MSISDN = lrcMeta.getIntValue("MSISDN");
        LRC_INDEX_PROVINCE = lrcMeta.getIntValue("PROVINCE");
        LRC_INDEX_CITY = lrcMeta.getIntValue("CITY");
//        LRC_INDEX_MONTH = lrcMeta.getIntValue("MONTH_ID");
//        LRC_INDEX_DAY = lrcMeta.getIntValue("DAY_ID");
        init();
    }

    public Map<String, String[]> getCellMap() {
        if (cellMap == null) {
            init();
        }
        return cellMap;
    }

    public Map<String, String[]> getLrcMap() {
        if (lrcMap == null) {
            init();
        }
        return lrcMap;
    }

    private void init() {
        cellMap = new HashMap<String, String[]>();
        lrcMap = new HashMap<String, String[]>();
        BufferedReader br = null;
        //获得当前作业的DistributedCache相关文件
        try {
           Path[] distributePaths = DistributedCache.getLocalCacheFiles(conf);
//            System.out.println("#distributePaths#" + distributePaths);
            String info = null;
            for (Path  p: distributePaths) {
                System.out.println("#p==#" + p.toString());
                p.toString()
                if (p.toString().toLowerCase().contains(cell_fileName.toLowerCase())) {
                    //读缓存文件，并放到mem中
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (info = br.readLine())) {
                        String[] parts = info.split(cell_delimiterIn, -1);
                        if (parts.length >= cell_columnSize) {
                            String country = "";
                            try {
                                country = DsFactory.getinstance().queryCountry(parts[CELL_INDEX_LONGITUDE], parts[CELL_INDEX_LATITUDE]);
                            } catch (Exception e) {
                            }
                            cellMap.put(parts[CELL_INDEX_LAC] + "-" + parts[CELL_INDEX_CELL_ID],
                                    new String[]{parts[CELL_INDEX_LONGITUDE], parts[CELL_INDEX_LATITUDE], parts[CELL_INDEX_PROVINCE]
                                            , parts[CELL_INDEX_CITY], country}
                            );
                            cell_succ++;
                        }else{
                           cell_error++;
                        }
                    }
                } else if (p.toString().toLowerCase().contains(lrc_fileName.toLowerCase())) {
                    //读缓存文件，并放到mem中
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (info = br.readLine())) {
                        String[] parts = info.split(lrc_delimiterIn, -1);
                        if (parts.length >= lrc_columnSize) {
                            lrcMap.put(parts[LRC_INDEX_MSISDN], new String[]{parts[LRC_INDEX_PROVINCE], parts[LRC_INDEX_CITY]});
                            lrc_succ++;
                        }else{
                            lrc_error++;
                        }
                    }
                }else{
                    file_error++;
                    errorFile.add(p.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

  //    public DimensionReader(){
//        lrcMap = new HashMap<String,DimensionLrc>();
//    }
//    public static void main(String []  args){
//        DimensionReader dr = new DimensionReader();
//        DimensionLrc d1 = new DimensionLrc("AAA","BBB","CCC","201508","03");
//        DimensionLrc d2 = new DimensionLrc("AAA","BBB","CCC","201509","01");
//        DimensionLrc d3 = new DimensionLrc("AAA","BBB","CCC","201508","05");
//        DimensionLrc d4 = new DimensionLrc("AAA","BBB","CCC","201507","03");
//        dr.putLastLrc("AA",d1);
//        dr.putLastLrc("AA",d2);
//        dr.putLastLrc("AA",d3);
//        dr.putLastLrc("AA",d4);
//        dr.putLastLrc("AAqs",d4);
//        dr.putLastLrc("AAdd",d4);
//        System.out.println(dr.getLrcMap());
//    }

    public int getCell_succ() {
        return cell_succ;
    }

    public int getCell_error() {
        return cell_error;
    }

    public int getLrc_succ() {
        return lrc_succ;
    }

    public int getLrc_error() {
        return lrc_error;
    }

    public int getFile_error() {
        return file_error;
    }

    public List<String> getErrorFile() {
        return errorFile;
    }
}
