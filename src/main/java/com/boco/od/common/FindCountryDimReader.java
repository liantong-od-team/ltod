package com.boco.od.common;


import com.boco.od.utils.geo.GisTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ranhualin on 2015/8/3.
 */
public class FindCountryDimReader implements Serializable {

    private Map<String, String[]> cellMap;
    private Map<String, String[]> lrcMap;
    private Configuration conf;

    private String cell_fileName;
    private String cell_delimiterIn;
    private int cell_columnSize;


    private int CELL_INDEX_LAC;
    private int CELL_INDEX_CELL_ID;
    private int CELL_INDEX_LONGITUDE;
    private int CELL_INDEX_LATITUDE;
    private int CELL_INDEX_PROVINCE;
    private int CELL_INDEX_CITY;
    private int CELL_INDEX_COUNTRY;

    private int cell_succ;
    private int cell_error;
    private int cell_other;
    private int cell_localfile_succ;
    private int cell_oracale_succ;
    private int cell_get_error;
    private int lrc_succ;
    private int lrc_error;
    private int file_error;

    private String join_type;

    private int dim_cell_col_size;

    private List<String> errorFile = new ArrayList<String>();

    public FindCountryDimReader(Configuration conf) {
        this.conf = conf;
        Metadata cellMeta = new Metadata(Constants.CELL_PROP_PATH);
        cell_fileName = conf.get("cell_fileName", cellMeta.getValue("fileName"));
        join_type = conf.get("join_type", "");
        //TODO
        cell_delimiterIn = /*",";//*/cellMeta.getValue("delimiterIn");
        //填好区县的维度表
        cell_columnSize = cellMeta.getIntValue("fixed_column.size");//cellMeta.getIntValue("column.size");
        CELL_INDEX_LAC = cellMeta.getIntValue("LAC");
        CELL_INDEX_CELL_ID = cellMeta.getIntValue("CELL_ID");
        CELL_INDEX_LONGITUDE = cellMeta.getIntValue("LONGITUDE");
        CELL_INDEX_LATITUDE = cellMeta.getIntValue("LATITUDE");
        CELL_INDEX_PROVINCE = cellMeta.getIntValue("PROVINCE");
        CELL_INDEX_CITY = cellMeta.getIntValue("CITY");
        CELL_INDEX_COUNTRY = cellMeta.getIntValue("COUNTRY");

        init();
    }

    public Map<String, String[]> getCellMap() {
        if (cellMap == null) {
            init();
        }
        return cellMap;
    }

    private void init() {
        cellMap = new HashMap<String, String[]>();
        BufferedReader br = null;
        //获得当前作业的DistributedCache相关文件
        try {
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(conf);
            System.out.println("#distributePaths#" + distributePaths);
            String info = null;
            for (Path p : distributePaths) {
                System.out.printf("#p==# %s size=%s \n", p.toString(), new File(p.toString()).length());
                if (p.toUri().getPath().toLowerCase().contains(cell_fileName.toLowerCase())) {
                    //读缓存文件，并放到mem中
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (info = br.readLine())) {
                        String[] parts = info.split(cell_delimiterIn, -1);
                        dim_cell_col_size = parts.length;
                        if (parts.length >= cell_columnSize) {
                            String country = "";
                            String provinc = parts[CELL_INDEX_PROVINCE];
                            if (provinc != null && (provinc.trim().equals("011") || provinc.trim().equals("013") || provinc.trim().equals("018"))) {

                                cellMap.put(parts[CELL_INDEX_LAC] + "-" + parts[CELL_INDEX_CELL_ID],
                                        new String[]{parts[CELL_INDEX_LONGITUDE], parts[CELL_INDEX_LATITUDE], parts[CELL_INDEX_PROVINCE]
                                                , parts[CELL_INDEX_CITY], parts[CELL_INDEX_COUNTRY]}

                                );
                                cell_succ++;
                            } else {
                                cell_other++;
                            }
                        } else {
                            cell_error++;
                        }
                    }
                } else {
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

    public int getCell_other() {
        return cell_other;
    }

    public int getCell_localfile_succ() {
        return cell_localfile_succ;
    }

    public int getCell_oracale_succ() {
        return cell_oracale_succ;
    }

    public int getCell_get_error() {
        return cell_get_error;
    }

    public int getCell_get_dim_cell_col_size() {
        return dim_cell_col_size;
    }

}
