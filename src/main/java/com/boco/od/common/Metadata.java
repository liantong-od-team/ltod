package com.boco.od.common;
import java.io.*;
import java.util.*;
/**
 * Created by ranhualin on 2015/7/30.
 */
public class Metadata {
    Properties prop;
    InputStream inputStream;
    // 返回配置对象
    public   Metadata(String filePath) {
        prop = new Properties();
        try {
            inputStream = Metadata.class
                    .getResourceAsStream(filePath);
            prop.load(inputStream);
        } catch (Exception e) {
            System.out.println("init properties error: " + filePath);
            e.printStackTrace();
        }finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    System.out.println("can't close the inputstream!");
                    e.printStackTrace();
                }
            }
        }
    }

    // 获取相应的配置文件信息
    public String getValue(String key) {
        if (prop.containsKey(key)) {
            return prop.getProperty(key);
        } else {
            return "";
        }
    }
    public int getIntValue(String key) {
        if (prop.containsKey(key)) {
            return Integer.parseInt(prop.getProperty(key));
        } else {
            return -1;
        }
    }
    public static void main(String [] args){
        Metadata meta= new Metadata(Constants.STAGE_PROP_PATH);
        System.out.println(meta.getValue("column.size"));
    }
}
