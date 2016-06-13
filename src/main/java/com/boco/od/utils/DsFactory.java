package com.boco.od.utils;

import oracle.sql.ARRAY;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.io.*;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.alibaba.druid.pool.DruidDataSourceFactory.createDataSource;

/**
 * Created by mars on 2015/8/4.
 */
public class DsFactory {

    private DataSource ds;
    private NamedParameterJdbcTemplate jdbc;
    private static final String PROPERTIES_PATH = "/conf/jdbc.properties";
    private static final String EMPTY = "";
    static String driver, url, username, password, initialSize, minIdle, maxActive;

    private static DsFactory dsins = new DsFactory();

    private DsFactory() {
        Properties prop = ConfigUtils.getConfig(PROPERTIES_PATH);
        driver = prop.getProperty("oracle.driver", EMPTY);
        url = prop.getProperty("oracle.url", EMPTY);
        username = prop.getProperty("oracle.username", EMPTY);
        password = prop.getProperty("oracle.password", EMPTY);
        initialSize = prop.getProperty("oracle.initial", EMPTY);
        minIdle = prop.getProperty("oracle.minIdle", EMPTY);
        maxActive = prop.getProperty("oracle.maxActive", EMPTY);

        Properties p = new Properties();
        p.setProperty("driverClassName", driver);
        p.setProperty("url", url);
        p.setProperty("username", username);
        p.setProperty("password", password);
        p.setProperty("initialSize", initialSize);
        p.setProperty("minIdle", minIdle);
        p.setProperty("maxActive", maxActive);

        System.out.println("driverClassName=" + driver);
        System.out.println("url=" + url);
        System.out.println("username=" + username);
        System.out.println("password=" + password);
        System.out.println("initialSize=" + initialSize);
        System.out.println("minIdle=" + minIdle);
        System.out.println("maxActive=" + maxActive);
        try {
            ds = createDataSource(p);
            jdbc = new NamedParameterJdbcTemplate(ds);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
        }

    }

    public static DsFactory getinstance() {
        return dsins;
    }


    private String sql_province = "select name from (select m.name,SDO_GEOM.RELATE(GEOM,'Contains', MDSYS.SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(%s, %s, 0), NULL, NULL), 0.5) status from GIS_PROVINCE m) where status = 'CONTAINS'";
    private String sql_country = "select name from (select m.name,SDO_GEOM.RELATE(GEOM,'Contains', MDSYS.SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(%s, %s, 0), NULL, NULL), 0.5) status from GIS_COUNTRY m) where status = 'CONTAINS'";

    private String sql_ft_tpl= "SELECT fn_getRegion(%s,%s) as name FROM dual";

    private boolean checkParams(String longitude, String latitude) {
        double dlng, dlat;
        try {
            dlng = Double.valueOf(longitude);
            dlat = Double.valueOf(latitude);
        } catch (Exception ex) {
            return false;
        }
        if (dlng <= 180 && dlng >= -180 && dlat <= 90 && dlat >= -90) {
            return true;
        } else {
            return false;
        }
    }

    public String queryProvince(String longitude, String latitude) {
        if(!checkParams(longitude,latitude)) {
            System.out.println("check failed.");
            return "";
        }
        String provSql = String.format(sql_province, ":lng", ":lat");
        Map<String, Double> paramArr = new HashMap<String, Double>();
        paramArr.put("lng", Double.valueOf(longitude));
        paramArr.put("lat", Double.valueOf(latitude));
//        System.out.println("provSql=" + provSql);
        List<Map<String,Object>> lst = jdbc.queryForList(provSql, paramArr);
        if (lst.size() > 0) {
            return String.valueOf(lst.get(0).get("name"));
        } else {
            return "";
        }
    }

    public Map<BigDecimal,String[]> queryBySql(String sql){
        List<Map<String,Object>> lst = jdbc.queryForList(sql, new HashMap());
        BigDecimal id,type;
        Map<BigDecimal,String[]> m = new HashMap();
        /*oracle.sql.ARRAY*/ java.sql.Array ele_arr;
        BigDecimal[] ele_info;
        for(int i=0;i<lst.size();i++){
            id = (BigDecimal)lst.get(i).get("OBJECTID");
            type = (BigDecimal)lst.get(i).get("GEOM.SDO_GTYPE");
            ele_arr= (java.sql.Array )lst.get(i).get("GEOM.SDO_ELEM_INFO") ;
            try {
                ele_info = (BigDecimal[])ele_arr.getArray();
                String s="#";
                for (BigDecimal b: ele_info){
                    s=s+b+"|";
                }
                if("490".equals(id)){
                    System.out.println("123");
                }
                System.out.println(id+"  "+type+"   "+s+"/");
                m.put(id,new String[]{type+"",s});
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return m;
    }

    public String queryft(String longitude, String latitude) throws UnsupportedEncodingException {
        if(!checkParams(longitude,latitude)){
            System.out.println("check failed.");
            return "";
        }
        String ftSql = String.format(sql_ft_tpl, ":lng", ":lat");
        Map<String, Double> paramArr = new HashMap<String, Double>();
        paramArr.put("lng", Double.valueOf(longitude));
        paramArr.put("lat", Double.valueOf(latitude));
//        System.out.println("countrySql=" + countrySql);
        List<Map<String,Object>> lst = jdbc.queryForList(ftSql, paramArr);
        if (lst.size() > 0) {
            Object str = lst.get(0).get("name");
//            return new String (String.valueOf(str).getBytes("iso-8859-1"),"GBK");
            return String.valueOf(str);
        } else {
            return "";
        }
    }


    public String queryCountry(String longitude, String latitude) {
        if(!checkParams(longitude,latitude)){
            System.out.println("check failed.");
            return "";
        }
        String countrySql = String.format(sql_country, ":lng", ":lat");
        Map<String, Double> paramArr = new HashMap<String, Double>();
        paramArr.put("lng", Double.valueOf(longitude));
        paramArr.put("lat", Double.valueOf(latitude));
//        System.out.println("countrySql=" + countrySql);
        List<Map<String,Object>> lst = jdbc.queryForList(countrySql, paramArr);
        if (lst.size() > 0) {
            return String.valueOf(lst.get(0).get("name"));
        } else {
            return "";
        }
    }

    public String[] queryAll(String longitude, String latitude) {
        String p = queryProvince(longitude, latitude);
        String c = queryCountry(longitude, latitude);
        return new String[]{p, c};
    }


/*



    public String queryProvince0(String longitude, String latitude) {

        String provSql = String.format(sql_province, ":lng", ":lat");
        Map<String, String> paramArr = new HashMap<String, String>();
        paramArr.put("lng", longitude);
        paramArr.put("lat", latitude);

        String r = jdbc.queryForObject(provSql, paramArr, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int i) throws SQLException {
                String s = rs.getString("name");
                return s;
            }
        });
        return r;
    }

    public String queryCountry0(String longitude, String latitude) {
        String countrySql = String.format(sql_country, ":lng", ":lat");
        Map<String, String> paramArr = new HashMap<String, String>();
        paramArr.put("lng", longitude);
        paramArr.put("lat", latitude);
        String r = jdbc.queryForObject(countrySql, paramArr, new RowMapper<String>(){
            @Override
            public String mapRow(ResultSet rs, int i) throws SQLException {
                String s = rs.getString("name");
                return s;
            }
        });

        return r;

    }
    public String[] queryAll0(String longitude, String latitude) {
        String p = queryProvince0(longitude, latitude);
        String c = queryCountry0(longitude, latitude);
        return new String[]{p, c};
    }
*/

    public static List<String> readDim() throws IOException {
        List<String> lnglat = new ArrayList<>();
        FileInputStream inputStream = null;
        String path= "E:\\workspace\\idea\\ltod\\src\\main\\resources\\conf\\dim.csv";
        Scanner sc = null;
        String line;
        int total = 0;
        try {
            File file = new File(path);
//            inputStream = new FileInputStream(path);
            sc = new Scanner(file, "UTF-8");
            while (sc.hasNextLine()) {
                total++;
                 line = sc.nextLine();
                lnglat.add(line);
            }
            System.out.println("total= "+total);
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
        return lnglat;
    }


    public static void main(String[] args) throws UnsupportedEncodingException {
        FileWriter fw =null;
        try {
            String[] arr;
            String r;
            String path = "e:/jwd.csv";
            fw =  new FileWriter(new String(path));
            int i=0;
            for(String s : DsFactory.readDim()){
                arr = s.split(",");
                r = DsFactory.getinstance().queryft(arr[0], arr[1]);
                fw.write(arr[0]+","+arr[1]+","+r+"\n");
                fw.flush();
                System.out.println(i++ +" "+ r);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        /*
        System.setProperty("NLS_LANG","AMERICAN_AMERICA.AL32UTF8");
        //lugouqiao
        String lng = "116.303253";
        String lat = "39.869436";
        /*System.out.println("res = " + DsFactory.getinstance().queryProvince(lng, lat));
        System.out.println("res = " + DsFactory.getinstance().queryCountry(lng, lat));
        for (String s : DsFactory.getinstance().queryAll(lng, lat)) {
            System.out.println(s);
        }*/
        /*
        long d1 = System.currentTimeMillis();
        for(int i=0;i<1;i++){
            String s = DsFactory.getinstance().queryft(lng, lat);
            System.out.println(s);
            System.out.println(new String(s.getBytes("utf-8"), "gbk"));
        }
        System.out.println("cost = "+(System.currentTimeMillis()-d1));*/

    }

}
