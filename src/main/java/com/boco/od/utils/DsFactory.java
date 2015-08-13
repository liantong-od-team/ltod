package com.boco.od.utils;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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




    public static void main(String[] args) {
        String lng = "116.372639";
        String lat = "39.924627";
        System.out.println("res = " + DsFactory.getinstance().queryProvince(lng, lat));
        System.out.println("res = " + DsFactory.getinstance().queryCountry(lng, lat));
        for (String s : DsFactory.getinstance().queryAll(lng, lat)) {
            System.out.println(s);
        }


    }

}
