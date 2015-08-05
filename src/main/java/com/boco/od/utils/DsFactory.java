package com.boco.od.utils;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
    static String driver, url, username, password;

    private static DsFactory dsins = new DsFactory();

    private DsFactory() {
        Properties prop = ConfigUtils.getConfig(PROPERTIES_PATH);
        driver = prop.getProperty("oracle.driver", EMPTY);
        url = prop.getProperty("oracle.url", EMPTY);
        username = prop.getProperty("oracle.username", EMPTY);
        password = prop.getProperty("oracle.password", EMPTY);


        Properties p = new Properties();
        p.setProperty("driverClassName", driver);
        p.setProperty("url", url);
        p.setProperty("username", username);
        p.setProperty("password", password);
        p.setProperty("initialSize", "1");
        p.setProperty("minIdle", "1");
        p.setProperty("maxActive", "10");

        System.out.println("driverClassName=" + driver);
        System.out.println("url=" + url);
        System.out.println("username=" + username);
        System.out.println("password=" + password);
        try {
            ds = createDataSource(p);
            jdbc = new NamedParameterJdbcTemplate(ds);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static DsFactory getinstance() {
        return dsins;
    }


    public String queryProvince(String longitude, String latitude) {

        Map<String, String> paramArr = new HashMap<String, String>();
        paramArr.put("lng", longitude);
        paramArr.put("lat", latitude);
        String r = jdbc.queryForObject("select Get_Prov_Country(:lng, :lat, 'prov') prov  FROM dual", paramArr, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int i) throws SQLException {
                String s = rs.getString("prov");
                return s;
            }
        });
        return r;
    }

    public String queryCountry(String longitude, String latitude) {

        Map<String, String> paramArr = new HashMap<String, String>();
        paramArr.put("lng", longitude);
        paramArr.put("lat", latitude);
        String r = jdbc.queryForObject("select Get_Prov_Country(:lng, :lat, 'country') country FROM dual", paramArr, new RowMapper<String>(){
            @Override
            public String mapRow(ResultSet rs, int i) throws SQLException {
                String s = rs.getString("country");
                return s;
            }
        });

        return r;

    }

    public String[] queryAll(String longitude, String latitude) {
        String p = queryProvince(longitude, latitude);
        String c = queryCountry(longitude, latitude);
        return new String[]{p, c};
    }


    public static void main(String[] args) {
        String lng = "116.372639";
        String lat = "39.924627";
        System.out.println(DsFactory.getinstance().queryProvince(lng, lat));
        System.out.println(DsFactory.getinstance().queryCountry(lng, lat));
        for(String s: DsFactory.getinstance().queryAll(lng, lat)){
            System.out.println(s);
        }



    }

}
