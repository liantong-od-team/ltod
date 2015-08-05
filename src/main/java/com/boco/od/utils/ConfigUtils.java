package com.boco.od.utils;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;


public class ConfigUtils {

    public static final Properties GLOBAL_PROPERTIES = getGlobalProperites();
    public static File file = null;

    /**
     * @return
     */
    public static Properties getGlobalProperites() {
//        return getConfig(getConfig(Constants.GLOBAL_PROP).getProperty("alias"));
        return new Properties();
    }

    /**
     * @param key
     * @return
     */
    public static String getGlobalValue(String key) {
        return GLOBAL_PROPERTIES.getProperty(key);
    }

    /**
     * @param filePath
     * @return
     */
    public static Properties getConfig(String filePath) {
        System.out.println("init properties: " + filePath);
        InputStream inputStream = null;
        Properties prop = null;
        try {
            inputStream = ConfigUtils.class.getResourceAsStream(filePath);
            prop = new Properties();
            prop.load(inputStream);
        } catch (Exception e) {
            System.out.println("init properties error: " + filePath);
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        if (null != prop) {
            ReadEL.replaceProp(prop);
        }
        return prop;
    }

    public static void main(String[] args) {
//		Properties prop = ConfigUtils.getConfig("/config/jdbc.properties");
        Properties prop = ConfigUtils.getConfig("/conf/metadatadef.properties");
        System.out.println(prop.getProperty("user"));
//		System.out.println(prop.getProperty("REDIS.PORT"));
//		System.out.println(getGlobalValue("DEBUG"));
    }
}
