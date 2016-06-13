package com.boco.od.utils.geo;

import com.boco.od.utils.DsFactory;

import java.io.*;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Created by mars on 2016/1/21.
 */
public class FixGeoData {

    public static final String ORG_FILE_PATH = "/conf/ST_R_XA.ctl";
    public static final String NEW_FILE_PATH = "./st_r_xa_fixed.ctl";

    private BufferedReader br;
    private BufferedWriter bw;

    /**
     *
     * @return
     */
    public Map<BigDecimal, String[]> readGeoInfo() {
//        String sql = "SELECT t.objectid,t.geom.SDO_GTYPE,t.geom.SDO_ELEM_INFO FROM T_XMSQ t ORDER BY t.objectid";
        String sql = "SELECT t.userid as objectid,t.geom.SDO_GTYPE,t.geom.SDO_ELEM_INFO FROM ST_R_XA t ORDER BY t.userid";
        return DsFactory.getinstance().queryBySql(sql);
    }

    public void rewritectl(String from, String to) throws IOException {
        InputStream is = FixGeoData.class.getResourceAsStream(from);
        br = new BufferedReader(new InputStreamReader(is));
        bw = new BufferedWriter(new FileWriter(new File(to)));
        String line;
        int id;
        StringBuilder sb = new StringBuilder();
        Map<BigDecimal, String[]> geoinfoMap = readGeoInfo();
        try {
            while (null != (line = br.readLine())) {
                if (!line.startsWith("#")) {
                    sb.setLength(0);
                    sb.append(line+"\n");
                    id=Integer.parseInt((line.split("\\|")[0]).trim());
                    String[] ele_type_desc = geoinfoMap.get(new BigDecimal(id));
                    //add type
                    line = br.readLine();
                    sb.append("#"+ele_type_desc[0]+"|"+line.split("\\|")[1]+"|"+"\n");
                    //add ele_array
                    do{
                        //read all array no matter on one line or mutipl
                        line = br.readLine();
                    }while(!line.endsWith("/"));
                    sb.append(ele_type_desc[1]+"\n");
                    bw.write(sb.toString());
                }else{
                    bw.write(line+"\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            bw.flush();
            bw.close();
            br.close();
        }
    }


    public static void main(String[] args) {
//        String sql = "SELECT t.objectid,t.geom.SDO_GTYPE,t.geom.SDO_ELEM_INFO FROM T_XMSQ t ORDER BY t.objectid";
//        DsFactory.getinstance().queryBySql(sql);
        FixGeoData fix = new FixGeoData();
        try {
            fix.rewritectl(ORG_FILE_PATH,NEW_FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
