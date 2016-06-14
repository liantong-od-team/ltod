package com.boco.od.utils.geo;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.geotools.geometry.jts.JTSFactoryFinder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class GisTool {
    private static GeometryFactory geometryFactory = JTSFactoryFinder
            .getGeometryFactory(null);

    public static final String PROVINCE_FILE_PATH = "/conf/t_province.ctl";
    public static final String COUNTRY_FILE_PATH = "/conf/t_country.ctl";
    public static final String FT_FILE_PATH = "/conf/st_r_xa_fixed.ctl";
    public static final String XMSQ_FILE_PATH = "/conf/t_xmsq_fixed.ctl";
    public static final String TEST_FILE_PATH = "/conf/test.ctl";

    private static List<LocationBean> provinceCache = new ArrayList<LocationBean>();
    private static List<LocationBean> regionCache = new ArrayList<LocationBean>();
    private static List<LocationBean> gisCache = new ArrayList<LocationBean>();

    private static Pattern pattern = Pattern.compile("\\|");

    private static GisTool ins = new GisTool();

    private BufferedReader br;

    private GisTool() {

    }

    public static GisTool getInstance() {
        return ins;
    }

    public void load(String filePath) {
        // // 省市=1 区县=2
        int areaType = filePath.equalsIgnoreCase(PROVINCE_FILE_PATH) ? 1 : 2;
        InputStream is = GisTool.class.getResourceAsStream(filePath);
        br = new BufferedReader(new InputStreamReader(is));
        String line;
        LocationBean bean = null;
        int skip = 0;
        String[] arr;
        String firstData = "";
        boolean hasReadFirstData = false;
        List<String> dataList = null;
        List<List<String>> polygonList = null;
        String data = "";
        try {
            while (null != (line = br.readLine())) {
                if (skip > 0) {
                    if (line.startsWith("#") && skip == 2) {
                        bean.setGeoType(Integer.parseInt(pattern.split(line
                                .substring(1))[0]));
                    }
                    if (line.startsWith("#") && skip == 1) {
                        String[] arrtmp = pattern.split(line.substring(1, line.length() - 1));
                        bean.setLocation_num(arrtmp.length / 3);
                        List<String> tmp = new ArrayList<String>();
                        for (int i = 0; i < arrtmp.length / 3; i += 3) {
                            tmp.add(arrtmp[i] + "|" + arrtmp[i + 1] + "|" + arrtmp[i + 2]);
                        }
                        bean.setLocation_desc(tmp);
                    }
                    skip--;
                    continue;
                }

                // 每个地市开始的第一行
                if (!line.startsWith("#")) {
                    //
                    // reset tmp params
                    firstData = "";
                    hasReadFirstData = false;
                    //
                    // init 4 per data section
                    bean = new LocationBean(line.substring(line.indexOf("|") + 1));
                    polygonList = new ArrayList<List<String>>();
                    dataList = new ArrayList<String>();
                    polygonList.add(dataList);
                    bean.setLocations(polygonList);
                    //
                    //
                    if (areaType == 1) {
                        provinceCache.add(bean);
                        bean.setAreaType(1);
                    } else {
                        regionCache.add(bean);
                        bean.setAreaType(2);
                    }
                    // 设置第一行与数据行之间的非数据行 个数
                    skip = 2;
                } else {
                    // 以#开头的都是数据行

                    arr = pattern.split(line.substring(1));

                    for (int i = 0; i <= (arr.length / 2 - 1) * 2; i += 2) {
                        data = arr[i] + " " + arr[i + 1];
                        /*if("118.101276 24.525856".equals(data)){
							System.out.println("here");
						}*/
                        /**
                         * 如果已读取过首记录，且又读取到了相同点，则认为形成一个闭合的polygon
                         */
						/*
						if (hasReadFirstData
								&& firstData.equalsIgnoreCase(data)) {
							dataList.add(data);
							dataList = new ArrayList<String>();
							polygonList.add(dataList);
						} else {
							dataList.add(data);
						}

						if (!hasReadFirstData) {
							firstData = data;
                            hasReadFirstData = true;
						}
						*/

                        dataList.add(data);
                        if (!hasReadFirstData) {
                            firstData = data;
                            hasReadFirstData = true;
                        } else {
                            if (firstData.equalsIgnoreCase(data)) {
                                dataList = new ArrayList<String>();
                                polygonList.add(dataList);
                                hasReadFirstData = false;
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void load2(String filePath) {
        // // 省市=1 区县=2
        int areaType = filePath.equalsIgnoreCase(PROVINCE_FILE_PATH) ? 1 : 2;
        InputStream is = GisTool.class.getResourceAsStream(filePath);
        br = new BufferedReader(new InputStreamReader(is));

        LocationBean bean = null;
        int skip = 0;
        String line;
        String[] arr;
        String firstData = "";
        boolean hasReadFirstData = false;
        List<String> dataList = null;
        List<List<String>> polygonList = null;
        List<String> alllocList = null;
        String data = "";
        try {
            line = br.readLine();
            while (!(null == line && alllocList.size() == 0)) {
                /*if (line.startsWith("#118.106263|24.527310")) {
                    System.out.println();
                }*/

                if (alllocList != null && alllocList.size() > 0) {
                    //get cfg
//					int area_num = bean.getLocation_num();
                    int descsize = bean.getLocation_desc().size();
                    int startPos = 0, endpos;
                    String desc;
                    List oneList;
                    for (int i = 0; i < descsize; i++) {
                        //能取到end 位置
                        if (i + 1 < descsize) {
                            desc = bean.getLocation_desc().get(i + 1);
                            endpos = Integer.parseInt(desc.substring(0, desc.indexOf("|")));
                            oneList = alllocList.subList(startPos, (endpos - 1) / 2);

                            startPos = (endpos - 1) / 2;
                        } else {
                            //读取到完毕
                            oneList = alllocList.subList(startPos, alllocList.size());
                        }
                        bean.getLocations().add(oneList);
                    }
                }
                //最后一条记录
                if(line==null) break;

                // 每个地市开始的第一行
                if (!line.startsWith("#")) {
                    //init bean
                    alllocList = new ArrayList<String>();
                    bean = new LocationBean(line.substring(line.indexOf("|") + 1));
                    polygonList = new ArrayList<List<String>>();

//					dataList = new ArrayList<String>();
//					polygonList.add(dataList);

                    bean.setLocations(polygonList);
                    gisCache.add(bean);
                    bean.setAreaType(0);
                    //set cfg
                    String typeline = br.readLine();
                    bean.setGeoType(Integer.parseInt(pattern.split(typeline
                            .substring(1))[0]));

                    String descline = br.readLine();
                    String[] arrtmp = pattern.split(descline.substring(1, descline.length() - 1));
                    bean.setLocation_num(arrtmp.length / 3);
                    List<String> tmp = new ArrayList<String>();
                    for (int i = 0; i < arrtmp.length; i += 3) {
                        tmp.add(arrtmp[i] + "|" + arrtmp[i + 1] + "|" + arrtmp[i + 2]);
                    }
                    bean.setLocation_desc(tmp);
                    //goto next
                    line = br.readLine();
                } else {
                    // 以#开头的都是数据行
                    do {
                        arr = pattern.split(line.substring(1));
                        for (int i = 0; i <= (arr.length / 2 - 1) * 2; i += 2) {
                            data = arr[i] + " " + arr[i + 1];
                            alllocList.add(data);
                        }
                    } while (null != (line = br.readLine()) && line.startsWith("#"));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String getGis(String lng, String lat) {
        return getloc(lng, lat, 0);
    }

    public String getCountry(String lng, String lat) {
        return getloc(lng, lat, 2);
    }

    public String getProvince(String lng, String lat) {
        return getloc(lng, lat, 1);
    }

    private String getloc(String lng, String lat, int areaType) {
        List<LocationBean> cache = null;
        if (areaType == 0) {
            cache = gisCache;
        } else if (areaType == 1) {
            cache = provinceCache;
        } else {
            cache = regionCache;
        }
        WKTReader reader = new WKTReader(geometryFactory);
        String loc_name = "";
        try {
            Point point = (Point) reader
                    .read("POINT (" + lng + " " + lat + ")");

            // 有经纬度的polygon个数
            int loc_size;
            Polygon polygon;
            //
            //循环区县
            //
            outter:
            for (LocationBean region : cache) {
                    boolean isInner = false;
                    boolean getIt = false;
                    for(int i=0;i<region.getLocations().size();i++){
                        String desc = region.getLocation_desc().get(i);
                        String type = desc.split("\\|")[1];

                        List<String>dataList =region.getLocations().get(i);
                        polygon = (Polygon) reader.read("POLYGON (("
                                + Joiner.on(",").join(dataList) + "))");
                        if (polygon.covers(point)) {
                            if("2003".equals(type.trim())){
                                isInner = true;
                            }
                            loc_name = region.getName();
                            getIt = true;
//                            System.out.println("desc="+desc+" "+new String(loc_name.getBytes(),"utf-8"));
                        }
                    }
                    if(getIt==true&& isInner==false){
                        break outter;
                    }
            }
        } catch (ParseException e1) {
            e1.printStackTrace();
        } /*catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }*/
        return loc_name;
    }

    private void showProv() {
        for (LocationBean bean : provinceCache) {
            System.out.println(bean.getName());
            System.out.println(bean.getGeoType());
            System.out.println("polygon size = " + bean.getLocations().size());
            for (int i = 0; i < bean.getLocations().size(); i++) {
                int locnums = bean.getLocations().get(i).size();
                System.out.println("第 " + i + " 个 polygon 的点有 " + locnums
                        + " 个");
            }

        }
    }

    private void showRegion() {
        for (LocationBean bean : regionCache) {
            if (bean.getGeoType() == 7) {
                System.out.println(bean.getName());
                System.out.println(bean.getGeoType());

                System.out.println("polygon size = "
                        + bean.getLocations().size());
                for (int i = 0; i < bean.getLocations().size(); i++) {
                    int locnums = bean.getLocations().get(i).size();
                    System.out.println("第 " + i + " 个 polygon 的点有 " + locnums
                            + " 个");
                }
            }

        }
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        //116.570624,40.069865 北京
//		String lng = "116.616186";
//		String lat = "40.058766";
        //116.717515,39.528602 廊坊 河北
//		String lng = "116.717515";
//		String lat = "39.528602";
        //117.163649,39.069382 天津
//		String lng = "117.163649";
//		String lat = "39.069382";

        //117.681098,40.556761 鹰手营子矿区
//		String lng = "117.681098";
//		String lat = "40.556761";

        String lng ="116.23501";// "118.046226"; //"118.100501"; //		"115.8173589";
        String lat ="39.89701";// "24.570831"; //"24.533257"; // "39.5072942";
//		GisTool.getInstance().load(PROVINCE_FILE_PATH);
//		GisTool.getInstance().load(COUNTRY_FILE_PATH);
//		GisTool.getInstance().load(FT_FILE_PATH);
//		GisTool.getInstance().load2(XMSQ_FILE_PATH);
        GisTool.getInstance().load2(FT_FILE_PATH);
//		 System.out.println("查询所在省市= "+new String(GisTool.getInstance().getProvince(lng, lat).getBytes(),"utf-8"));
        System.out.println("查询所在区县= " + new String(GisTool.getInstance().getGis(lng, lat).getBytes(), "utf-8"));



		/*

		 GisTool.getInstance().showProv();
        long d1 = System.currentTimeMillis();

        for(int i=0;i<100;i++){
            GisTool.getInstance().getCountry(lng, lat);
        }
        System.out.println("cost = "+(System.currentTimeMillis()-d1));

//		GisTool.getInstance().load(COUNTRY_FILE_PATH);
//		GisTool.getInstance().showRegion();
		System.out.println("end");
*/
    }
}
