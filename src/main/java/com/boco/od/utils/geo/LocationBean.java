package com.boco.od.utils.geo;

import java.util.List;

public class LocationBean {
	
	public LocationBean(String name){
		this.name = name;
	}

	private String name;
	// 省市=1 区县=2
	private int areaType;

	// 坐标组
	private List<List<String>> locations;

	// 形成的面的类型 3 or 7
	private int geoType;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAreaType() {
		return areaType;
	}

	public void setAreaType(int areaType) {
		this.areaType = areaType;
	}

	

	public List<List<String>> getLocations() {
		return locations;
	}

	public void setLocations(List<List<String>> locations) {
		this.locations = locations;
	}

	public int getGeoType() {
		return geoType;
	}

	public void setGeoType(int geoType) {
		this.geoType = geoType;
	}

}
