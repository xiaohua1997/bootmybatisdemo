package com.moumou.bootmybatisdemo.dataAlignment.model;

public class Hive {
	
	private String yewu;
	private String mapping;
	
	public Hive() {
		super();
	}

	public Hive(String yewu, String mapping) {
		super();
		this.yewu = yewu;
		this.mapping = mapping;
	}

	public String getYewu() {
		return yewu;
	}

	public void setYewu(String yewu) {
		this.yewu = yewu;
	}

	public String getMapping() {
		return mapping;
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
	}

	@Override
	public String toString() {
		return "Hive [yewu=" + yewu + ", mapping=" + mapping + "]";
	}
}
