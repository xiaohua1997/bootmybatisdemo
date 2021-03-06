package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class SrcTableNameConvert extends PageNum{
	
	@ExcelColumn("srcTableName")
	private String srcTableName;
	@ExcelColumn("tgtTableName")
	private String tgtTableName;
	@ExcelColumn("sys")
	private String sys;
	@ExcelColumn("remark")
	private String remark;
	
	
	public SrcTableNameConvert() {
		super();
	}
	public SrcTableNameConvert(String srcTableName, String tgtTableName, String sys, String remark) {
		super();
		this.srcTableName = srcTableName;
		this.tgtTableName = tgtTableName;
		this.sys = sys;
		this.remark = remark;
	}
	public String getSrcTableName() {
		return srcTableName;
	}
	public void setSrcTableName(String srcTableName) {
		this.srcTableName = srcTableName;
	}
	public String getTgtTableName() {
		return tgtTableName;
	}
	public void setTgtTableName(String tgtTableName) {
		this.tgtTableName = tgtTableName;
	}
	public String getSys() {
		return sys;
	}
	public void setSys(String sys) {
		this.sys = sys;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	
	@Override
	public String toString() {
		return "SrcTableNameConvert [srcTableName=" + srcTableName + ", tgtTableName=" + tgtTableName + ", sys=" + sys
				+ ", remark=" + remark + "]";
	}
}
