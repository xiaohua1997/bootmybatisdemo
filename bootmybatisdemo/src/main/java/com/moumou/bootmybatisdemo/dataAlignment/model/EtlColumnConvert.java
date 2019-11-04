package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class EtlColumnConvert extends PageNum{
	
	@ExcelColumn("srcColumn")
	private String srcColumn;
	@ExcelColumn("tgtColumn")
	private String tgtColumn;
	@ExcelColumn("tableName")
	private String tableName;
	@ExcelColumn("sys")
	private String sys;
	
	
	public EtlColumnConvert() {
		super();
	}

	public EtlColumnConvert(String srcColumn, String tgtColumn, String tableName, String sys) {
		super();
		this.srcColumn = srcColumn;
		this.tgtColumn = tgtColumn;
		this.tableName = tableName;
		this.sys = sys;
	}

	public String getSrcColumn() {
		return srcColumn;
	}

	public void setSrcColumn(String srcColumn) {
		this.srcColumn = srcColumn;
	}

	public String getTgtColumn() {
		return tgtColumn;
	}

	public void setTgtColumn(String tgtColumn) {
		this.tgtColumn = tgtColumn;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSys() {
		return sys;
	}

	public void setSys(String sys) {
		this.sys = sys;
	}

	@Override
	public String toString() {
		return "EtlColumnConvert [srcColumn=" + srcColumn + ", tgtColumn=" + tgtColumn + ", tableName=" + tableName
				+ ", sys=" + sys + "]";
	}
}
