package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class EtlTypeConvertCopy extends PageNum{
	@ExcelColumn("srcDbType")
	private String srcDbType1;
	@ExcelColumn("srcColumnType")
	private String srcColumnType1;
	@ExcelColumn("tgtDbType")
	private String tgtDbType1;
	@ExcelColumn("tgtColumnType")
	private String tgtColumnType1;
	@ExcelColumn("tgtColumnBigType")
	private String tgtColumnBigType1;
	@ExcelColumn("tgtColumnLength")
	private String tgtColumnLength1;
	@ExcelColumn("tgtColumnDefault")
	private String tgtColumnDefault1;
	@ExcelColumn("tgtColumnFormat")
	private String tgtColumnFormat1;
	@ExcelColumn("convertMode")
	private String convertMode1;
	
	
	public EtlTypeConvertCopy() {
		super();
	}


	public EtlTypeConvertCopy(String srcDbType1, String srcColumnType1, String tgtDbType1, String tgtColumnType1,
			String tgtColumnBigType1, String tgtColumnLength1, String tgtColumnDefault1, String tgtColumnFormat1,
			String convertMode1) {
		this.srcDbType1 = srcDbType1;
		this.srcColumnType1 = srcColumnType1;
		this.tgtDbType1 = tgtDbType1;
		this.tgtColumnType1 = tgtColumnType1;
		this.tgtColumnBigType1 = tgtColumnBigType1;
		this.tgtColumnLength1 = tgtColumnLength1;
		this.tgtColumnDefault1 = tgtColumnDefault1;
		this.tgtColumnFormat1 = tgtColumnFormat1;
		this.convertMode1 = convertMode1;
	}


	public String getSrcDbType1() {
		return srcDbType1;
	}


	public void setSrcDbType1(String srcDbType1) {
		this.srcDbType1 = srcDbType1;
	}


	public String getSrcColumnType1() {
		return srcColumnType1;
	}


	public void setSrcColumnType1(String srcColumnType1) {
		this.srcColumnType1 = srcColumnType1;
	}


	public String getTgtDbType1() {
		return tgtDbType1;
	}


	public void setTgtDbType1(String tgtDbType1) {
		this.tgtDbType1 = tgtDbType1;
	}


	public String getTgtColumnType1() {
		return tgtColumnType1;
	}


	public void setTgtColumnType1(String tgtColumnType1) {
		this.tgtColumnType1 = tgtColumnType1;
	}


	public String getTgtColumnBigType1() {
		return tgtColumnBigType1;
	}


	public void setTgtColumnBigType1(String tgtColumnBigType1) {
		this.tgtColumnBigType1 = tgtColumnBigType1;
	}


	public String getTgtColumnLength1() {
		return tgtColumnLength1;
	}


	public void setTgtColumnLength1(String tgtColumnLength1) {
		this.tgtColumnLength1 = tgtColumnLength1;
	}


	public String getTgtColumnDefault1() {
		return tgtColumnDefault1;
	}


	public void setTgtColumnDefault1(String tgtColumnDefault1) {
		this.tgtColumnDefault1 = tgtColumnDefault1;
	}


	public String getTgtColumnFormat1() {
		return tgtColumnFormat1;
	}


	public void setTgtColumnFormat1(String tgtColumnFormat1) {
		this.tgtColumnFormat1 = tgtColumnFormat1;
	}


	public String getConvertMode1() {
		return convertMode1;
	}


	public void setConvertMode1(String convertMode1) {
		this.convertMode1 = convertMode1;
	}

	@Override
	public String toString() {
		return "EtlTypeConvertCopy [srcDbType1=" + srcDbType1 + ", srcColumnType1=" + srcColumnType1 + ", tgtDbType1="
				+ tgtDbType1 + ", tgtColumnType1=" + tgtColumnType1 + ", tgtColumnBigType1=" + tgtColumnBigType1
				+ ", tgtColumnLength1=" + tgtColumnLength1 + ", tgtColumnDefault1=" + tgtColumnDefault1
				+ ", tgtColumnFormat1=" + tgtColumnFormat1 + ", convertMode1=" + convertMode1 + "]";
	}
}
