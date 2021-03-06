package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class EtlTypeConvert extends PageNum{
	
	@ExcelColumn("srcDbType")
	private String srcDbType;
	@ExcelColumn("srcColumnType")
	private String srcColumnType;
	@ExcelColumn("tgtDbType")
	private String tgtDbType;
	@ExcelColumn("tgtColumnType")
	private String tgtColumnType;
	@ExcelColumn("tgtColumnBigType")
	private String tgtColumnBigType;
	@ExcelColumn("tgtColumnLength")
	private String tgtColumnLength;
	@ExcelColumn("tgtColumnDefault")
	private String tgtColumnDefault;
	@ExcelColumn("tgtColumnFormat")
	private String tgtColumnFormat;
	@ExcelColumn("convertMode")
	private String convertMode;
	
	
	public EtlTypeConvert() {
		super();
	}

	public EtlTypeConvert(String srcDbType, String srcColumnType, String tgtDbType, String tgtColumnType,
			String tgtColumnBigType, String tgtColumnLength, String tgtColumnDefault, String tgtColumnFormat,
			String convertMode) {
		super();
		this.srcDbType = srcDbType;
		this.srcColumnType = srcColumnType;
		this.tgtDbType = tgtDbType;
		this.tgtColumnType = tgtColumnType;
		this.tgtColumnBigType = tgtColumnBigType;
		this.tgtColumnLength = tgtColumnLength;
		this.tgtColumnDefault = tgtColumnDefault;
		this.tgtColumnFormat = tgtColumnFormat;
		this.convertMode = convertMode;
	}

	public String getSrcDbType() {
		return srcDbType;
	}

	public void setSrcDbType(String srcDbType) {
		this.srcDbType = srcDbType;
	}

	public String getSrcColumnType() {
		return srcColumnType;
	}

	public void setSrcColumnType(String srcColumnType) {
		this.srcColumnType = srcColumnType;
	}

	public String getTgtDbType() {
		return tgtDbType;
	}

	public void setTgtDbType(String tgtDbType) {
		this.tgtDbType = tgtDbType;
	}

	public String getTgtColumnType() {
		return tgtColumnType;
	}

	public void setTgtColumnType(String tgtColumnType) {
		this.tgtColumnType = tgtColumnType;
	}

	public String getTgtColumnBigType() {
		return tgtColumnBigType;
	}

	public void setTgtColumnBigType(String tgtColumnBigType) {
		this.tgtColumnBigType = tgtColumnBigType;
	}

	public String getTgtColumnLength() {
		return tgtColumnLength;
	}

	public void setTgtColumnLength(String tgtColumnLength) {
		this.tgtColumnLength = tgtColumnLength;
	}

	public String getTgtColumnDefault() {
		return tgtColumnDefault;
	}

	public void setTgtColumnDefault(String tgtColumnDefault) {
		this.tgtColumnDefault = tgtColumnDefault;
	}

	public String getTgtColumnFormat() {
		return tgtColumnFormat;
	}

	public void setTgtColumnFormat(String tgtColumnFormat) {
		this.tgtColumnFormat = tgtColumnFormat;
	}

	public String getConvertMode() {
		return convertMode;
	}

	public void setConvertMode(String convertMode) {
		this.convertMode = convertMode;
	}

	@Override
	public String toString() {
		return "EtlTypeConvert [srcDbType=" + srcDbType + ", srcColumnType=" + srcColumnType + ", tgtDbType="
				+ tgtDbType + ", tgtColumnType=" + tgtColumnType + ", tgtColumnBigType=" + tgtColumnBigType
				+ ", tgtColumnLength=" + tgtColumnLength + ", tgtColumnDefault=" + tgtColumnDefault
				+ ", tgtColumnFormat=" + tgtColumnFormat + ", convertMode=" + convertMode + "]";
	}
}
