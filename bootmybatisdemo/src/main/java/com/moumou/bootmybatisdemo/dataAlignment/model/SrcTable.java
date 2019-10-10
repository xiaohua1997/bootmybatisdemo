package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class SrcTable {
	
	@ExcelColumn("sys")
    private String sys;
	@ExcelColumn("dbSid")
    private String dbSid;
	@ExcelColumn("tableSchema")
    private String tableSchema;
	@ExcelColumn("tableName")
    private String tableName;
	@ExcelColumn("tableCnName")
    private String tableCnName;
	@ExcelColumn("incCdt")
    private String incCdt;
	@ExcelColumn("ifMark")
    private String ifMark;
	@ExcelColumn("tableType")
    private String tableType;
	@ExcelColumn("templateCode")
    private String templateCode;
	@ExcelColumn("isPutToEtldb")
    private String isPutToEtldb;

	
    public SrcTable() {
		super();
	}

	public SrcTable(String sys, String dbSid, String tableSchema, String tableName, String tableCnName, String incCdt,
			String ifMark, String tableType, String templateCode, String isPutToEtldb) {
		super();
		this.sys = sys;
		this.dbSid = dbSid;
		this.tableSchema = tableSchema;
		this.tableName = tableName;
		this.tableCnName = tableCnName;
		this.incCdt = incCdt;
		this.ifMark = ifMark;
		this.tableType = tableType;
		this.templateCode = templateCode;
		this.isPutToEtldb = isPutToEtldb;
	}

	public String getSys() {
        return sys;
    }

    public void setSys(String sys) {
        this.sys = sys;
    }

    public String getDbSid() {
        return dbSid;
    }

    public void setDbSid(String dbSid) {
        this.dbSid = dbSid;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableCnName() {
        return tableCnName;
    }

    public void setTableCnName(String tableCnName) {
        this.tableCnName = tableCnName;
    }

    public String getIncCdt() {
        return incCdt;
    }

    public void setIncCdt(String incCdt) {
        this.incCdt = incCdt;
    }

    public String getIfMark() {
        return ifMark;
    }

    public void setIfMark(String ifMark) {
        this.ifMark = ifMark;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getTemplateCode() {
        return templateCode;
    }

    public void setTemplateCode(String templateCode) {
        this.templateCode = templateCode;
    }

    public String getIsPutToEtldb() {
        return isPutToEtldb;
    }

    public void setIsPutToEtldb(String isPutToEtldb) {
        this.isPutToEtldb = isPutToEtldb;
    }

    @Override
    public String toString() {
        return "SrcTable{" +
                "sys='" + sys + '\'' +
                ", dbSid='" + dbSid + '\'' +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableCnName='" + tableCnName + '\'' +
                ", incCdt='" + incCdt + '\'' +
                ", ifMark='" + ifMark + '\'' +
                ", tableType='" + tableType + '\'' +
                ", templateCode='" + templateCode + '\'' +
                ", isPutToEtldb='" + isPutToEtldb + '\'' +
                '}';
    }
}
