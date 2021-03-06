package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class SrcColumn extends PageNum{
	
	@ExcelColumn("sys")
    private String sys;
	@ExcelColumn("dbSid")
    private String dbSid;
	@ExcelColumn("dbSchema")
    private String dbSchema;
	@ExcelColumn("tableName")
    private String tableName;
	@ExcelColumn("columnId")
    private String columnId;
	@ExcelColumn("columnName")
    private String columnName;
	@ExcelColumn("columnType")
    private String columnType;
	@ExcelColumn("columnCnName")
    private String columnCnName;
	@ExcelColumn("isPk")
    private String isPk;
	@ExcelColumn("notNull")
    private String notNull;
	@ExcelColumn("defaultValue")
    private String defaultValue;
	@ExcelColumn("isDk")
    private String isDk;
	@ExcelColumn("breakFlag")
    private String breakFlag;

    public SrcColumn() {
		super();
	}

	public SrcColumn(String sys, String dbSid, String dbSchema, String tableName, String columnId, String columnName,
			String columnType, String columnCnName, String isPk, String notNull, String defaultValue, String isDk,
			String breakFlag) {
		super();
		this.sys = sys;
		this.dbSid = dbSid;
		this.dbSchema = dbSchema;
		this.tableName = tableName;
		this.columnId = columnId;
		this.columnName = columnName;
		this.columnType = columnType;
		this.columnCnName = columnCnName;
		this.isPk = isPk;
		this.notNull = notNull;
		this.defaultValue = defaultValue;
		this.isDk = isDk;
		this.breakFlag = breakFlag;
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

    public String getDbSchema() {
        return dbSchema;
    }

    public void setDbSchema(String dbSchema) {
        this.dbSchema = dbSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnCnName() {
        return columnCnName;
    }

    public void setColumnCnName(String columnCnName) {
        this.columnCnName = columnCnName;
    }

    public String getIsPk() {
        return isPk;
    }

    public void setIsPk(String isPk) {
        this.isPk = isPk;
    }

    public String getNotNull() {
        return notNull;
    }

    public void setNotNull(String notNull) {
        this.notNull = notNull;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getIsDk() {
        return isDk;
    }

    public void setIsDk(String isDk) {
        this.isDk = isDk;
    }

    public String getBreakFlag() {
        return breakFlag;
    }

    public void setBreakFlag(String breakFlag) {
        this.breakFlag = breakFlag;
    }

    @Override
    public String toString() {
        return "SrcColumn{" +
                "sys='" + sys + '\'' +
                ", dbSid='" + dbSid + '\'' +
                ", dbSchema='" + dbSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columnId='" + columnId + '\'' +
                ", columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", columnCnName='" + columnCnName + '\'' +
                ", isPk='" + isPk + '\'' +
                ", notNull='" + notNull + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                ", isDk='" + isDk + '\'' +
                ", breakFlag='" + breakFlag + '\'' +
                '}';
    }
}
