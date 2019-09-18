package com.moumou.bootmybatisdemo.dataAlignment.model;

public class SrcColumn {
    private String sys;
    private String dbSid;
    private String dbSchema;
    private String tableName;
    private String columnId;
    private String columnName;
    private String columnType;
    private String columnCnName;
    private String isPk;
    private String notNull;
    private String defaultValue;
    private String isDk;
    private String breakFlag;

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
