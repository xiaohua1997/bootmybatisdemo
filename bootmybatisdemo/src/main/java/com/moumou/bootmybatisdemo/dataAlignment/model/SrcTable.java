package com.moumou.bootmybatisdemo.dataAlignment.model;

public class SrcTable {
    private String sys;
    private String dbSid;
    private String tableSchema;
    private String tableName;
    private String tableCnName;
    private String incCdt;
    private String ifMark;
    private String tableType;
    private String templateCode;
    private String isPutToEtldb;

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
