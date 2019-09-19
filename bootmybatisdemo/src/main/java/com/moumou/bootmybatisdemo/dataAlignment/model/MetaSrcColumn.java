package com.moumou.bootmybatisdemo.dataAlignment.model;

public class MetaSrcColumn {
    private String sys;
    private String dbSid;
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String columnDesc;
    private String columnType;
    private String nullable;
    private String totalCount;
    private String columnNullCnt;
    private String columnNullRate;
    private String columnMaxVal;
    private String columnMinVal;
    private String codeRate;
    private String legalType = "Y";
    private String remarks;
    private String pk;
    private String columnId;
    private String ColumnCnName;

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    public String getColumnCnName() {
        return ColumnCnName;
    }

    public void setColumnCnName(String columnCnName) {
        ColumnCnName = columnCnName;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public MetaSrcColumn() {

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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnDesc() {
        return columnDesc;
    }

    public void setColumnDesc(String columnDesc) {
        this.columnDesc = columnDesc;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getNullable() {
        return nullable;
    }

    public void setNullable(String nullable) {
        this.nullable = nullable;
    }

    public String getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(String totalCount) {
        this.totalCount = totalCount;
    }

    public String getColumnNullCnt() {
        return columnNullCnt;
    }

    public void setColumnNullCnt(String columnNullCnt) {
        this.columnNullCnt = columnNullCnt;
    }

    public String getColumnNullRate() {
        return columnNullRate;
    }

    public void setColumnNullRate(String columnNullRate) {
        this.columnNullRate = columnNullRate;
    }

    public String getColumnMaxVal() {
        return columnMaxVal;
    }

    public void setColumnMaxVal(String columnMaxVal) {
        this.columnMaxVal = columnMaxVal;
    }

    public String getColumnMinVal() {
        return columnMinVal;
    }

    public void setColumnMinVal(String columnMinVal) {
        this.columnMinVal = columnMinVal;
    }

    public String getCodeRate() {
        return codeRate;
    }

    public void setCodeRate(String codeRate) {
        this.codeRate = codeRate;
    }

    public String getLegalType() {
        return legalType;
    }

    public void setLegalType(String legalType) {
        this.legalType = legalType;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    @Override
    public String toString() {
        return "MetaSrcColumn{" +
                "sys='" + sys + '\'' +
                ", dbSid='" + dbSid + '\'' +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columnId='" + columnId + '\'' +
                ", columnName='" + columnName + '\'' +
                ", columnDesc='" + columnDesc + '\'' +
                ", columnType='" + columnType + '\'' +
                ", nullable='" + nullable + '\'' +
                ", pk='" + pk + '\'' +
                ", totalCount='" + totalCount + '\'' +
                ", columnNullCnt='" + columnNullCnt + '\'' +
                ", columnNullRate='" + columnNullRate + '\'' +
                ", columnMaxVal='" + columnMaxVal + '\'' +
                ", columnMinVal='" + columnMinVal + '\'' +
                ", codeRate='" + codeRate + '\'' +
                ", legalType='" + legalType + '\'' +
                ", remarks='" + remarks + '\'' +
                '}';
    }

    public MetaSrcColumn(String sys, String dbSid, String tableSchema, String tableName, String columnName, String columnDesc,
                         String columnType, String nullable, String totalCount, String columnNullCnt, String columnNullRate,
                         String columnMaxVal, String columnMinVal, String codeRate, String legalType, String remarks) {
        this.sys = sys;
        this.dbSid = dbSid;
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnDesc = columnDesc;
        this.columnType = columnType;
        this.nullable = nullable;
        this.totalCount = totalCount;
        this.columnNullCnt = columnNullCnt;
        this.columnNullRate = columnNullRate;
        this.columnMaxVal = columnMaxVal;
        this.columnMinVal = columnMinVal;
        this.codeRate = codeRate;
        this.legalType = legalType;
        this.remarks = remarks;
    }
}
