package com.moumou.bootmybatisdemo.dataAlignment.model;

public class TableInfo {
    private String info ;
    private String tableName;
    private String schema;

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public TableInfo() {
    }

    public TableInfo(String info, String tableName, String schema) {
        this.info = info;
        this.tableName = tableName;
        this.schema = schema;
    }

}
