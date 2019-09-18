package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;
import lombok.Data;

@Data
public class SrcSystem {

    @ExcelColumn("sys")
    private String sys;
    @ExcelColumn("sysNum")
    private String sysNum;
    @ExcelColumn("dbType")
    private String dbType;
    @ExcelColumn("dbVersion")
    private String dbVersion;
    @ExcelColumn("dbSid")
    private String dbSid;
    @ExcelColumn("dbSchema")
    private String dbSchema;
    @ExcelColumn("dbCharset")
    private String dbCharset;
    @ExcelColumn("dbIp")
    private String dbIp;
    @ExcelColumn("dbPort")
    private String dbPort;
    @ExcelColumn("username")
    private String username;
    @ExcelColumn("password")
    private String password;
    @ExcelColumn("encrpassword")
    private String encrpassword;
    @ExcelColumn("remark")
    private String remark;

    public SrcSystem() {
    }

    public SrcSystem(String sys, String sysNum, String dbSid, String dbSchema) {
        this.sys = sys;
        this.sysNum = sysNum;
        this.dbSid = dbSid;
        this.dbSchema = dbSchema;
    }

    public SrcSystem(String sys, String sysNum, String dbType, String dbVersion, String dbSid, String dbSchema, String dbCharset, String dbIp, String dbPort, String username, String password, String encrpassword, String remark) {
        this.sys = sys;
        this.sysNum = sysNum;
        this.dbType = dbType;
        this.dbVersion = dbVersion;
        this.dbSid = dbSid;
        this.dbSchema = dbSchema;
        this.dbCharset = dbCharset;
        this.dbIp = dbIp;
        this.dbPort = dbPort;
        this.username = username;
        this.password = password;
        this.encrpassword = encrpassword;
        this.remark = remark;
    }

    public String getSys() {
        return sys;
    }

    public void setSys(String sys) {
        this.sys = sys;
    }

    public String getSysNum() {
        return sysNum;
    }

    public void setSysNum(String sysNum) {
        this.sysNum = sysNum;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbVersion() {
        return dbVersion;
    }

    public void setDbVersion(String dbVersion) {
        this.dbVersion = dbVersion;
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

    public String getDbCharset() {
        return dbCharset;
    }

    public void setDbCharset(String dbCharset) {
        this.dbCharset = dbCharset;
    }

    public String getDbIp() {
        return dbIp;
    }

    public void setDbIp(String dbIp) {
        this.dbIp = dbIp;
    }

    public String getDbPort() {
        return dbPort;
    }

    public void setDbPort(String dbPort) {
        this.dbPort = dbPort;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEncrpassword() {
        return encrpassword;
    }

    public void setEncrpassword(String encrpassword) {
        this.encrpassword = encrpassword;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @Override
    public String toString() {
        return "SrcSystem{" +
                "sys='" + sys + '\'' +
                ", sysNum='" + sysNum + '\'' +
                ", dbType='" + dbType + '\'' +
                ", dbVersion='" + dbVersion + '\'' +
                ", dbSid='" + dbSid + '\'' +
                ", dbSchema='" + dbSchema + '\'' +
                ", dbCharset='" + dbCharset + '\'' +
                ", dbIp='" + dbIp + '\'' +
                ", dbPort='" + dbPort + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", encrpassword='" + encrpassword + '\'' +
                ", remark='" + remark + '\'' +
                '}';
    }
}
