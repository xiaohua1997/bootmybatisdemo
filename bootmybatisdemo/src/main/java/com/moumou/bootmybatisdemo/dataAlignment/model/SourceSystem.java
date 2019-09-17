package com.moumou.bootmybatisdemo.dataAlignment.model;

public class SourceSystem {

	private String sys;
	private String sys_num;
	private String dbType;
	private String dbVersion;
	private String dbSid;
	private String dbSchema;
	private String dbCharset;
	private String dbIp;
	private String dbPort;
	private String userName;
	private String passWord;
	private String encrPassword;
	private String remark;
	public String getSys() {
		return sys;
	}
	public void setSys(String sys) {
		this.sys = sys;
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
	public String getSys_num() {
		return sys_num;
	}
	public void setSys_num(String sys_num) {
		this.sys_num = sys_num;
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
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassWord() {
		return passWord;
	}
	public void setPassWord(String passWord) {
		this.passWord = passWord;
	}
	public String getEncrPassword() {
		return encrPassword;
	}
	public void setEncrPassword(String encrPassword) {
		this.encrPassword = encrPassword;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	public SourceSystem() {
		super();
	}
	
	public SourceSystem(String sys, String sys_num, String dbType, String dbVersion, String dbSid, String dbSchema,
			String dbCharset, String dbIp, String dbPort, String userName, String passWord, String encrPassword,
			String remark) {
		super();
		this.sys = sys;
		this.sys_num = sys_num;
		this.dbType = dbType;
		this.dbVersion = dbVersion;
		this.dbSid = dbSid;
		this.dbSchema = dbSchema;
		this.dbCharset = dbCharset;
		this.dbIp = dbIp;
		this.dbPort = dbPort;
		this.userName = userName;
		this.passWord = passWord;
		this.encrPassword = encrPassword;
		this.remark = remark;
	}
	@Override
	public String toString() {
		return "SourceSystem [sys=" + sys + ", sys_num=" + sys_num + ", dbType=" + dbType + ", dbVersion=" + dbVersion
				+ ", dbSid=" + dbSid + ", dbSchema=" + dbSchema + ", dbCharset=" + dbCharset + ", dbIp=" + dbIp
				+ ", dbPort=" + dbPort + ", userName=" + userName + ", passWord=" + passWord + ", encrPassword="
				+ encrPassword + ", remark=" + remark + "]";
	}
	
	
}
