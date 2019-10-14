package com.moumou.bootmybatisdemo.dataAlignment.service;

public interface DdlAndDmlService {
	
	String ddlAndDmlAll(String level, String dbtype);
	
	String ddlAndDmlSys(String level, String dbtype, String sys);
	
	String ddlAndDmlSchema(String level, String dbtype, String sys, String sid, String schema);
	
	String ddlAndDmlTable(String level, String dbtype, String sys, String sid, String schema, String tableName);
}
