package com.moumou.bootmybatisdemo.dataAlignment.service;


public interface JobService {
	
	String jobAll(String level, String scheduleSystem, String dbtype);
	
	String jobSys(String level, String scheduleSystem, String dbtype,String sys);
	
	String jobSchame(String level, String scheduleSystem, String dbtype,String sys,String sid, String schema);
	
	String jobTable(String level, String scheduleSystem, String dbtype,String sys,String sid, String schema, String tableName);
}
