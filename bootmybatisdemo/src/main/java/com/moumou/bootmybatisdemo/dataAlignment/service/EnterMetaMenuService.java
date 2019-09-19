package com.moumou.bootmybatisdemo.dataAlignment.service;

public interface EnterMetaMenuService {
	String synAllSystem();
	
	String synSystem(String sys);
	
	String synSchema(String sys, String sid, String schema);
	
	String synTable(String sys, String sid, String schema, String tableInfo);
}
