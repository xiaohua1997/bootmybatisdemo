package com.moumou.bootmybatisdemo.dataAlignment.service;

public interface EnterMetaMenuService {
	String updateAllSystem();
	
	String updateSystem(String sys);
	
	String updateSchema(String sys, String sid, String schema);
	
	String updateTable();
}
