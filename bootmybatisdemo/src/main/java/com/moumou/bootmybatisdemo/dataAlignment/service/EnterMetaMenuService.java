package com.moumou.bootmybatisdemo.dataAlignment.service;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;

public interface EnterMetaMenuService {
	String synAllSystem();
	
	String synSystem(String sys);
	
	String synSchema(String sys, String sid, String schema);
	
	String synTable(SrcTable srcTable);
	
	boolean judgeTable(SrcTable srcTable);
}
