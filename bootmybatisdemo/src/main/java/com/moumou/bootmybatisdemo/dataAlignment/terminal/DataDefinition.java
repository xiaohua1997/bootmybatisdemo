package com.moumou.bootmybatisdemo.dataAlignment.terminal;

import java.io.IOException;

import com.moumou.bootmybatisdemo.dataAlignment.ext.kettle.OracleDdlCreator;

public class DataDefinition {
	
	public void generate(String level, String dbtype
			, boolean dropBeforeCreate, String keywordStyle ) {
		if("oracle".equals(dbtype)){
			OracleDdlCreator oracleDdlCreator = new OracleDdlCreator();
			try {
				oracleDdlCreator.createAllOracleDdl(level);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public void generate(String level, String dbtype, String sys
			, boolean dropBeforeCreate, String keywordStyle ) {
		
	}
	
	public void generate(String level, String dbtype, String sys, String sid, String schema
			, boolean dropBeforeCreate, String keywordStyle ) {
		
	}
	
	public void generate(String level, String dbtype, String sys, String sid, String schema, String tableName
			, boolean dropBeforeCreate, String keywordStyle ) {
		
	}
	
}
