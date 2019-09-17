package com.moumou.bootmybatisdemo.dataAlignment.terminal;

import java.io.IOException;
import java.sql.SQLException;

import com.moumou.bootmybatisdemo.dataAlignment.ext.kettle.KettleJobCreator;
import com.moumou.bootmybatisdemo.dataAlignment.ext.kettle.OracleDmlCreator;

public class DataManipulation {
	/**
	 * 
	 * @param level
	 * @param dataManipulationTool
	 * @param dbtype
	 * @param keywordStyle
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public void generate(String level, String dataManipulationTool, String dbtype
			, String keywordStyle) throws SQLException, IOException {
		//TODO 优先支持kettle作业的生成且目标库为Oracle;level参数暂不做使用，keywordStye 源系统涉及大小写，此参数暂不支持
		if("oracle".equals(dbtype)){
			if("ods_m".equals(level)){//生成kettle作业
				KettleJobCreator kettJobCreator = new KettleJobCreator();
				kettJobCreator.createAllKettleJob();
			}else if("ods".equals(level)){//生成ods层的sql处理脚本
				OracleDmlCreator oracleDmlCreator = new OracleDmlCreator();
				oracleDmlCreator.createAllOdsDml();
			}
		}
	}
	
	/**
	 * 按系统sid生成
	 * @param level
	 * @param dataManipulationTool
	 * @param dbtype
	 * @param sys
	 * @param keywordStyle 
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public void generate(String level, String dataManipulationTool, String dbtype
			, String sys
			, String keywordStyle) throws SQLException, IOException {
		//TODO 优先支持kettle作业的生成且目标库为Oracle;level参数暂不做使用，keywordStye 源系统涉及大小写，此参数暂不支持
		if("oracle".equals(dbtype)){
			if("ods_m".equals(level)){//生成kettle作业
				KettleJobCreator kettJobCreator = new KettleJobCreator();
				kettJobCreator.createKettleJob(sys);
			}else if("ods".equals(level)){//生成ods层的sql处理脚本
				OracleDmlCreator oracleDmlCreator = new OracleDmlCreator();
				oracleDmlCreator.createOdsDml(sys);
			}
		}
		
	}
	
	/**
	 * 
	 * @param level
	 * @param dataManipulationTool
	 * @param dbtype
	 * @param sys
	 * @param sid
	 * @param schema
	 * @param keywordStyle
	 * @throws SQLException
	 * @throws IOException
	 */
	public void generate(String level, String dataManipulationTool, String dbtype
			, String sys, String sid, String schema
			, String keywordStyle) throws SQLException, IOException {
		//TODO 优先支持kettle作业的生成且目标库为Oracle;level参数暂不做使用，keywordStye 源系统涉及大小写，此参数暂不支持	
		if("oracle".equals(dbtype)){
			if("ods_m".equals(level)){//生成kettle作业
				KettleJobCreator kettJobCreator = new KettleJobCreator();
				kettJobCreator.createKettleJob(sys,sid,schema);
			}else if("ods".equals(level)){//生成ods层的sql处理脚本
				OracleDmlCreator oracleDmlCreator = new OracleDmlCreator();
				oracleDmlCreator.createOdsDml(sys,sid,schema);
			}
		}
		
	}
	/**
	 * 
	 * @param level
	 * @param dataManipulationTool
	 * @param dbtype
	 * @param sys
	 * @param sid
	 * @param schema
	 * @param tableName
	 * @param keywordStyle
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public void generate(String level, String dataManipulationTool, String dbtype
			, String sys, String sid, String schema, String tableName
			, String keywordStyle) throws SQLException, IOException {
		//TODO 优先支持kettle作业的生成且目标库为Oracle;level参数暂不做使用，keywordStye 源系统涉及大小写，此参数暂不支持
		if("oracle".equals(dbtype)){
			if("ods_m".equals(level)){//生成kettle作业
				KettleJobCreator kettJobCreator = new KettleJobCreator();
				kettJobCreator.createKettleJob(sys,sid,schema,tableName);
			}else if("ods".equals(level)){//生成ods层的sql处理脚本
				OracleDmlCreator oracleDmlCreator = new OracleDmlCreator();
				oracleDmlCreator.createOdsDml(sys,sid,schema,tableName);
			}
		}
		
	}
}
