package com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceSystemDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceTableDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban.plugin.AzkabanShell;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban.plugin.BatchDateBlackList;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban.plugin.JobDateOffset;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.DbCheckJob;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.KettleJob;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.OracleJob;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.SqoopJob;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr.SrcdbToTgtdb;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.ScheduleJobs;

public class ScheduleJobs {
	/***
	 * 生成所有调度作业
	 * @param level 目前可能存在如下几种传入值：all，ods，ods_m（仅限oracle），chk
	 * @param scheduleSystem 目前可能存在如下几种传入值：azkaban
	 * @param dbtype 目前可能存在如下几种传入值：impala，oracle
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public void generate(String level, String scheduleSystem, String dbtype) 
			throws UnsupportedEncodingException, IOException, SQLException {
		if(scheduleSystem.equals("azkaban")) {
			if (dbtype.equals("impala")) {
				azkabanImpala(level, null, null, null, null);
			} else if (dbtype.equals("oracle")) {
				azkabanOracle(level, null, null, null, null);
			}
		}
	}

	private void azkabanOracle(String level, String sys, String sid, String schema, String tableName)
			throws UnsupportedEncodingException, IOException {
		AzkabanShell aAzkabanShell = new AzkabanShell();
		String dir = URLDecoder.decode(ScheduleJobs.class.getClassLoader().getResource("").getPath(), "utf-8")
				+"/kettle_project";
		FileUtils.forceMkdir(new File(dir));
		String flowName = "kettle_oracle_basic";
		int interval_second = 300;
		
		// 获得schema-num信息
		HashMap<String, String> schemaNumMap = new SrcdbToTgtdb().getConvertTableSchema();
		// 获得表名转换信息
		HashMap<String, String> tableNameConvertMap = new SrcdbToTgtdb().getConvertTable();
		
		azkabanCommonFiles(dir, schemaNumMap, tableNameConvertMap);
		
		if (level.equals("chk")) {
			DbCheckJob aDbCheckJob = new DbCheckJob(dir, flowName, interval_second);
			aDbCheckJob.append_JZJY_run_dbo();
			aDbCheckJob.append_RZRQ_run_dbo();
			aDbCheckJob.append_GPQQ_kbssoptsett_dbo();
			aDbCheckJob.append_TYZH_kbssacct_dbo();
			aDbCheckJob.append_ZJGL_kbssfms_dbo();
			
			aAzkabanShell.generateDbCheck(dir);
		} else {			
			//获得表信息
			SourceTableDao aSourceTableDao = new SourceTableDao();
			List<SourceTable> lstSourceTable= null;
			
			if (null != sys && null != sid && null != schema && null != tableName) {
				SourceTable aSourceTable = aSourceTableDao.getTableInfo_Filter(sys, sid, schema, tableName);
				lstSourceTable = new ArrayList<SourceTable>();
				lstSourceTable.add(aSourceTable);
			} else if (null != sys && null != sid && null != schema) {
				lstSourceTable = aSourceTableDao.getTableInfo_Filter(sys, sid, schema);
			} else if (null != sys) {
				lstSourceTable = aSourceTableDao.getTableInfo_Filter(sys);
			} else if (null == sys && null == sid && null == schema && null == tableName) {
				lstSourceTable = aSourceTableDao.getTableInfo_Filter();
			}
			
			if(level.equals("all")) {
				DbCheckJob aDbCheckJob = new DbCheckJob(dir, flowName, interval_second);
				
				// 只有在生成全部系统的全部层次作业时，才是Flow文件需要完全重新生成的时候
				if (null == sys && null == sid && null == schema && null == tableName) {
					aDbCheckJob.createProjectFile(true);
					// TODO 旧逻辑不适用了，需要改
					//aDbCheckJob.createFlowFile(true);
				}
				
				aDbCheckJob.append_JZJY_run_dbo();
				aDbCheckJob.append_RZRQ_run_dbo();
				aDbCheckJob.append_GPQQ_kbssoptsett_dbo();
				aDbCheckJob.append_TYZH_kbssacct_dbo();
				aDbCheckJob.append_ZJGL_kbssfms_dbo();

				KettleJob kettleJob = new KettleJob(dir, flowName);
				kettleJob.appendJobsToFlowFile(lstSourceTable, schemaNumMap, tableNameConvertMap);
				
//				OracleJob oracleJob = new OracleJob(dir, flowName);
//				oracleJob.appendJobsToFlowFile(lstSourceTable, schemaNumMap, tableNameConvertMap);
				
				aAzkabanShell.generateDbCheck(dir);
				aAzkabanShell.generateKettleImportShell();
				aAzkabanShell.generateOracleExecuter();
			} else if (level.equals("ods")) {	
				OracleJob oracleJob = new OracleJob(dir, flowName);
				// TODO 这里需要改造了
				//oracleJob.appendJobsToFlowFile(lstSourceTable, schemaNumMap, tableNameConvertMap);
				
				aAzkabanShell.generateOracleExecuter();
			} else if (level.equals("ods_m")) {
				KettleJob kettleJob = new KettleJob(dir, flowName);
				kettleJob.appendJobsToFlowFile(lstSourceTable, schemaNumMap, tableNameConvertMap);
				
				aAzkabanShell.generateKettleImportShell();
			}
		}
	}

	private void azkabanImpala(String level, String sys, String sid, String schema, String tableName)
			throws UnsupportedEncodingException, IOException, SQLException {
		AzkabanShell aAzkabanShell = new AzkabanShell();
		String dir = URLDecoder.decode(ScheduleJobs.class.getClassLoader().getResource("").getPath(), "utf-8")
				+"/sqoop_project";
		FileUtils.forceMkdir(new File(dir));
		String flowName = "sqoop_basic";
		int interval_second = 300;
		
		// 获得schema-num信息
		HashMap<String, String> schemaNumMap = new SrcdbToTgtdb().getConvertTableSchema();
		// 获得表名转换信息
		HashMap<String, String> tableNameConvertMap = new SrcdbToTgtdb().getConvertTable();
		
		azkabanCommonFiles(dir, schemaNumMap, tableNameConvertMap);
		
		 if (level.equals("chk")) {
				DbCheckJob aDbCheckJob = new DbCheckJob(dir, flowName, interval_second);
				aDbCheckJob.append_JZJY_run_dbo();
				aDbCheckJob.append_RZRQ_run_dbo();
				aDbCheckJob.append_GPQQ_kbssoptsett_dbo();
				aDbCheckJob.append_TYZH_kbssacct_dbo();
				aDbCheckJob.append_ZJGL_kbssfms_dbo();

				aAzkabanShell.generateDbCheck(dir);
		} else {
			//获得系统信息
			SourceSystemDao aSourceSystemDao = new SourceSystemDao();
			List<SourceSystem> lstSourceSystem = null;
			
			if (null != sys && null != sid && null != schema) {
				SourceSystem aSourceSystem = aSourceSystemDao.getSourceSystem(sys, sid, schema);
				lstSourceSystem = new ArrayList<SourceSystem>();
				lstSourceSystem.add(aSourceSystem);
			} else if (null != sys) {
				lstSourceSystem = aSourceSystemDao.getSourceSystem(sys);
			} else if (null == sys && null == sid && null == schema) {
				lstSourceSystem = aSourceSystemDao.getSourceSystem();
			}
			
			//获得表信息
			SourceTableDao aSourceTableDao = new SourceTableDao();
			List<SourceTable> lstSourceTable= null;
			
			if (null != sys && null != sid && null != schema && null != tableName) {
				SourceTable aSourceTable = aSourceTableDao.getTableInfo_Filter(sys, sid, schema, tableName);
				lstSourceTable = new ArrayList<SourceTable>();
				lstSourceTable.add(aSourceTable);
			} else if (null != sys && null != sid && null != schema) {
				lstSourceTable = aSourceTableDao.getTableInfo_Filter(sys, sid, schema);
			} else if (null != sys) {
				lstSourceTable = aSourceTableDao.getTableInfo_Filter(sys);
			} else if (null == sys && null == sid && null == schema && null == tableName) {
				lstSourceTable = aSourceTableDao.getTableInfo_Filter();
			}
			
			if(level.equals("all")) {
				DbCheckJob aDbCheckJob = new DbCheckJob(dir, flowName, interval_second);
				
				// 只有在生成全部系统的全部层次作业时，才是Flow文件需要完全重新生成的时候
				if (null == sys && null == sid && null == schema && null == tableName) {
					aDbCheckJob.createProjectFile(true);
					// TODO 旧逻辑不适用了，需要改
					//aDbCheckJob.createFlowFile(true);
				}
				
				aDbCheckJob.append_JZJY_run_dbo();
				aDbCheckJob.append_RZRQ_run_dbo();
				aDbCheckJob.append_GPQQ_kbssoptsett_dbo();
				aDbCheckJob.append_TYZH_kbssacct_dbo();
				aDbCheckJob.append_ZJGL_kbssfms_dbo();

				SqoopJob aSqoopJob = new SqoopJob(dir, flowName);
				aSqoopJob.appendJobsToFlowFile(lstSourceTable, schemaNumMap, tableNameConvertMap);
				
				aAzkabanShell.generateDbCheck(dir);
				aAzkabanShell.generateSqoopImportShell(lstSourceSystem);
			} else if (level.equals("ods")) {
				SqoopJob aSqoopJob = new SqoopJob(dir, flowName);
				aSqoopJob.appendJobsToFlowFile(lstSourceTable, schemaNumMap, tableNameConvertMap);
				
				aAzkabanShell.generateSqoopImportShell(lstSourceSystem);
			}
		}
	}

	private void azkabanCommonFiles(String dir, HashMap<String, String> schemaNumMap,
			HashMap<String, String> tableNameConvertMap) throws IOException {
		//生成作业运行日期偏移量
		JobDateOffset aJobDateOffset = new JobDateOffset(schemaNumMap, tableNameConvertMap);
		aJobDateOffset.generateAllJob(dir, false, false, true);
		System.out.println("JobDateOffset OK");
		//生成运行日期黑名单
		BatchDateBlackList aBatchDateBlackList = new BatchDateBlackList();
		String start_dt = "20180101";
		String end_dt = "20291231";
		aBatchDateBlackList.generate(dir, start_dt, end_dt, true);
		System.out.println("BatchDateBlackList OK");
	}
	
	/***
	 * 按系统缩写名称生成调度作业
	 * @param level 目前可能存在如下几种传入值：all，ods，ods_m（仅限oracle），chk
	 * @param scheduleSystem 目前可能存在如下几种传入值：azkaban
	 * @param dbtype 目前可能存在如下几种传入值：impala，oracle
	 * @param sys 系统缩写
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public void generate(String level, String scheduleSystem, String dbtype, String sys) 
			throws UnsupportedEncodingException, IOException, SQLException {
		if(scheduleSystem.equals("azkaban")) {
			if (dbtype.equals("impala")) {
				azkabanImpala(level, sys, null, null, null);
			} else if (dbtype.equals("oracle")) {
				azkabanOracle(level, sys, null, null, null);
			}
		}
	}
	
	public void generate(String level, String scheduleSystem, String dbtype, String sys, String sid, String schema) 
			throws UnsupportedEncodingException, IOException, SQLException {
		if(scheduleSystem.equals("azkaban")) {
			if (dbtype.equals("impala")) {
				azkabanImpala(level, sys, sid, schema, null);
			} else if (dbtype.equals("oracle")) {
				azkabanOracle(level, sys, sid, schema, null);
			}
		}
	}
	
	public void generate(String level, String scheduleSystem, String dbtype, String sys, String sid, String schema, String tableName) 
			throws UnsupportedEncodingException, IOException, SQLException {
		if(scheduleSystem.equals("azkaban")) {
			if (dbtype.equals("impala")) {
				azkabanImpala(level, sys, sid, schema, tableName);
			} else if (dbtype.equals("oracle")) {
				azkabanOracle(level, sys, sid, schema, tableName);
			}
		}
	}
}
