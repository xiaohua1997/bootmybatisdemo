package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban.plugin;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;
import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceSystemDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceTableDateOffsetDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.Job;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.KettleJob;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.OracleJob;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2.SqoopJob;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTableDateOffset;

public class JobDateOffset {
	public static final String EOL = "\n";
	public static final String CHARSET_NAME = "UTF-8";
	
	protected String fileName = "job_date_offset.conf";
	protected String target_dir;
	
	private HashMap<String, SourceSystem> srcMap;
	private HashMap<String, String> schemaNumMap;
	private HashMap<String, String> tableNameConvertMap;
	
	public JobDateOffset(HashMap<String, String> schemaNumMap, HashMap<String, String> tableNameConvertMap) {
		
		this.schemaNumMap = schemaNumMap;
		this.tableNameConvertMap = tableNameConvertMap;
		
		try {
			this.srcMap = new SourceSystemDao().getSourceSystemToMap();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void generateAllJob(String target_dir, boolean kettle, boolean oracle, boolean sqoop) throws IOException {
		this.target_dir = target_dir;
		
		//重新创建空白文件
		File file = new File(this.target_dir + File.separator + this.fileName);
		FileUtils.write(file, "", CHARSET_NAME, false);
		
		//读取表中的所有数据
		SourceTableDateOffsetDao aSourceTableDateOffsetDao = new SourceTableDateOffsetDao();
		for (SourceTableDateOffset item : aSourceTableDateOffsetDao.getRecords()) {
			//判断需要生成的作业类型（目前没有这个逻辑）
			//调用对应生成方法	
			if(kettle) {
				this.generateKettleJob(item, true);
			}
			if(oracle) {
				this.generateOracleJob(item, true);
			}
			if(sqoop) {
				this.generateSqoopJob(item, true);
			}
			
		}
		
	}
	
	private void generateSqoopJob(SourceTableDateOffset record, boolean append) throws IOException {
		String content = "";
		
		String sys = record.get_sys();
		String sid = record.get_db_sid();
		String schema = record.get_table_schema();
		String tableName = record.get_table_name();
		
		String key = schema + "," + sid + "," + sys ;
		
		SourceSystem  aSourceSystem = srcMap.get(key);
		if(null == aSourceSystem) {
			System.out.println("在src_system中找不到"+key);
		}
		String sys_num = aSourceSystem.getSys_num();
		String dbType = aSourceSystem.getDbType();
		
		//构建文件内容
		String jobName = null;
		
		if(schemaNumMap.containsKey(key)) {
			if(dbType.equalsIgnoreCase("mssql")) {
				if (schema.equalsIgnoreCase("dbo")) {
					jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + tableName;
				} else {
					jobName = SqoopJob.JOB_PREFIX + sys_num + "_"+ sys + "_" + sid + "_" + schema + "_" + tableName;
				}
			} else {
				jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + schema + "_" + tableName;
			}
		} else {		
			if(dbType.equalsIgnoreCase("mssql")) {
		        jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + tableName;
			} else {
				jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + schema + "_" + tableName;
			}
		}

		//确定jobName拼写风格
		jobName = StringExtension.toStyleString(jobName);
		content = jobName + "=" + record.get_offset() + EOL;
		
		File file = new File(this.target_dir + File.separator + this.fileName);
		FileUtils.writeStringToFile(file, content, CHARSET_NAME, append);
	}
	
	private void generateKettleJob(SourceTableDateOffset record, boolean append) throws IOException{
		String content = "";
		
		String sys = record.get_sys();
		String sid = record.get_db_sid();
		String schema = record.get_table_schema();
		String tableName = record.get_table_name();
		
		// 表名缩写转换
		tableName = Job.getShortTableName(tableNameConvertMap, tableName, sys);
		
		String key = schema + "," + sid + "," + sys ;
		
		//构建文件内容
		String jobName = null;
		if(schemaNumMap.containsKey(key)) {
			String schemaNum = schemaNumMap.get(key);
			
			jobName =  KettleJob.PREFIX + sys + "_" + schemaNum + "_" + tableName;
		} else {
			jobName = KettleJob.PREFIX  + sys + "_" + tableName;
		}

		//确定jobName拼写风格
		jobName = StringExtension.toStyleString(jobName);
		content = jobName + "=" + record.get_offset() + EOL;
		
		File file = new File(this.target_dir + File.separator + this.fileName);
		FileUtils.writeStringToFile(file, content, CHARSET_NAME, append);

	}
	
	private void generateOracleJob(SourceTableDateOffset record, boolean append) throws IOException{
		String content = "";
		
		String sys = record.get_sys();
		String sid = record.get_db_sid();
		String schema = record.get_table_schema();
		String tableName = record.get_table_name();
		
		// 表名缩写转换
		tableName = Job.getShortTableName(tableNameConvertMap, tableName, sys);
		String key = schema + "," + sid + "," + sys ;
		
		String jobName = null;
		if(schemaNumMap.containsKey(key)) {
			String schemaNum = schemaNumMap.get(key);
			
			jobName = OracleJob.PREFIX + sys + "_" + schemaNum + "_" + tableName;
		} else {
			//构建文件内容
			jobName = OracleJob.PREFIX + sys + "_" + tableName;
		}

		//确定jobName拼写风格
		jobName = StringExtension.toStyleString(jobName);
		content = jobName + "=" + record.get_offset() + EOL;
		
		File file = new File(this.target_dir + File.separator + this.fileName);
		FileUtils.writeStringToFile(file, content, CHARSET_NAME, append);
	}
	
	private void generateDbCheckJob(String fileName, boolean append) {
		
	}
	
	private void generateFileCheckJob(String fileName, boolean append) {
		
	}
	
}
