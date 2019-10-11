package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;

public class KettleJob extends Job {
	public static final String PREFIX = "ods_e_";
	private OracleJob _OracleJob;

	public KettleJob(String dir, String flowName) {
		super(dir, flowName);
		_OracleJob = new OracleJob(dir, flowName);
	}
	
	@Override
	public List<String> appendJobsToFlowFile(List<SourceTable> lstTable, HashMap<String, String> schemaNumMap,
			HashMap<String, String> tableNameConvertMap) throws IOException {
		this.createProjectFile(false);
		//this.createFlowFile(false);
		
		List<String> lstJobResult = new ArrayList<String>();
	
		//循环lstTable
		for (SourceTable sourceTable : lstTable) {
			
			if(!sourceTable.getIs_put_to_etldbString().equalsIgnoreCase("Y")) {
				continue;
			}
			
			String sys = sourceTable.getSyString();
			String sid = sourceTable.getDb_sidString();
			String schema = sourceTable.getTable_schemaString();
			String ifMark = sourceTable.getIf_markString();
			String tableName = sourceTable.getTable_nameString();
			// 表名缩写转换
			tableName = Job.getShortTableName(tableNameConvertMap, tableName, sys);

			//srcTableSchema + "," + db_sid + "," + sys
			String key = schema + "," + sid + "," + sys ;

			//构建文件内容
			//设置type
			String jobType = "command";
			
			String jobName = null;
			String command = null;
			List<String> lstDependsOn = null;

			String schemaNum = schemaNumMap.get(key);
			if(null != schemaNum) {
				jobName =  KettleJob.PREFIX + sys + "_" + schemaNum + "_" + tableName;
				command = "bash ./call_kettle.sh ${para_date} " 
						+ StringExtension.toStyleString(sys) + " "
						+ StringExtension.toStyleString(schemaNum) + " "
						+ StringExtension.toStyleString(tableName);
			} else {
				jobName = KettleJob.PREFIX  + sys + "_" + tableName;
				command = "bash ./call_kettle.sh ${para_date} " 
						+ StringExtension.toStyleString(sys) + " "
						+ "0" + " "
						+ StringExtension.toStyleString(tableName);
			}

			// 确定该Job所属的Flow文件的名称
			String flowName = sys + "_" + ifMark;
			// 确定flowName拼写风格
			flowName = StringExtension.toStyleString(flowName);
			this.createFlowFile(false, flowName, false);
			// sub-flow定义
			String subFlowName = jobName.replace(KettleJob.PREFIX, "");
			// 确定subFlowName拼写风格
			subFlowName = StringExtension.toStyleString(subFlowName);

			// 对JZJY和RZRQ系统的作业配置check作业依赖
			if(sys.equals("JZJY")) {
				lstDependsOn = new ArrayList<String>();
				lstDependsOn.add(DbCheckJob.PREFIX + "JZJY_run_dbo");
				//设置dependencies
				this.appendJob(subFlowName, "flow", null, lstDependsOn, flowName, 1);
			} else if (sys.equals("RZRQ")) {
				lstDependsOn = new ArrayList<String>();
				lstDependsOn.add(DbCheckJob.PREFIX + "RZRQ_run_dbo");
				//设置dependencies
				this.appendJob(subFlowName, "flow", null, lstDependsOn, flowName, 1);
			} else if (sys.equals("GPQQ")) {
				lstDependsOn = new ArrayList<String>();
				lstDependsOn.add(DbCheckJob.PREFIX + "GPQQ_kbssoptsett_dbo");
				//设置dependencies
				this.appendJob(subFlowName, "flow", null, lstDependsOn, flowName, 1);
			} else if (sys.equals("TYZH")) {
				lstDependsOn = new ArrayList<String>();
				lstDependsOn.add(DbCheckJob.PREFIX + "TYZH_kbssacct_dbo");
				//设置dependencies
				this.appendJob(subFlowName, "flow", null, lstDependsOn, flowName, 1);
			} else if (sys.equals("ZJGL")) {
				lstDependsOn = new ArrayList<String>();
				lstDependsOn.add(DbCheckJob.PREFIX + "ZJGL_kbssfms_dbo");
				//设置dependencies
				this.appendJob(subFlowName, "flow", null, lstDependsOn, flowName, 1);
			} else {
				this.appendJob(subFlowName, "flow", null, null, flowName, 1);
			}

			// 确定jobName拼写风格
			jobName = StringExtension.toStyleString(jobName);
			// 添加kettle Job
			this.appendJob(jobName, jobType, command, null, flowName, 2);
			lstJobResult.add(jobName);

			// 添加Oracle Job
			String oralceJobName = this._OracleJob.appendOralcJob(schemaNum, sys, tableName, flowName, 2);
			lstJobResult.add(oralceJobName);
		}
		
		return lstJobResult;
	}

}
