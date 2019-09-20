package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;

public class OracleJob extends Job {
	public static final String PREFIX = "ods_t_";
	
	public OracleJob(String dir, String flowName) {
		super(dir, flowName);
	}
	
	@Override
	public List<String> appendJobsToFlowFile(List<SourceTable> lstTable, HashMap<String, String> schemaNumMap,
			HashMap<String, String> tableNameConvertMap) throws IOException {
		this.createProjectFile(false);
		this.createFlowFile(false);
		
		//java -jar ./OracleExecuter.jar 20190416 ./sql/bbb.sql
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
			tableName = this.getShortTableName(tableNameConvertMap, tableName, sys);
			//srcTableSchema + "," + db_sid + "," + sys
			String key = schema + "," + sid + "," + sys ;
			String schemaNum = schemaNumMap.get(key);

			// 确定该Job所属的Flow文件的名称
			String flowName = sys + "_" + ifMark;
			this.createFlowFile(false, flowName);
			// TODO 后续确定该方法是否还需要保留
			//String subFlowName = jobName.replace(KettleJob.PREFIX, "");

			String jobName = appendOralcJob(schemaNum, sys, tableName, this.defaultFlowName, 1);
			lstJobResult.add(jobName);
		}
		
		return lstJobResult;
	}

	public String appendOralcJob(String schemaNum, String sys, String tableName, String flowName, int itemYamlLevel) throws IOException {
		String jobName = null;
		String command = null;
		List<String> lstDependsOn = null;

		//设置type
		String jobType = "command";

		if(null != schemaNum) {
			jobName = OracleJob.PREFIX + sys + "_" + schemaNum + "_" + tableName;
			command = "bash ./call_oracle.sh ${para_date} "
					+ "./oracle/" + StringExtension.toStyleString(sys) + "/"
					+ StringExtension.toStyleString(sys) + "_"
					+ StringExtension.toStyleString(schemaNum) + "_"
					+ StringExtension.toStyleString(tableName)
					+ ".sql";

			lstDependsOn = new ArrayList<String>();
			String dependsJobName = StringExtension.toStyleString(KettleJob.PREFIX + sys + "_" +  schemaNum + "_" + tableName);
			lstDependsOn.add(dependsJobName);
		} else {
			//构建文件内容
			jobName = OracleJob.PREFIX + sys + "_" + tableName;
			//设置Command
			command = "bash ./call_oracle.sh ${para_date} "
							+ "./oracle/" + StringExtension.toStyleString(sys) + "/"
							+ StringExtension.toStyleString(sys) + "_"
							+ StringExtension.toStyleString(tableName)
							+ ".sql";
			//设置dependencies
			lstDependsOn = new ArrayList<String>();
			String dependsJobName = StringExtension.toStyleString(KettleJob.PREFIX + sys + "_" + tableName);
			lstDependsOn.add(dependsJobName);
		}

		// 确定jobName拼写风格
		jobName = StringExtension.toStyleString(jobName);
		this.appendJob(jobName, jobType, command, lstDependsOn, flowName, itemYamlLevel);
		return jobName;
	}

}
