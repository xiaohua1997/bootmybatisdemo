package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceSystemDao;

public class SqoopJob extends Job {
	public static final String JOB_PREFIX = "ods_e_";
	public static final String TABLE_PREFIX = "ods_";
	
	public SqoopJob(String dir, String flowName) {
		super(dir, flowName);
	}
	
	@Override
	public List<String> appendJobsToFlowFile(List<SourceTable> lstTable, HashMap<String, String> schemaNumMap,
			HashMap<String, String> tableNameConvertMap) throws IOException {
		this.createProjectFile(false);
		//this.createFlowFile(false);
		
		List<String> lstJobResult = new ArrayList<String>();
		
		HashMap<String, SourceSystem> srcMap = null;
		try {
			srcMap = new SourceSystemDao().getSourceSystemToMap();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		for (SourceTable sourceTable : lstTable) {
			
			if(!sourceTable.getIs_put_to_etldbString().equalsIgnoreCase("Y")) {
				continue;
			}
			
			String sys = sourceTable.getSyString();
			String sid = sourceTable.getDb_sidString();
			String schema = sourceTable.getTable_schemaString();
			String tableName = sourceTable.getTable_nameString();
			String ifMark = sourceTable.getIf_markString();

			//srcTableSchema + "," + db_sid + "," + sys
			String key = schema + "," + sid + "," + sys ;
			
			SourceSystem  aSourceSystem = srcMap.get(key);
			if(null == aSourceSystem) {
				System.out.println("在src_system中找不到"+key);
			}
			String sys_num = aSourceSystem.getSys_num();
			String dbType = aSourceSystem.getDbType();


			//构建文件内容
			String jobName = null;
			String targetTableName = null;
			List<String> lstDependsOn = null;
			
			//设置type
			String jobType = "command";
			if(schemaNumMap.containsKey(key)) {
				if(dbType.equalsIgnoreCase("mssql")) {
					if (schema.equalsIgnoreCase("dbo")) {
						jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + tableName;
						targetTableName = SqoopJob.TABLE_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + tableName;
					} else {
						jobName = SqoopJob.JOB_PREFIX + sys_num + "_"+ sys + "_" + sid + "_" + schema + "_" + tableName;
						targetTableName = SqoopJob.TABLE_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + schema + "_" + tableName;
					}
				} else {
					jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + schema + "_" + tableName;
					targetTableName = SqoopJob.TABLE_PREFIX + sys_num + "_" + sys + "_" + schema + "_" + tableName;
				}
			} else {		
				if(dbType.equalsIgnoreCase("mssql")) {
			        jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + tableName;
			        targetTableName = SqoopJob.TABLE_PREFIX + sys_num + "_" + sys + "_" + sid + "_" + tableName;
				} else {
					jobName = SqoopJob.JOB_PREFIX + sys_num + "_" + sys + "_" + schema + "_" + tableName;
					targetTableName = SqoopJob.TABLE_PREFIX + sys_num + "_" + sys + "_" + schema + "_" + tableName;
				}
			}
			
			//设置Command
			String command = "bash ./call_"+ sys + "_" + sid + "_" + schema+ "_to_impala.sh ${para_date} " 
							+ StringExtension.toStyleString(sys) + " "  //确定sys参数拼写风格
							+ schema + " "
							+ tableName + " "
							+ StringExtension.toStyleString(targetTableName); //确定目标表名拼写风格

			// 确定该Job所属的Flow文件的名称
			String flowName = sys + "_" + ifMark;
			// 确定flowName拼写风格
			flowName = StringExtension.toStyleString(flowName);

			this.createFlowFile(false, flowName);

			// sub-flow定义
			String subFlowName = jobName.replace(SqoopJob.JOB_PREFIX, "");
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
			// 写入作业
			this.appendJob(jobName, jobType, command, null, flowName, 2);

			lstJobResult.add(jobName);
		}
		
		return lstJobResult;
	}

}
