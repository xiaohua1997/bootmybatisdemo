package com.moumou.bootmybatisdemo.dataAlignment.ext.azkaban2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.common.StringExtension;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;

public class DbCheckJob extends Job{
	public static final String PREFIX = "chk_db_";
	public int interval_second; 
			
	public DbCheckJob(String dir, String flowName, int interval_second) {
		super(dir, flowName);
		this.interval_second = interval_second;
	}
	
	@Override
	public List<String> appendJobsToFlowFile(List<SourceTable> lstTable, HashMap<String, String> schemaNumMap,
			HashMap<String, String> tableNameConvertMap) throws IOException {
		
		return null;
	}

	public void append_JZJY_run_dbo() throws IOException{
		append_JZJY_run_dbo(StringExtension.toStyleString("JZJY_F"));
		append_JZJY_run_dbo(StringExtension.toStyleString("JZJY_I"));
	}
	public void append_RZRQ_run_dbo() throws IOException{
		append_RZRQ_run_dbo(StringExtension.toStyleString("RZRQ_F"));
		append_RZRQ_run_dbo(StringExtension.toStyleString("RZRQ_I"));
	}

	public void append_GPQQ_kbssoptsett_dbo() throws IOException{
		append_GPQQ_kbssoptsett_dbo(StringExtension.toStyleString("GPQQ_F"));
		append_GPQQ_kbssoptsett_dbo(StringExtension.toStyleString("GPQQ_I"));
	}

	public void append_TYZH_kbssacct_dbo() throws IOException{
		append_TYZH_kbssacct_dbo(StringExtension.toStyleString("TYZH_F"));
		append_TYZH_kbssacct_dbo(StringExtension.toStyleString("TYZH_I"));
	}

	public void append_ZJGL_kbssfms_dbo() throws IOException{
		append_ZJGL_kbssfms_dbo(StringExtension.toStyleString("ZJGL_F"));
		append_ZJGL_kbssfms_dbo(StringExtension.toStyleString("ZJGL_I"));
	}

	public void append_JZJY_run_dbo(String flowName) throws IOException {
		this.createProjectFile(false);
		this.createFlowFile(false, flowName);
		
		String jobType = "command";
		
		String jobname = DbCheckJob.PREFIX + "JZJY_run_dbo";
		String command = "bash ./call_dbcheck.sh ${para_date} "
				+ jobname
				+ " ./check/sys/JZJY_run_dbo.properties"
				+ " ./check/sys/JZJY_run_dbo.sql"
				+ " V"
				+ " " + this.interval_second
				+ " \" \""
				+ " \"@{sys:batch_date}\"";
		this.appendJob(jobname, jobType, command, null, flowName, 1);
	}
	
	public void append_RZRQ_run_dbo(String flowName) throws IOException {
		this.createProjectFile(false);
		this.createFlowFile(false, flowName);
		
		String jobType = "command";
		
		String jobname = DbCheckJob.PREFIX + "RZRQ_run_dbo";
		String command = "bash ./call_dbcheck.sh ${para_date} "
				+ jobname
				+ " ./check/sys/RZRQ_run_dbo.properties"
				+ " ./check/sys/RZRQ_run_dbo.sql"
				+ " V"
				+ " " + this.interval_second
				+ " \" \""
				+ " \"@{sys:batch_date}\"";
		this.appendJob(jobname, jobType, command, null, flowName, 1);
	}

	public void append_GPQQ_kbssoptsett_dbo(String flowName) throws IOException {
		this.createProjectFile(false);
		this.createFlowFile(false, flowName);

		String jobType = "command";

		String jobname = DbCheckJob.PREFIX + "GPQQ_kbssoptsett_dbo";
		String command = "bash ./call_dbcheck.sh ${para_date} "
				+ jobname
				+ " ./check/sys/GPQQ_kbssoptsett_dbo.properties"
				+ " ./check/sys/GPQQ_kbssoptsett_dbo.sql"
				+ " V"
				+ " " + this.interval_second
				+ " \" \""
				+ " \"@{sys:batch_date}\"";
		this.appendJob(jobname, jobType, command, null, flowName, 1);
	}

	public void append_TYZH_kbssacct_dbo(String flowName) throws IOException {
		this.createProjectFile(false);
		this.createFlowFile(false, flowName);

		String jobType = "command";

		String jobname = DbCheckJob.PREFIX + "TYZH_kbssacct_dbo";
		String command = "bash ./call_dbcheck.sh ${para_date} "
				+ jobname
				+ " ./check/sys/TYZH_kbssacct_dbo.properties"
				+ " ./check/sys/TYZH_kbssacct_dbo.sql"
				+ " V"
				+ " " + this.interval_second
				+ " \" \""
				+ " \"@{sys:batch_date}\"";
		this.appendJob(jobname, jobType, command, null, flowName, 1);
	}

	public void append_ZJGL_kbssfms_dbo(String flowName) throws IOException {
		this.createProjectFile(false);
		this.createFlowFile(false, flowName);

		String jobType = "command";

		String jobname = DbCheckJob.PREFIX + "ZJGL_kbssfms_dbo";
		String command = "bash ./call_dbcheck.sh ${para_date} "
				+ jobname
				+ " ./check/sys/ZJGL_kbssfms_dbo.properties"
				+ " ./check/sys/ZJGL_kbssfms_dbo.sql"
				+ " V"
				+ " " + this.interval_second
				+ " \" \""
				+ " \"@{sys:batch_date}\"";
		this.appendJob(jobname, jobType, command, null, flowName, 1);
	}

}
