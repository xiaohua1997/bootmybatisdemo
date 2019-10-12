package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.JobService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.ScheduleJobs;

public class JobServiceImpl implements JobService{
	
	 ScheduleJobs scheduleJobs = new ScheduleJobs();

	@Override
	public String jobAll(String level, String scheduleSystem, String dbtype) {
		try {
			scheduleJobs.generate(level, scheduleSystem, dbtype);
			return "true";
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "false";
		} catch (IOException e) {
			e.printStackTrace();
			return "false";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String jobSys(String level, String scheduleSystem, String dbtype, String sys) {
		try {
			scheduleJobs.generate(level, scheduleSystem, dbtype, sys);
			return "true";
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "false";
		} catch (IOException e) {
			e.printStackTrace();
			return "false";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String jobSchame(String level, String scheduleSystem, String dbtype, String sys, String sid, String schema) {
		try {
			scheduleJobs.generate(level, scheduleSystem, dbtype, sys, sid, schema);
			return "true";
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "false";
		} catch (IOException e) {
			e.printStackTrace();
			return "false";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String jobTable(String level, String scheduleSystem, String dbtype,String sys,String sid, String schema, String tableName) {
		try {
			scheduleJobs.generate(level, scheduleSystem, dbtype, sys, sid, schema, tableName);
			return "true";
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "false";
		} catch (IOException e) {
			e.printStackTrace();
			return "false";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}
}
