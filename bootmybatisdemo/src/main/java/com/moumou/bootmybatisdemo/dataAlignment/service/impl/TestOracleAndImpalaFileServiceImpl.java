package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.SrcSystemMapper;
import com.moumou.bootmybatisdemo.dataAlignment.service.TestOracleAndImpalaFileService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.kettle.KettleJobCreator;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr.TestOracleAndImpalaFile;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.ScheduleJobs;
@Service
public class TestOracleAndImpalaFileServiceImpl implements TestOracleAndImpalaFileService{

	private TestOracleAndImpalaFile testOracleAndImpalaFile = new TestOracleAndImpalaFile();
	private ScheduleJobs aScheduleJobs = new ScheduleJobs();
	private KettleJobCreator kettleJobCreator = new KettleJobCreator();
    @Autowired
    private SrcSystemMapper srcSystemMapper;
	@Override
	public String oracleFileDdl() {
		try {
			testOracleAndImpalaFile.oracleFile();
			return "success";
		} catch (Exception e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String impalaFileDdl() {
		try {
			testOracleAndImpalaFile.impalaFile();
			return "success";
		} catch (IOException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String oracleJobFile() {
		String level = "all",scheduleSystem = "azkaban",dbtype = "oracle";
		try {
			aScheduleJobs.generate(level, scheduleSystem, dbtype);
			return "success";
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
	public String impalaJobFile() {
		String level = "all",scheduleSystem = "azkaban",dbtype = "impala";
		try {
			aScheduleJobs.generate(level, scheduleSystem, dbtype);
			return "success";
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
	public String oracleKettle() {
		
		List<String> s = srcSystemMapper.querySys();
    		try {
    			for(String sys:s) {
				new KettleJobCreator().createKettleJob(sys);
    			}
    			return "success";
			} catch (SQLException e) {
				e.printStackTrace();
				return "false";
			} catch (IOException e) {
				e.printStackTrace();
				return "false";
			}
	}

}
