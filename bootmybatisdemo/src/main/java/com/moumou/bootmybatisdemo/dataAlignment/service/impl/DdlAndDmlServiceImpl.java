package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.service.DdlAndDmlService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr.TestOracleAndImpalaFile;

@Service
public class DdlAndDmlServiceImpl implements DdlAndDmlService{

	public TestOracleAndImpalaFile testOracleAndImpalaFile = new TestOracleAndImpalaFile();
	@Override
	public String ddlAndDmlAll(String level, String dbtype) {
		try {
			testOracleAndImpalaFile.generate(level, dbtype);;
			return "true";
		} catch (Exception e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String ddlAndDmlSys(String level, String dbtype, String sys) {
		try {
			testOracleAndImpalaFile.generate(level, dbtype, sys);
			return "true";
		} catch (Exception e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String ddlAndDmlSchema(String level, String dbtype, String sys, String sid, String schema) {
		try {
			testOracleAndImpalaFile.generate(level, dbtype, sys, sid, schema);
			return "true";
		} catch (Exception e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String ddlAndDmlTable(String level, String dbtype, String sys, String sid, String schema, String tableName) {
		try {
			testOracleAndImpalaFile.generate(level, dbtype, sys, sid, schema, tableName);
			return "true";
		} catch (Exception e) {
			e.printStackTrace();
			return "false";
		}
	}

}
