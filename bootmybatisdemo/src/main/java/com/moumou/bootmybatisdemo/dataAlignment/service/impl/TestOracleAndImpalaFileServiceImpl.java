package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.io.IOException;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.service.TestOracleAndImpalaFileService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr.TestOracleAndImpalaFile;
@Service
public class TestOracleAndImpalaFileServiceImpl implements TestOracleAndImpalaFileService{

	private TestOracleAndImpalaFile testOracleAndImpalaFile = new TestOracleAndImpalaFile();

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

}
