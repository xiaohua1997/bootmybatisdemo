package com.moumou.bootmybatisdemo.dataAlignment.service;

public interface TestOracleAndImpalaFileService {
	
	 String oracleFileDdl(String level);
	 String impalaFileDdl(String level);
	 String oracleJobFile();
	 String impalaJobFile();
	 String oracleKettle();
}
