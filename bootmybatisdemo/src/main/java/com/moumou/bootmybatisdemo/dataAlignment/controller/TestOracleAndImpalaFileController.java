package com.moumou.bootmybatisdemo.dataAlignment.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.service.TestOracleAndImpalaFileService;

@RestController
@RequestMapping("/oracleandimpalafile")
public class TestOracleAndImpalaFileController {
	
	@Autowired
	private TestOracleAndImpalaFileService testOracleAndImpalaFileService;
	@RequestMapping(value="/oraclefile",method = RequestMethod.GET)
	public String oracleFile() {
		return testOracleAndImpalaFileService.oracleFileDdl();
	}
	
	@RequestMapping(value="/impalafile",method = RequestMethod.GET)
	public String impalaFile() {
		return testOracleAndImpalaFileService.impalaFileDdl();
	}
	

}
