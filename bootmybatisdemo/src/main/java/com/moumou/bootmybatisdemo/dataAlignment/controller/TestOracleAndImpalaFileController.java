package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.TestOracleAndImpalaFileService;
import com.moumou.bootmybatisdemo.util.JsonResult;

@RestController
@RequestMapping("/oracleandimpalafile")
public class TestOracleAndImpalaFileController {
	
	@Autowired
	private TestOracleAndImpalaFileService testOracleAndImpalaFileService;
	@RequestMapping(value="/oraclefile", method=RequestMethod.POST, produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult createFile(@RequestBody SrcTable srcTable) {
		String level = srcTable.getLevel();
		Map<String, Object> map = new HashMap<String, Object>();
		String s = testOracleAndImpalaFileService.oracleFileDdl(level);
		if("success".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "生成oracle的ddl完成"); 
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
	}
	
	@RequestMapping(value="/impalafile", method=RequestMethod.POST, produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult impalaFile(@RequestBody SrcTable srcTable) {
		String level = srcTable.getLevel();
		
		Map<String, Object> map = new HashMap<String, Object>();
		String s = testOracleAndImpalaFileService.impalaFileDdl(level);
		if("success".equals(s)) {
			map.put("status", "success");
			map.put("msg", "生成impala的ddl完成");
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
	}
	
	@RequestMapping(value="/oraclejobfile",method = RequestMethod.GET)
	public JsonResult oracleJobFile() {
		
		Map<String, Object> map = new HashMap<String, Object>();
		String s = testOracleAndImpalaFileService.oracleJobFile();
		if("success".equals(s)) {
			map.put("status", "success");
			map.put("msg", "生成oracle的job作业完成");
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
	}
	
	@RequestMapping(value="/impalajobfile",method = RequestMethod.GET)
	public JsonResult impalaJobFile() {
		
		Map<String, Object> map = new HashMap<String, Object>();
		String s = testOracleAndImpalaFileService.impalaJobFile();
		if("success".equals(s)) {
			map.put("status", "success");
			map.put("msg", "生成impala的job作业完成");
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
	}
	
	@RequestMapping(value="/oraclekettle",method = RequestMethod.GET)
	public JsonResult oracleKettle() {
		
		Map<String, Object> map = new HashMap<String, Object>();
		String s = testOracleAndImpalaFileService.oracleKettle();
		if("success".equals(s)) {
			map.put("status", "success");
			map.put("msg", "生成oracle的kettle作业完成");
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
	}
}
