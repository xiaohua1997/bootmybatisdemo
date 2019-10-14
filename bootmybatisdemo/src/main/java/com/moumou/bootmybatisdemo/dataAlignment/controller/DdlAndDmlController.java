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
import com.moumou.bootmybatisdemo.dataAlignment.service.DdlAndDmlService;
import com.moumou.bootmybatisdemo.util.JsonResult;

@RestController
@RequestMapping("/DllAndDml")
public class DdlAndDmlController {
	
	@Autowired
	private DdlAndDmlService ddlAndDmlService;
	
	@RequestMapping(value="/CreateFile", method=RequestMethod.POST, produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult createFile(@RequestBody SrcTable srcTable) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		String level = srcTable.getLevel();
		String dbtype = srcTable.getDbtype();
		String sys = srcTable.getSys();
		String sid = srcTable.getDbSid();
		String schema = srcTable.getTableSchema();
		String tableName = srcTable.getTableName();
		if(!"".equals(level) && !"".equals(dbtype) && "".equals(sys) && "".equals(sid) && "".equals(schema) && "".equals(tableName)) {
			String s = ddlAndDmlService.ddlAndDmlAll(level,dbtype);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "生成ddl和dml完成");
			}else {
				map.put("status", "error");
		        map.put("msg", "生成失败");
			}
		}else if(!"".equals(level) && !"".equals(dbtype) && !"".equals(sys) && "".equals(sid) && "".equals(schema) && "".equals(tableName)) {
			String s = ddlAndDmlService.ddlAndDmlSys(level, dbtype, sys);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "生成ddl和dml完成");
			}else {
				map.put("status", "error");
		        map.put("msg", "生成失败");
			}
		}else if(!"".equals(level) && !"".equals(dbtype) && !"".equals(sys) && !"".equals(sid) && !"".equals(schema) && "".equals(tableName)) {
			String s = ddlAndDmlService.ddlAndDmlSchema(level, dbtype, sys, sid, schema);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "生成ddl和dml完成");
			}else {
				map.put("status", "error");
		        map.put("msg", "生成失败");
			}
		}else if(!"".equals(level) && !"".equals(dbtype) && !"".equals(sys) && !"".equals(sid) && !"".equals(schema) && !"".equals(tableName)) {
			String s = ddlAndDmlService.ddlAndDmlTable(level, dbtype, sys, sid, schema, tableName);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "生成ddl和dml完成");
			}else {
				map.put("status", "error");
		        map.put("msg", "生成失败");
			}
		}
		return new JsonResult(map);
	}

}
