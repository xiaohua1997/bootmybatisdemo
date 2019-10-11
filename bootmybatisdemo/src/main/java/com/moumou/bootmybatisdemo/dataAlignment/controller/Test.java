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
import com.moumou.bootmybatisdemo.dataAlignment.service.EnterMetaMenuService;
import com.moumou.bootmybatisdemo.util.JsonResult;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/test")
public class Test {
	
	@Autowired
	public EnterMetaMenuService enterMetaMenuService;
	
	@ApiOperation("同步所有：src_column")
	@RequestMapping(value = "/mytest",method = RequestMethod.GET)
	public @ResponseBody JsonResult myTest() {
		SrcTable srcTable = new SrcTable();
		srcTable.setSys("");
		srcTable.setDbSid("");
		srcTable.setTableSchema("");
		srcTable.setTableName("");
		return srcColumn(srcTable);
	}
	
	@ApiOperation("同步所有：src_column")
	@RequestMapping(value="/srccolumn",method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult srcColumn(@RequestBody SrcTable srcTable) {
		Map<String,Object> map = new HashMap<String,Object>();
		String sys = srcTable.getSys();
		String sid = srcTable.getDbSid();
		String schema = srcTable.getTableSchema();
		String tableName = srcTable.getTableName();
		if("".equals(sys) && "".equals(sid) && "".equals(schema) && "".equals(tableName)) {
			String s = enterMetaMenuService.synAllSystem();
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "更新所有系统完成");
			} else {
				map.put("status", "error");
				map.put("msg", "更新失败");
			}
		}else if(!"".equals(sys) && "".equals(sid) && "".equals(schema) && "".equals(tableName)) {
			String s = enterMetaMenuService.synSystem(sys);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "更新" + sys + "完成");
			} else {
				map.put("status", "error");
				map.put("msg", "更新失败");
			}
		}else if(!"".equals(sys) && !"".equals(sid) && !"".equals(schema) && "".equals(tableName)) {
			String s = enterMetaMenuService.synSchema(sys, sid, schema);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "更新"+ sys + "-" + sid + "-" + schema + "完成");
			} else {
				map.put("status", "error");
				map.put("msg", "更新失败");
			}
		}else if(!"".equals(sys) && !"".equals(sid) && !"".equals(schema) && !"".equals(tableName)) {
			String s = enterMetaMenuService.synTable(srcTable);
			if("true".equals(s)) {
				map.put("status", "success");
		        map.put("msg", "更新"+ sys + "-" + sid + "-" + schema + "-" + tableName + "完成");
			} else {
				map.put("status", "error");
				map.put("msg", "更新失败");
			}
		}
		
		return new JsonResult(map);
	}

}
