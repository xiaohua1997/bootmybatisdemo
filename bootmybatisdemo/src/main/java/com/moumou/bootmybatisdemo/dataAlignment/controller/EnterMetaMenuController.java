package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.EnterMetaMenuService;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/entermetamenu")
public class EnterMetaMenuController {
	
	@Autowired
	private EnterMetaMenuService enterMetaMenuService;
	
	@ApiOperation("同步所有：src_column")
	@RequestMapping(value = "/synallsystem",method = RequestMethod.GET)
	public String synAllSystem() {
		Map<String, String> map = new HashMap<String, String>();
		String s;
		s=enterMetaMenuService.synAllSystem();
		if("成功".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新所有系统完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return enterMetaMenuService.synAllSystem();
	}
	
	@RequestMapping(value = "/synSystem", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synSystem (@RequestBody SrcTable srcTable){
		Map<String, String> map = new HashMap<String, String>();
		String s,sys=srcTable.getSys();
		s=enterMetaMenuService.synSystem(sys);
		if("成功".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新" + sys + "完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return enterMetaMenuService.synSystem(srcTable.getSys());
	}
	
	@RequestMapping(value = "/synSchema", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synSchema (@RequestBody SrcTable srcTable){
		Map<String, String> map = new HashMap<String, String>();
		String s,sys=srcTable.getSys(),sid=srcTable.getDbSid(), schema=srcTable.getTableSchema();
		s=enterMetaMenuService.synSchema(sys, sid, schema);
		if("成功".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新"+ sys + "-" + sid + "-" + schema + "完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return enterMetaMenuService.synSchema(sys, sid, schema);
	}
	
	@RequestMapping(value = "/synTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synTable (@RequestBody SrcTable srcTable){
		Map<String, String> map = new HashMap<String, String>();
		String s,sys=srcTable.getSys(),sid=srcTable.getDbSid(), schema=srcTable.getTableSchema(),tableName=srcTable.getTableName();
		s=enterMetaMenuService.synTable(srcTable);
		if("成功".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新"+ sys + "-" + sid + "-" + schema + "-" + tableName + "完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return enterMetaMenuService.synTable(srcTable);
	}
	
	@RequestMapping(value = "/judgeTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody boolean judgeTable (@RequestBody SrcTable srcTable){
		Map<String, String> map = new HashMap<String, String>();
		boolean f = enterMetaMenuService.judgeTable(srcTable);
		if(f) {
			map.put("status", "success");
	        map.put("msg", "表已存在");
		} else {
			map.put("status", "error");
			map.put("msg", "表不存在，新表");
		}
	  return enterMetaMenuService.judgeTable(srcTable);
	}
}
