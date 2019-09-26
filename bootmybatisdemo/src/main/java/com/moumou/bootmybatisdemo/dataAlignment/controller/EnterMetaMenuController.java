package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.EnterMetaMenuService;
import com.moumou.bootmybatisdemo.util.JsonResult;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/entermetamenu")
public class EnterMetaMenuController {
	
	@Autowired
	private EnterMetaMenuService enterMetaMenuService;

	@ApiOperation("同步所有：src_column")
	@RequestMapping(value = "/synallsystem",method = RequestMethod.GET)
	public JsonResult synAllSystem() {
		System.out.println("synAllSystem");
		Map<String, Object> map = new HashMap<String, Object>();
		String s = enterMetaMenuService.synAllSystem();
		if("true".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新所有系统完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return new JsonResult(map);
//		return enterMetaMenuService.synAllSystem();
	}
	
	@RequestMapping(value = "/synSystem", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult synSystem (@RequestBody SrcTable srcTable){
		System.out.println("synSystem");
		System.out.println(srcTable);
		Map<String, Object> map = new HashMap<String, Object>();
		String sys = srcTable.getSys();
		String s = enterMetaMenuService.synSystem(sys);
		if("true".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新" + sys + "完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return new JsonResult(map);
//		return enterMetaMenuService.synSystem(srcTable.getSys());
	}
	
	@RequestMapping(value = "/synSchema", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult synSchema (@RequestBody SrcTable srcTable){
		System.out.println("synSchema");
		System.out.println(srcTable);
		Map<String, Object> map = new HashMap<String, Object>();
		String sys = srcTable.getSys();
		String sid = srcTable.getDbSid();
		String schema = srcTable.getTableSchema();
		String s = enterMetaMenuService.synSchema(sys, sid, schema);
		if("true".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新"+ sys + "-" + sid + "-" + schema + "完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return new JsonResult(map);
//		return enterMetaMenuService.synSchema(sys, sid, schema);
	}
	
	@RequestMapping(value = "/synTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult synTable (@RequestBody SrcTable srcTable){
		System.out.println("synTable");
		System.out.println(srcTable);
		Map<String, Object> map = new HashMap<String, Object>();
		String sys = srcTable.getSys();
		String sid = srcTable.getDbSid();
		String schema = srcTable.getTableSchema();
		String tableName = srcTable.getTableName();
		String s = enterMetaMenuService.synTable(srcTable);
		if("true".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "更新"+ sys + "-" + sid + "-" + schema + "-" + tableName + "完成");
		} else {
			map.put("status", "error");
			map.put("msg", "更新失败");
		}
		return new JsonResult(map);
//		return enterMetaMenuService.synTable(srcTable);
	}
	
	@RequestMapping(value = "/judgeTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody JsonResult judgeTable (@RequestBody SrcTable srcTable){
		System.out.println("judgeTable");
		System.out.println(srcTable);
		Map<String, Object> map = new HashMap<String, Object>();
		boolean f = enterMetaMenuService.judgeTable(srcTable);
		if(f) {
			map.put("status", "success");
	        map.put("msg", "表已存在");
		} else {
			map.put("status", "error");
			map.put("msg", "表不存在，新表");
		}
		return new JsonResult(map);
//		return enterMetaMenuService.judgeTable(srcTable);
	}
}
