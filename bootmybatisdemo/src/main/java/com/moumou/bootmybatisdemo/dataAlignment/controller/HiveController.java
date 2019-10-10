package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.model.Hive;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.HiveService;
import com.moumou.bootmybatisdemo.util.JsonResult;

@RestController
@RequestMapping("/hive")
public class HiveController {
	
	@Autowired
	private HiveService hiveService;
	/*
	  * 批量生产所有的hive脚本和hive的impala建表语句
	 */
	@RequestMapping(value="/shellandimpala",method = RequestMethod.GET)
	public JsonResult hives() {
		
		Map<String, Object> map = new HashMap<String, Object>();
		String s = hiveService.hives();
		if("success".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "生成所有hive脚本和建表语句完成"); 
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
	}
	
	/*
	  * 指定生产hive脚本和hive的impala建表语句。参数（yewu、mapping）
	 * yewu：列如：02 产品\\产品mapping\\产品mapping.xlsx
	 * mapping：例如：股票
	 */
    @RequestMapping(value="/ashellandaimpala",method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody JsonResult aHive (@RequestBody Hive hive) {
    	
    	Map<String, Object> map = new HashMap<String, Object>();
    	String s = hiveService.aHive(hive);
    	if("success".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "生成所有hive脚本和建表语句完成"); 
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
    }
    
    /*
           * 生成hive的job作业和azkaban调度文件
     */
    @RequestMapping(value="/hivejob",method = RequestMethod.GET)
    public  JsonResult hiveJob() {
    	Map<String,Object> map = new HashMap<String,Object>();
    	String s = hiveService.hiveJob();
    	if("success".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "生成hive的job作业和azkaban调度文件"); 
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
    }
}
