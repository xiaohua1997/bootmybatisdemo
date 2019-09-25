package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.service.HiveService;
import com.moumou.bootmybatisdemo.util.JsonResult;

@RestController
@RequestMapping("/hive")
public class HiveController {
	
	@Autowired
	private HiveService hiveService;
	@RequestMapping(value="/shellandimpala",method = RequestMethod.GET)
	public JsonResult hive() {
		Map<String, Object> map = new HashMap<String, Object>();
		String s = hiveService.hive();
		if("success".equals(s)) {
			map.put("status", "success");
	        map.put("msg", "生成hive脚本和建表语句完成"); 
		}else {
			map.put("status", "false");
	        map.put("msg", "生成失败"); 
		}
		return new JsonResult(map);
		
	}
}
