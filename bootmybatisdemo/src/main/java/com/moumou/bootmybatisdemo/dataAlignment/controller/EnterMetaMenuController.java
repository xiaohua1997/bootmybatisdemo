package com.moumou.bootmybatisdemo.dataAlignment.controller;

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
		return enterMetaMenuService.synAllSystem();
	}
	
	@RequestMapping(value = "/synSystem", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synSystem (@RequestBody SrcTable srcTable){
	  return enterMetaMenuService.synSystem(srcTable.getSys());
	}
	
	@RequestMapping(value = "/synSchema", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synSchema (@RequestBody SrcTable srcTable){
	  return enterMetaMenuService.synSchema(srcTable.getSys(), srcTable.getDbSid(), srcTable.getTableSchema());
	}
	
	@RequestMapping(value = "/synTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synTable (@RequestBody SrcTable srcTable){
	  return enterMetaMenuService.synTable(srcTable);
	}
	
	@RequestMapping(value = "/judgeTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody boolean judgeTable (@RequestBody SrcTable srcTable){
	  return enterMetaMenuService.judgeTable(srcTable);
	}
}
