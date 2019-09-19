package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.service.EnterMetaMenuService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.MetadataManagement;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.StartMenu;

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
	public @ResponseBody String synSystem (@RequestBody SrcSystem srcSystem){
	  return enterMetaMenuService.synSystem(srcSystem.getSys());
	}
	
	@RequestMapping(value = "/synSchema", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String synSchema (@RequestBody SrcSystem srcSystem){
	  return enterMetaMenuService.synSchema(srcSystem.getSys(), srcSystem.getDbSid(), srcSystem.getDbSchema());
	}
	
	
	
	public String meta() {
		String sys="";
		String sid="";
		String schema="";
		String tableInfo="";
		MetadataManagement aMetadataManagement = new MetadataManagement();
		try {
			aMetadataManagement.updateFromSourceDB();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 所有系统
		try {
			aMetadataManagement.updateFromSourceDB(sys);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 指定系统缩写
		try {
			aMetadataManagement.updateFromSourceDB(sys, sid, schema);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 指定数据库模式
		StartMenu startMenu = new StartMenu();
		startMenu.parseTableInfo(aMetadataManagement, sys, sid, schema, tableInfo);// 指定表
		return "";
	}
	
	
	
}
