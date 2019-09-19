package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.sql.SQLException;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.MetadataManagement;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.StartMenu;

@RestController
@RequestMapping("/entermetamenu")
public class EnterMetaMenuController {
	
	
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
