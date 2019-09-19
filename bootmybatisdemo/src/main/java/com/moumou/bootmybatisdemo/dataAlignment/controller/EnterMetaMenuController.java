package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.sql.SQLException;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.terminal.MetadataManagement;

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
		parseTableInfo(aMetadataManagement, sys, sid, schema, tableInfo);// 指定表
		return "";
	}
	
	
	private static void parseTableInfo(MetadataManagement aMetadataManagement
			,String sys, String sid, String schema, String tableInfo) {
		String[] arrInfo = tableInfo.split("\\t");
		if(arrInfo.length >= 11) {
			String tableName = arrInfo[2].trim();
			String tableCnName = arrInfo[3].trim();
			String etl_flag = arrInfo[10].trim();
			String if_mark = "F";
			if("增量".equals(etl_flag)) {
				if_mark = "I";
			}
			
			//System.out.println(tableName + "," + tableCnName + "," + if_mark);
			
			// 调用接口
			try {
				aMetadataManagement.updateFromSourceDB(sys, sid, schema, tableName, tableCnName, if_mark);
			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
			}
		} else if (arrInfo.length == 1 && !arrInfo[0].trim().isEmpty()) {
			String tableName = arrInfo[0].trim();
			// 如果是新表，要求输入中文名
			try {
				if(aMetadataManagement.exists(sys, sid, schema, tableName)) {
					aMetadataManagement.updateFromSourceDB(sys, sid, schema, tableName);
				} else {
					System.out.println("新增表。请输入中文名:");
					String tableCnName = "";
					System.out.println("新增表。请输入F/I:");
					String if_mark = "".toUpperCase();
					
					aMetadataManagement.updateFromSourceDB(sys, sid, schema, tableName, tableCnName, if_mark);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("输入格式错误");
			return ;
		}
		

	}
}
