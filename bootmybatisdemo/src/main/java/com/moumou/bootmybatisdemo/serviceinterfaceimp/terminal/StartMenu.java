package com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal;

import java.sql.SQLException;

public class StartMenu {
	
	public String  parseTableInfo(MetadataManagement aMetadataManagement
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
			return "";
		}
		return "";
	}
}
