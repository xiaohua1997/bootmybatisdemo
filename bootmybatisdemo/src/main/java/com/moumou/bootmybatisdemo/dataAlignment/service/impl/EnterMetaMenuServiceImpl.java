package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.service.EnterMetaMenuService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.MetadataManagement;

@Service
public class EnterMetaMenuServiceImpl implements EnterMetaMenuService {
	
	private MetadataManagement aMetadataManagement;

	@Override
	public String synAllSystem() {
		try {
			aMetadataManagement.updateFromSourceDB();
			return "成功";
		} catch (SQLException e) {
			e.printStackTrace();
			return "失败";
		}
	}

	@Override
	public String synSystem(String sys) {
		try {
			aMetadataManagement.updateFromSourceDB(sys);
			return "成功";
		} catch (SQLException e) {
			e.printStackTrace();
			return "失败";
		}
	}

	@Override
	public String synSchema(String sys, String sid, String schema) {
		try {
			aMetadataManagement.updateFromSourceDB(sys, sid, schema);
			return "成功";
		} catch (SQLException e) {
			e.printStackTrace();
			return "失败";
		}
	}

	@Override
	public String synTable(String sys, String sid, String schema, String tableInfo) {
		
		return null;
	}
}
