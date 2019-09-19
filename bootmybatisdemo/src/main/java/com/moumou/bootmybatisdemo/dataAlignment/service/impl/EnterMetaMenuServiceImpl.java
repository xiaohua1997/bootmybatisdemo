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
	public String updateAllSystem() {
		try {
			aMetadataManagement.updateFromSourceDB();
			return "成功";
		} catch (SQLException e) {
			e.printStackTrace();
			return "失败";
		}
	}

	@Override
	public String updateSystem(String sys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String updateSchema(String sys, String sid, String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String updateTable() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
