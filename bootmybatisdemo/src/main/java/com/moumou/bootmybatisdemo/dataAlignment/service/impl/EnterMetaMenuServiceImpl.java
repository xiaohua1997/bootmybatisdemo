package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.sql.SQLException;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.EnterMetaMenuService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.MetadataManagement;

@Service
public class EnterMetaMenuServiceImpl implements EnterMetaMenuService {

	private MetadataManagement aMetadataManagement = new MetadataManagement();

	@Override
	public String synAllSystem() {
		try {
			aMetadataManagement.updateFromSourceDB();
			return "true";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String synSystem(String sys) {
		try {
			aMetadataManagement.updateFromSourceDB(sys);
			return "true";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String synSchema(String sys, String sid, String schema) {
		try {
			aMetadataManagement.updateFromSourceDB(sys, sid, schema);
			return "true";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
	}

	@Override
	public String synTable(SrcTable srcTable) {
		boolean f = judgeTable(srcTable);
		try {
			if (f) {
				aMetadataManagement.updateFromSourceDB(srcTable.getSys(), srcTable.getDbSid(),
						srcTable.getTableSchema(), srcTable.getTableName());
			} else {
				aMetadataManagement.updateFromSourceDB(srcTable.getSys(), srcTable.getDbSid(),
						srcTable.getTableSchema(), srcTable.getTableName(), srcTable.getTableCnName(),
						srcTable.getIfMark());
			}
			return "true";
		} catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}

	}

	@Override
	public boolean judgeTable(SrcTable srcTable) {
		return aMetadataManagement.exists(srcTable.getSys(), srcTable.getDbSid(), srcTable.getTableSchema(),
				srcTable.getTableName());
	}
}
