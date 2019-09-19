package com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal;

import java.sql.SQLException;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceTableDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metadata.UpdateAction;

public class MetadataManagement {
	private UpdateAction _updateAction = new UpdateAction();
	
	public boolean exists(String sys, String sid, String schema, String tableName) {
		SourceTableDao aSourceTableDao = new SourceTableDao();
		boolean sourcetableExists = aSourceTableDao.exists_FromAll(sys, sid, schema, tableName);
		return sourcetableExists;
	}
	
	public void updateFromSourceDB() throws SQLException {
		this._updateAction.update();
		System.out.println("更新所有系统完成");
	}
	
	public void updateFromSourceDB(String sys) throws SQLException {
		this._updateAction.update(sys);
		System.out.println("更新" + sys + "完成");
	}
	
	public void updateFromSourceDB(String sys, String sid, String schema) throws SQLException {
		this._updateAction.update(sys, sid, schema, null);
		System.out.println("更新"+ sys + "-" + sid + "-" + schema + "完成");
	}
	
	public void updateFromSourceDB(String sys, String sid, String schema, String tableName) throws SQLException {
		this.updateFromSourceDB(sys, sid, schema, tableName, null, null);
	}
	
	public void updateFromSourceDB(String sys, String sid, String schema, String tableName, String tableCnName, String if_mark) throws SQLException {
		this._updateAction.update(sys, sid, schema, tableName, tableCnName, if_mark, null);
		
		if(null == tableCnName) {
			System.out.println("更新"+ sys + "-" + sid + "-" + schema + "-" + tableName + "完成");
		} else {
			System.out.println("新增"+ sys + "-" + sid + "-" + schema + "-" + tableName + "完成");
		}
	}
}
