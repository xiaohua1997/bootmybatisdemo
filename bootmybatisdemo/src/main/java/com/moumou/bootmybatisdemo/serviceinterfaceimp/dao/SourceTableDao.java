package com.moumou.bootmybatisdemo.serviceinterfaceimp.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.*;
import com.moumou.bootmybatisdemo.dataAlignment.model.*;


public class SourceTableDao {
	public List<SourceTable> getTableInfo_Filter() {
		String sqlString = this.getBasicSqlBlock_SelectAll_Filter()
				+ this.getBasicSqlBlock_PublicWhereCondition()
				+ this.getBasicSqlBlock_OrderByPrimayKeyAsc();
		return getList(sqlString);
	}
	
	public List<SourceTable> getTableInfo_Filter(String sys) {
		String sqlString = this.getBasicSqlBlock_SelectAll_Filter()
				+ this.getBasicSqlBlock_PublicWhereCondition()
				+ " and t1.sys='" + sys + "'"
				+ this.getBasicSqlBlock_OrderByPrimayKeyAsc();
		return getList(sqlString);
	}
	
	public List<SourceTable> getTableInfo_Filter(String sys, String sid, String schema) {
		String sqlString = this.getBasicSqlBlock_SelectAll_Filter()
				+ this.getBasicSqlBlock_PublicWhereCondition() 
				+ " and t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.table_schema='" + schema + "'"
				+ this.getBasicSqlBlock_OrderByPrimayKeyAsc();
		return getList(sqlString);
	}

	public SourceTable getTableInfo_Filter(String sys, String sid, String schema, String tableName){
		String sqlString = this.getBasicSqlBlock_SelectAll_Filter()
				+ this.getBasicSqlBlock_PublicWhereCondition()
				+ " and t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.table_schema='"+ schema + "'" 
				+ " and t1.table_name='" + tableName + "'"
				+ this.getBasicSqlBlock_OrderByPrimayKeyAsc();
		List<SourceTable> lst = getList(sqlString);
		if (null == lst || lst.size() == 0) {
			return null;
		} else {
			return lst.get(0);
		}
	}
	
	/**
	 * 获取转换了表名后的表信息
	 * @param sys
	 * @return
	 */
	public List<SourceTable> getConvertedTableInfo_Filter(String sys) {
		String sqlString = this.getConvertedTableInfoSqlBlock_SelectAll_Filter()
				+ this.getBasicSqlBlock_PublicWhereCondition()
				+ " and t1.sys='" + sys + "'"
				+ this.getBasicSqlBlock_OrderByPrimayKeyAsc();
		return getList(sqlString);
	}
	
	/**
	 * 
	 * @return
	 */
	private String getConvertedTableInfoSqlBlock_SelectAll_Filter() {
		String sqlFragment = "select t1.sys,t1.db_sid,t1.table_schema,IFNULL(t3.tgt_table_name,t1.table_name) as table_name,t1.table_cn_name,t1.inc_cdt,t1.if_mark,t1.table_type,t1.template_code,t1.is_put_to_etldb "
				+ " from src_table t1 left join src_tablename_convert t3 on t1.sys = t3.sys and t1.table_name = t3.src_table_name ";
		return sqlFragment;
	}
	
	
	private String getBasicSqlBlock_SelectAll_Filter() {
		String sqlFragment = "select t1.* from src_table t1"
				+ " inner join (select distinct sys, db_sid, db_schema, table_name from src_column ) t2 "
				+ " on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.table_schema = t2.db_schema and t1.table_name = t2.table_name";
		return sqlFragment;
	}
	
	private String getBasicSqlBlock_PublicWhereCondition() {
		String sqlFragment = " where upper(t1.is_put_to_etldb)='Y'";
		return sqlFragment;
	}
	
	private String getBasicSqlBlock_OrderByPrimayKeyAsc() {
		String sqlFragment = " order by t1.sys asc, t1.db_sid asc, t1.table_schema asc, t1.table_name asc";
		return sqlFragment;
	}
	
	private List<SourceTable> getList(String sql) {
		ResultSet resultSet=null;
		List<SourceTable> list = new ArrayList<SourceTable>();
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edw_dev","mysql");
		Connection connection = jdbcConn.getDbConnection();
		if (connection == null) {
			return null;
		}
		
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String syString = resultSet.getString(1);
				String db_sidString = resultSet.getString(2);
				String table_schemaString = resultSet.getString(3);
				String table_nameString = resultSet.getString(4);
				String table_cn_nameString = resultSet.getString(5);
				String inc_cdtString = resultSet.getString(6);
				String if_markString = resultSet.getString(7);
				String table_typeString = resultSet.getString(8);
				String template_codeString = resultSet.getString(9);
				String is_put_to_etldbString = resultSet.getString(10);
				SourceTable sourceTable = new SourceTable(syString, db_sidString, table_schemaString, table_nameString, table_cn_nameString
						, inc_cdtString, if_markString, table_typeString, template_codeString, is_put_to_etldbString);
				list.add(sourceTable);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if(resultSet!=null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			jdbcConn.closeDbConnection();
			
		}
		return list;
	}

	public void insert(SourceTable newSourceTable, Connection conn) throws SQLException {
		// 插入一条记录到src_table
		if (null == newSourceTable) {
			System.out.println("newSourceTable is null. Break Insert Into src_table.");
			return ;
		}
		
		String sql = "insert into src_table(sys, db_sid, table_schema, table_name, table_cn_name, inc_cdt, if_mark, table_type, template_code, is_put_to_etldb)"
				+ " values(?,?,?,?,?,?,?,?,?,?)";
		
		PreparedStatement pst = conn.prepareStatement(sql);
		pst.setString(1, newSourceTable.getSyString());
		pst.setString(2, newSourceTable.getDb_sidString());
		pst.setString(3, newSourceTable.getTable_schemaString());
		pst.setString(4, newSourceTable.getTable_nameString());
		pst.setString(5, newSourceTable.getTable_cn_nameString());
		pst.setString(6, newSourceTable.getInc_cdtString());
		pst.setString(7, newSourceTable.getIf_markString());
		pst.setString(8, newSourceTable.getTable_typeString());
		pst.setString(9, newSourceTable.getTemplate_codeString());
		pst.setString(10, newSourceTable.getIs_put_to_etldbString());
		
		System.out.println("insert into src_table affect rows:" + pst.executeUpdate());
		pst.close();
	}

	/**
	 * 从本表的所有行中，判断该表的行是否存在
	 * @param sys
	 * @param sid
	 * @param schema
	 * @param tableName
	 * @return
	 */
	public boolean exists_FromAll(String sys, String sid, String schema, String tableName) {
		boolean flag = false;
		String sql = "select t1.* from src_table t1"
				+ " where t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.table_schema='" + schema + "'"
				+ " and t1.table_name='" + tableName + "'";
		List<SourceTable> lst = getList(sql);
		if (null == lst || lst.size() == 0) {
			flag = false;
		} else {
			flag = true;
		}
		
		return flag;
	}

	public List<SourceTable> getTableInfo_FromAll(String sys, String sid, String schema) {
		String sqlString = "select t1.* from src_table t1"
				+ " where t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.table_schema='" + schema + "'"
				+ this.getBasicSqlBlock_OrderByPrimayKeyAsc();
		return getList(sqlString);
	}
}
