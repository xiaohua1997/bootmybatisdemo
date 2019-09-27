package com.moumou.bootmybatisdemo.serviceinterfaceimp.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTableDateOffset;

public class SourceTableDateOffsetDao {
	
	public List<SourceTableDateOffset> getRecords() {
		String sqlString = "select * from source_table_date_offset order by sys asc, db_sid asc, table_schema asc, table_name asc";
		return getList(sqlString);
	}
	
	private List<SourceTableDateOffset> getList(String sql) {
		ResultSet resultSet = null;
		List<SourceTableDateOffset> list = new ArrayList<SourceTableDateOffset>();
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edw_dev","mysql");
		Connection connection = jdbcConn.getDbConnection();
		if (connection == null) {
			return null;
		}
		
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				SourceTableDateOffset item = new SourceTableDateOffset();
				item.set_sys(resultSet.getString("sys"));
				item.set_db_sid(resultSet.getString("db_sid"));
				item.set_table_schema(resultSet.getString("table_schema"));
				item.set_table_name(resultSet.getString("table_name"));
				item.set_offset(resultSet.getInt("offset"));
				
				list.add(item);
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
}

