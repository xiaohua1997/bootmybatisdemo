package com.moumou.bootmybatisdemo.dataAlignment.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlackList;

public class CustomDateBlackListDao {
	
	public List<CustomDateBlackList> getRecords() {
		String sqlString = "select * from custom_date_black_list order by batch_date asc";
		return getList(sqlString);
	}
	
	private List<CustomDateBlackList> getList(String sql) {
		ResultSet resultSet = null;
		List<CustomDateBlackList> list = new ArrayList<CustomDateBlackList>();
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edwassisdb","mysql");
		Connection connection = jdbcConn.getDbConnection();
		if (connection == null) {
			return null;
		}
		
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				CustomDateBlackList item = new CustomDateBlackList();
				item.set_batch_date(resultSet.getString("batch_date"));
				item.set_comment(resultSet.getString("comment"));
				
				list.add(item);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if(resultSet!=null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			jdbcConn.closeDbConnection();
			
		}
		return list;
	}
}

