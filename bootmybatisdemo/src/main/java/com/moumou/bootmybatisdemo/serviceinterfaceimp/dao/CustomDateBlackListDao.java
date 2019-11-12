package com.moumou.bootmybatisdemo.serviceinterfaceimp.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlack;

public class CustomDateBlackListDao {
	
	public List<CustomDateBlack> getRecords() {
		String sqlString = "select * from custom_date_black_list order by batch_date asc";
		return getList(sqlString);
	}
	
	private List<CustomDateBlack> getList(String sql) {
		ResultSet resultSet = null;
		List<CustomDateBlack> list = new ArrayList<CustomDateBlack>();
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edw_dev","mysql");
		Connection connection = jdbcConn.getDbConnection();
		if (connection == null) {
			return null;
		}
		
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				CustomDateBlack item = new CustomDateBlack();
				item.setBatchdate(resultSet.getString("batch_date"));
				item.setComment(resultSet.getString("comment"));
				
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

