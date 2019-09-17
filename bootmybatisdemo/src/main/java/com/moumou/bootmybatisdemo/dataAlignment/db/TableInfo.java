package com.moumou.bootmybatisdemo.dataAlignment.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableInfo {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		Connection conn = DriverManager.getConnection("jdbc:sqlserver://192.10.30.153:4433;User=sa;Password=Cs@wkzq12345;DatabaseName=run");
		DatabaseMetaData m_DBMetaData = conn.getMetaData(); 
		String m_TableName = null;
		ResultSet tableRet = m_DBMetaData.getTables(null, "%",m_TableName,new String[]{"TABLE"}); 
		while(tableRet.next()) {
			
			String columnName; 
			String columnType; 
			String TABLE_NAME = tableRet.getString("TABLE_NAME");
			System.out.println("TABLE_NAME:"+TABLE_NAME);
			ResultSet colRet = m_DBMetaData.getColumns(null,"%", TABLE_NAME,"%"); 
			while(colRet.next()) { 
				columnName = colRet.getString("COLUMN_NAME"); 
				columnType = colRet.getString("TYPE_NAME"); 
				int datasize = colRet.getInt("COLUMN_SIZE"); 
				int digits = colRet.getInt("DECIMAL_DIGITS"); 
				int nullable = colRet.getInt("NULLABLE"); 
				System.out.println(columnName+" / "+columnType+"/"+datasize+"/"+digits+"/"+ nullable); 
			}
		 
		}
	}

}
