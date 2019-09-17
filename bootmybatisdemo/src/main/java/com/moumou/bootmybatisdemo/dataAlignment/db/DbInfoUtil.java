package com.moumou.bootmybatisdemo.dataAlignment.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
 
/**
 * 
 * <p>Description: 获取数据库基本信息的工具类</p>
 * 
 * @author qxl
 * @date 2016年7月22日 下午1:00:34
 */
public class DbInfoUtil {
	
	/**
	 * 根据数据库的连接参数，获取指定表的基本信息：字段名、字段类型、字段注释
	 * @param driver 数据库连接驱动
	 * @param url 数据库连接url
	 * @param user	数据库登陆用户名
	 * @param pwd 数据库登陆密码
	 * @param table	表名
	 * @return Map集合
	 */
	public static List getTableInfo(String driver,String url,String user,String pwd,String table){
		List result = new ArrayList();
		
		Connection conn = null;		
		DatabaseMetaData dbmd = null;
		
		try {
			conn = getConnections(driver,url,user,pwd);
			
			dbmd = conn.getMetaData();
			ResultSet resultSet = dbmd.getTables(null, "%", table, new String[] { "TABLE" });
			
			while (resultSet.next()) {
				Map map1 = new HashMap();
		    	String tableName=resultSet.getString("TABLE_NAME");
		        String zs = resultSet.getString("REMARKS");
		        map1.put("zs", zs);
		        String tabletype = resultSet.getString("TABLE_TYPE");
		        map1.put("tabletype", tabletype);
		        result.add(map1);
		    	//System.out.println(zs);
		    	
		    	if(tableName.equals(table)){
		    		ResultSet rs = conn.getMetaData().getColumns(null, null,tableName, "%");
 
		    		while(rs.next()){
		    			//System.out.println("字段名："+rs.getString("COLUMN_NAME")+"--字段注释："+rs.getString("REMARKS")+"--字段数据类型："+rs.getString("TYPE_NAME"));
		    			Map map = new HashMap();
		    			String colName = rs.getString("COLUMN_NAME");
		    			map.put("colName", colName);
		    			String tablecat = rs.getString("TABLE_CAT");
		    			map.put("tablecat", tablecat);
		    			String tableschem = rs.getString("TABLE_SCHEM");
		    			map.put("tableschem", tableschem);
						/*
						 * String columndef = rs.getString("COLUMN_DEF").substring(26);
						 * //System.out.println(columndef); map.put("columndef", columndef);
						 */
						
						  int nullable = rs.getInt("NULLABLE"); 
						  if(nullable == 0) { 
							  map.put("nullable", "Y"); 
							  }else if(nullable == 1) { 
								  map.put("nullable", "N");
								  }
						
		    			String remarks = rs.getString("REMARKS");
//		    			if(remarks == null || remarks.equals("")){
//		    				remarks = colName;
//		    			}
		    			map.put("remarks",remarks);
		    			
		    			String dbType = rs.getString("TYPE_NAME");
		    			map.put("dbType",dbType);
		    			
		    			map.put("valueType", changeDbType(dbType));
		    			result.add(map);
		    			
		    			//System.out.println(map);
		    		}
		    	}
		    }
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		//System.out.println(result);
		return result;
	}
	
	private static String changeDbType(String dbType) {
		dbType = dbType.toUpperCase();
		switch(dbType){
			case "VARCHAR":
			case "VARCHAR2":
			case "CHAR":
				return "1";
			case "NUMBER":
			case "DECIMAL":
				return "4";
			case "INT":
			case "SMALLINT":
			case "INTEGER":
				return "2";
			case "BIGINT":
				return "6";
			case "DATETIME":
			case "TIMESTAMP":
			case "DATE":
				return "7";
			default:
				return "1";
		}
	}
 
	//获取连接
	private static Connection getConnections(String driver,String url,String user,String pwd) throws Exception {
		Connection conn = null;
		try {
			Properties props = new Properties();
			props.put("remarksReporting", "true");
			props.put("user", user);
			props.put("password", pwd);
			Class.forName(driver);
			conn = DriverManager.getConnection(url, props);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return conn;
	}
	
	//其他数据库不需要这个方法 oracle和db2需要
	private static String getSchema(Connection conn) throws Exception {
		String schema;
		schema = conn.getMetaData().getUserName();
		if ((schema == null) || (schema.length() == 0)) {
			throw new Exception("ORACLE数据库模式不允许为空");
		}
		return schema.toUpperCase().toString();
 
	}
 
	public static void main(String[] args) {
		
		//这里是Oracle连接方法
		
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String url = "jdbc:sqlserver://192.10.30.153:4433;DatabaseName=run";
		String user = "sa";
		String pwd = "Cs@wkzq12345";
		//String table = "FZ_USER_T";
		String table = "applylist";
		
		//mysql
		/*
		String driver = "com.mysql.jdbc.Driver";
		String user = "root";
		String pwd = "123456";
		String url = "jdbc:mysql://localhost/onlinexam"
				+ "?useUnicode=true&characterEncoding=UTF-8";
		String table = "oe_student";
		*/
		
		List<Map> list = getTableInfo(driver,url,user,pwd,table);
		for(Map map :list) {

			for(Object key: map.keySet()) {
				String str = (String)map.get(key);
			//System.out.println("key:"+key);
			//System.out.println("value:"+str);
			}
			//System.out.println("一个字段结束了=============================");
		}
		//System.out.println(list);
		//System.out.println(list.size());
	}
	
}
