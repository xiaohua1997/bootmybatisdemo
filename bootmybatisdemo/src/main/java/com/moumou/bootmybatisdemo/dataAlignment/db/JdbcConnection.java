package com.moumou.bootmybatisdemo.dataAlignment.db;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnection {

	private  Connection conn = null;
	private  String driver = "" ;//驱动
	private  String url = ""; //连接字符串
	private  String username = "";
	private  String password = "";	

	
	/**
	 * 默认获取知识库连接
	 * @param userName
	 * @param passWord
	 * @param ip
	 * @param port
	 * @param dbName
	 * @param dbType
	 */
	public  JdbcConnection(String userName,String passWord,String ip,String port,String dbName,String dbType){
		dbType = dbType.toLowerCase();
		username = userName;
		password = passWord;
		if("oracle".equals(dbType)){
			driver = "oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@//"+ip+":"+port+"/"+dbName;
		}else if ("sqlserver".equals(dbType)||"mssql".equals(dbType)){
			driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
			url = "jdbc:sqlserver://"+ip+":"+port+";DatabaseName="+dbName;
		}else if ("mysql".equals(dbType)){
			driver="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + passWord + "&useUnicode=true&characterEncoding=UTF8";
		}else {
			System.out.println("ERRO:数据库类型有误！");
		}
	}

	/**
	 * 默认知识库连接
	 * @return
	 */
	public  Connection getDbConnection(){
		if(conn==null){
			 try {
				Class.forName(driver);
				conn = DriverManager.getConnection(url,username,password);
				//System.out.println("INFO:数据库连接成功!");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return conn;
	}
	
	/**
	 * 默认知识库连接
	 */
	public  void  closeDbConnection(){
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * 根据系统代码获取源或目标数据库连接
	 * @param userName
	 * @param passWord
	 * @param ip
	 * @param port
	 * @param dbName
	 * @param dbType
	 * @param sysCode
	 */
	public  JdbcConnection(String userName,String passWord,String ip,String port,String dbName,String dbType,String sysCode){
		String _driver = null;
		String _url = null;
		if("oracle".equals(dbType.toLowerCase())){
			_driver = "oracle.jdbc.driver.OracleDriver";
			_url = "jdbc:oracle:thin:@//"+ip+":"+port+"/"+dbName;
		}else if ("sqlserver".equals(dbType)||"mssql".equals(dbType)){
			_driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
			_url = "jdbc:sqlserver://"+ip+":"+port+";DatabaseName="+dbName;
		}else if ("mysql".equals(dbType)){
			_driver="com.mysql.jdbc.Driver";
			_url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + passWord + "&useUnicode=true&characterEncoding=UTF8";
		}else {
			System.out.println("ERRO:数据库类型有误！");
			return;
		}

	}	
	
}
