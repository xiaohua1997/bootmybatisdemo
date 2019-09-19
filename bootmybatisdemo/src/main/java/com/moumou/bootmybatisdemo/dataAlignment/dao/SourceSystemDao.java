package com.moumou.bootmybatisdemo.dataAlignment.dao;

import java.sql.SQLException;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;

/**
 * 源系统信息Dao
 * @author simon
 *
 */
public class SourceSystemDao {
	
	/**
	 * 获取所有源系统信息
	 * @return
	 * @throws SQLException
	 */
	public java.util.List<SourceSystem> getSourceSystem() throws SQLException{
		java.util.List<SourceSystem> ret = null;
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edwassisdb","mysql");
		String sql = "select * from src_system";
		java.sql.Connection conn = jdbcConn.getDbConnection();
		java.sql.PreparedStatement pstm = conn.prepareStatement(sql);
		java.sql.ResultSet rs = pstm.executeQuery();
		ret = convertSourceSystemFromRs(rs);
		try{
			rs.close();
			pstm.close();
			jdbcConn.closeDbConnection();
		}catch(SQLException ex){
			ex.printStackTrace();
		}
		return ret;
	}
	
	public List<SourceSystem> getSourceSystem(String sys) throws SQLException {
		java.util.List<SourceSystem> ret = null;
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edwassisdb","mysql");
		String sql = "select * from src_system where sys='"+ sys +"'";
		java.sql.Connection conn = jdbcConn.getDbConnection();
		java.sql.PreparedStatement pstm = conn.prepareStatement(sql);
		java.sql.ResultSet rs = pstm.executeQuery();
		ret = convertSourceSystemFromRs(rs);
		try{
			rs.close();
			pstm.close();
			jdbcConn.closeDbConnection();
		}catch(SQLException ex){
			ex.printStackTrace();
		}
		return ret;
	}
	
	public SourceSystem getSourceSystem(String sys, String sid, String schema) throws SQLException {
		List<SourceSystem> ret = null;
		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edwassisdb","mysql");
		String sql = "select * from src_system where sys='"+ sys +"' and db_sid = '" + sid + "' and db_schema = '" + schema + "'";
		java.sql.Connection conn = jdbcConn.getDbConnection();
		java.sql.PreparedStatement pstm = conn.prepareStatement(sql);
		java.sql.ResultSet rs = pstm.executeQuery();
		ret = convertSourceSystemFromRs(rs);
		try{
			rs.close();
			pstm.close();
			jdbcConn.closeDbConnection();
		}catch(SQLException ex){
			ex.printStackTrace();
		}
		return ((null != ret && ret.size() > 0) ? ret.get(0) : null );
	}
//	/**
//	 * 根据系统简称获取源系统信息
//	 * @param sysCode
//	 * @return
//	 * @throws SQLException 
//	 */
//	public SourceSystem getSourceSystem(String sysCode) throws SQLException{
//		SourceSystem ret = null;
//		JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edwassisdb","mysql");
//		String sql = "select * from src_system where sys='"+sysCode+"'";
//		java.sql.Connection conn = jdbcConn.getDbConnection();
//		java.sql.PreparedStatement pstm = conn.prepareStatement(sql);
//		java.sql.ResultSet rs = pstm.executeQuery();
//		java.util.List<SourceSystem> list = convertSourceSystemFromRs(rs);
//		ret = list!=null&&!list.isEmpty()?list.get(0):null;
//		try{
//
//			rs.close();
//			pstm.close();
//			jdbcConn.closeDbConnection();
//		}catch(SQLException ex){
//			ex.printStackTrace();
//		}
//		return ret;
//	}
	
	
	private java.util.List<SourceSystem> convertSourceSystemFromRs(java.sql.ResultSet rs) throws SQLException{
		java.util.List<SourceSystem> ret = new java.util.ArrayList<SourceSystem>();
		while(rs.next()){
			SourceSystem sourceSystem = new SourceSystem();
			sourceSystem.setDbCharset(rs.getString("db_charset"));
			sourceSystem.setDbIp(rs.getString("db_ip"));
			sourceSystem.setDbPort(rs.getString("db_port"));
			sourceSystem.setDbSchema(rs.getString("db_schema"));
			sourceSystem.setDbSid(rs.getString("db_sid"));
			sourceSystem.setDbType(rs.getString("db_type"));
			sourceSystem.setDbVersion(rs.getString("db_version"));
			sourceSystem.setEncrPassword(rs.getString("encrpassword"));
			sourceSystem.setPassWord(rs.getString("password"));
			sourceSystem.setRemark(rs.getString("remark"));
			sourceSystem.setUserName(rs.getString("username"));
			sourceSystem.setSys(rs.getString("sys"));
			sourceSystem.setSys_num(rs.getString("sys_num"));
			ret.add(sourceSystem);
		}
		return ret;
	}
	
	/**
	 * 获得所有SourceSystem的信息，并转换为Key-Value形式
	 * Key = schema + "," + sid + "," + sys
	 * Value = SourceSystem对象
	 * @param 
	 * @return 存储了所有SourceSystem对象的HashMap
	 * @throws SQLException 
	 */
	public java.util.HashMap<String, SourceSystem> getSourceSystemToMap() throws SQLException{
		java.util.HashMap<String, SourceSystem> result = new java.util.HashMap<>();
		
		java.util.List<SourceSystem> lstSourceSystem = this.getSourceSystem();
		
		for (SourceSystem sourceSystem : lstSourceSystem) {
			String sys = sourceSystem.getSys();
			String sid = sourceSystem.getDbSid();
			String schema = sourceSystem.getDbSchema();
			
			//srcTableSchema + "," + db_sid + "," + sys
			String key = schema + "," + sid + "," + sys ;
			
			if (!result.containsKey(key)) {
				result.put(key, sourceSystem);
			}
		}
		
		return result;
	}
}
