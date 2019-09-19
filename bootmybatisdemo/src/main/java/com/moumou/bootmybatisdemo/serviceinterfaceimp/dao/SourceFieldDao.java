package com.moumou.bootmybatisdemo.serviceinterfaceimp.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.DbcpUtil;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;

public class SourceFieldDao {



    public List<SourceField> getFieldInfo(String prefixName) {
        String sqlString = "select * from src_column where sys='" + prefixName + "'"
        				+ " order by sys asc, db_sid asc, db_schema asc, table_name asc, column_id asc" ;
        return getList(sqlString);

    }


    public List<SourceField> getFieldInfo(String prefixName, String tableName) {
        String sqlString = "select * from src_column where sys='" + prefixName + "' and table_name='" + tableName + "'"
        				+ " order by sys asc, db_sid asc, db_schema asc, table_name asc, column_id asc";
        return getList(sqlString);

    }

    public List<SourceField> getFieldInfo(String sys, String dbsid, String dbschema) {
        String sqlString = "select * from src_column where sys='" + sys + "' and db_sid='" + dbsid + "' and db_schema='" + dbschema + "' group by table_name,column_name" //TODO：group by的意义？有问题就应该报错
        				+ " order by sys asc, db_sid asc, db_schema asc, table_name asc, column_id asc";
        return getList(sqlString);

    }


    public List<SourceField> getFieldInfo(String sys, String dbsid, String dbschema,String tableName) {
        String sqlString = "select * from src_column where sys='" + sys + "' and db_sid='" + dbsid + "' and db_schema='" + dbschema + "' and table_name='"+ tableName +"' group by table_name,column_name" //TODO：group by的意义？有问题就应该报错
        				+ " order by sys asc, db_sid asc, db_schema asc, table_name asc, column_id asc";
        return getList(sqlString);

    }


    private List<SourceField> getList(String sqlString) {
        Connection conn = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        List<SourceField> list = new ArrayList<SourceField>();
        try {
            JdbcConnection jdbcConn = new JdbcConnection("edw","edw123456","192.10.30.15","3306","edw_dev","mysql");
            conn = jdbcConn.getDbConnection();
            preparedStatement = conn.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String sysString = resultSet.getString(1);
                String db_sidString = resultSet.getString(2);
                String db_schemaString = resultSet.getString(3);
                String table_nameString = resultSet.getString(4);
                int column_id = resultSet.getInt(5);
                String column_nameString = resultSet.getString(6);
                String column_typeString = resultSet.getNString(7);
                String column_cn_nameString = resultSet.getNString(8);
                String is_pkString = resultSet.getNString(9);
                String not_nullString = resultSet.getNString(10);
                String default_valueString = resultSet.getNString(11);
                String is_dkString = resultSet.getNString(12);
                String break_flag = resultSet.getNString(13);
                SourceField sourceField = new SourceField(sysString, db_sidString, db_schemaString, table_nameString, column_id, column_nameString, column_typeString, column_cn_nameString, is_pkString, not_nullString, default_valueString, is_dkString, break_flag);
                list.add(sourceField);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                    preparedStatement.close();
                    conn.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
        return list;

    }


	public void delete(String sys, String sid, String schema, String tableName, Connection conn) throws SQLException {
		// 按主键删除src_column中的记录
		String sql = "delete from src_column where sys=? and db_sid=? and db_schema=? and table_name=?";
		PreparedStatement pst = conn.prepareStatement(sql);
		pst.setString(1, sys);
		pst.setString(2, sid);
		pst.setString(3, schema);
		pst.setString(4, tableName);
		
		System.out.println("delete from src_column affect rows:" + pst.executeUpdate());
		
		pst.close();
		
	}


	public void insert(List<SourceField> lstNewSourceField, Connection conn) throws SQLException {
		// 插入多条记录到src_column
		if (null == lstNewSourceField || lstNewSourceField.size() == 0) {
			System.out.println("lstNewSourceField is Empty. Break Insert Into src_column.");
			return;
		}
		
		String sql = "insert into src_column(sys, db_sid, db_schema, table_name, column_id, column_name, column_type, column_cn_name, is_pk, not_null, default_value, is_dk, break_flag)"
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = conn.prepareStatement(sql);
		
		long count = 0;
		for (SourceField aSourceField : lstNewSourceField) {
			pst.clearParameters();
			pst.setString(1, aSourceField.getSysString()); //sys
			pst.setString(2, aSourceField.getDb_sidString()); //db_sid
			pst.setString(3, aSourceField.getDb_schemaString()); //db_schema
			pst.setString(4, aSourceField.getTable_nameString()); //table_name
			pst.setInt(5, aSourceField.getColumn_id()); //column_id
			pst.setString(6, aSourceField.getColumn_nameString()); //column_name
			pst.setString(7, aSourceField.getColumn_typeString()); //column_type
			pst.setString(8, aSourceField.getColumn_cn_nameString()); //column_cn_name
			pst.setString(9, aSourceField.getIs_pkString());  //is_pk
			pst.setString(10, aSourceField.getNot_nullString()); //not_null
			pst.setString(11, aSourceField.getDefault_valueString()); //default_value
			pst.setString(12, aSourceField.getIs_dkString()); //is_dk
			pst.setString(13, aSourceField.getBreak_flag()); //break_flag
			
			count = count + pst.executeUpdate();
		}
		
		System.out.println("insert into src_column affect rows:" + count);
		pst.close();
	}
}
