package com.moumou.bootmybatisdemo.dataAlignment.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;

public class SourceFieldLogDao {

	public void insert(String serialId, List<SourceField> lstSourceField, Connection conn) throws SQLException {
		// 插入多条记录到src_column_log
		if (null == lstSourceField || lstSourceField.size() == 0) {
			System.out.println("lstSourceField is Empty. Break Insert Into src_column_log.");
			return;
		}
		
		String sql = "insert into src_column_log(serial_id, sys, db_sid, db_schema, table_name, column_id, column_name, column_type, column_cn_name, is_pk, not_null, default_value, is_dk, break_flag) "
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		PreparedStatement pst =  conn.prepareStatement(sql);
		long count = 0;
		for (SourceField aSourceField : lstSourceField) {
			pst.clearParameters();
			pst.setString(1, serialId); //serial_id
			pst.setString(2, aSourceField.getSysString()); //sys
			pst.setString(3, aSourceField.getDb_sidString()); //db_sid
			pst.setString(4, aSourceField.getDb_schemaString()); //db_schema
			pst.setString(5, aSourceField.getTable_nameString()); //table_name
			pst.setInt(6, aSourceField.getColumn_id()); //column_id
			pst.setString(7, aSourceField.getColumn_nameString()); //column_name
			pst.setString(8, aSourceField.getColumn_typeString()); //column_type
			pst.setString(9, aSourceField.getColumn_cn_nameString()); //column_cn_name
			pst.setString(10, aSourceField.getIs_pkString()); //is_pk
			pst.setString(11, aSourceField.getNot_nullString()); //not_null
			pst.setString(12, aSourceField.getDefault_valueString()); //default_value
			pst.setString(13, aSourceField.getIs_dkString()); //is_dk
			pst.setString(14, aSourceField.getBreak_flag()); //break_flag
					
			count = count + pst.executeUpdate();
		}
		pst.close();
		
		System.out.println("insert into src_column_log affect rows:" + count);
	}

}
