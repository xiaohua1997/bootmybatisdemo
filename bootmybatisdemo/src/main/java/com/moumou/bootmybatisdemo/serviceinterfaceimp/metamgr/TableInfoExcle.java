package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;

public class TableInfoExcle {

	static final String UNDERLINE = "_";
	static final String CURRENTDBNAME = "ODS";
	// ORACLE 和 impala 关键字
	private static HashSet<String> keyword = new HashSet<String>();
	public static HashMap<String, String> isPkMap = new HashMap<String, String>();
	HashMap<String, String> hashMapImpalaConvert = new HashMap<String, String>();

	static {
		String sqlString = "SELECT keyword FROM src_column,etl_keyword where src_column.column_name = etl_keyword.keyword GROUP BY keyword";
		// 查询那些表有主键
		String sqlStringIsPk = "select table_name,is_pk from src_column  where is_pk = 'Y' group by table_name ,sys ";
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		ResultSet resultSet = null;
		ResultSet resultSetIsPk = null;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			PreparedStatement preparedStatementIsPk = connection
					.prepareStatement(sqlStringIsPk);
			resultSet = preparedStatement.executeQuery();
			resultSetIsPk = preparedStatementIsPk.executeQuery();

			while (resultSet.next()) {

				String keyword = resultSet.getString(1);
				TableInfoExcle.keyword.add(keyword.toLowerCase());
			}
			while (resultSetIsPk.next()) {
				String tableNamePk = resultSetIsPk.getString(1);
				String isPk = resultSetIsPk.getString(2);
				TableInfoExcle.isPkMap.put(tableNamePk.toLowerCase(), isPk);
			}
			// srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
	}

	// 存储表名和列名
	HashMap<String, String> typeConvertMap = new HashMap<String, String>();

	/**
	 * 表级 启动方法    作用：内部循环调用qryTableInfo(srctypeString1, tgttypeString1); 将各系统表级信息汇总
	 * 传入 % 获取全部表 否则按照传入的系统操作
	 * @return  表级信息Map集合
	 */
	public Map<String, List<String[]>> qryTableInfoGather(String sys) {
		String sqlString;
		if("%" != sys){
			sqlString = "SELECT sys, db_schema from src_column WHERE sys= \'"+ sys +"\' GROUP BY sys,db_schema";
		}else{
			sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
		}
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		ResultSet resultSet = null;
		List<String[]> qryTableInfoTableOracleAll = new ArrayList<>();
		List<String[]> qryTableInfoTableImpalaAll = new ArrayList<>();
		Map<String, List<String[]>> qryTableInfoTableAll = new HashMap<String, List<String[]>>();
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String srctypeString1 = resultSet.getString(1);
				String tgttypeString1 = resultSet.getString(2);
				//查询每个系统的表级信息
				Map<String, List<String[]>> qryTableInfoTable = new TableInfoExcle().qryTableInfo(srctypeString1, tgttypeString1);
				List<String[]> dataList = qryTableInfoTable.get("oracle");
				List<String[]> dataListImpala = qryTableInfoTable.get("impala");
				qryTableInfoTableOracleAll.addAll(dataList);
				qryTableInfoTableImpalaAll.addAll(dataListImpala);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
		qryTableInfoTableAll.put("oracle", qryTableInfoTableOracleAll);
		qryTableInfoTableAll.put("impala", qryTableInfoTableImpalaAll);
		return qryTableInfoTableAll;
	}
	
	/**
	 *   查询表级数据字典 
	 *   根据传入的sys和db_schema查询各系统的表级信息
	 *   此方法将所有表级信息处理完毕（只需要处理表名）
	 * @param srctypeString sys
	 * @param tgttypeString db_schema
	 * @return tableInfoAllMap 表级信息
	 */
	public Map<String, List<String[]>> qryTableInfo(String srctypeString,
			String tgttypeString) {
		String sqlString1 = "SELECT * from src_column where sys='"
				+ srctypeString + "' and db_schema='" + tgttypeString
				+ "' ORDER BY sys,db_schema,column_id asc;";
		//查询oracle的表级信息
		HashMap<String, List<SourceField>> tableDes = new TableInfoExcle().getTableDes("mysql", "oracle", sqlString1);
		//查询impala的表级信息
		HashMap<String, List<SourceField>> tableDesImpala = new TableInfoExcle().getTableDesImpala("mysql,", "hive", sqlString1);
		String sqlString = "select scl.table_name as '表英文名', stb.table_cn_name as '中文名', case when stb.inc_cdt is null then '快照' else '增量' end,case when stb.sys is null then scl.sys else stb.sys end 'sys' from src_column scl left join src_table stb on stb.table_name = scl.table_name and stb.sys=scl.sys where  scl.sys='"
				+ srctypeString
				+ "' and scl.db_schema='"
				+ tgttypeString
				+ "' GROUP BY scl.table_name,stb.table_name,stb.sys,scl.sys ";
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		ResultSet resultSet = null;
		Connection connection = jdbcConn.getDbConnection();
		String tableName = "";
		String tableCnName = "";
		String tableMold = "";
		String sys;
		HashMap<String, String> sysConvert = getSys();
		List<String[]> dataList = new ArrayList<String[]>();
		List<String[]> dataListImpala = new ArrayList<String[]>();
		Map<String, List<String[]>> tableInfoAllMap = new HashMap<String, List<String[]>>();
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				// sys系统缩写
				sys = resultSet.getString(4);
				// 表名
				tableName = resultSet.getString(1);
				String afterTableName = sys + UNDERLINE + gainTableNameIndex(tableDes, tableName) + tableName;
				String afterTableNameImpala = getTableNameImpala(tableDesImpala, tableName);
				// 表中文名
				tableCnName = resultSet.getString(2);
				// 表类型（快照，增量等）
				tableMold = resultSet.getString(3);
				if (isPkMap.containsKey(tableName)) {
					dataList.add(new String[] { "数据仓库", "EDW", "ODS",
							afterTableName, tableCnName, "", tableMold, "是" });
					dataListImpala.add(new String[] { "数据仓库", "EDW", "ODS",
							afterTableNameImpala, tableCnName, "", tableMold,
							"是" });

				} else {
					dataList.add(new String[] { "数据仓库", "EDW", "ODS",
							afterTableName, tableCnName, "", tableMold, "" });
					dataListImpala.add(new String[] { "数据仓库", "EDW", "ODS",
							afterTableNameImpala, tableCnName, "", tableMold,
							"" });
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
		tableInfoAllMap.put("oracle", dataList);
		tableInfoAllMap.put("impala", dataListImpala);
		return tableInfoAllMap;
	}

	/**
	 * 获取到一个表的描述包含列信息
	 * 
	 * @param srcString	源系统
	 * @param tgtString	目标系统
	 * @param sqlString	查询的sql语句createTableToday传进来的
	 * @return HashMap<String, List<SourceField>> key : String value:
	 *         List<SourceField>
	 */
	public HashMap<String, List<SourceField>> getTableDes(String srcString,
			String tgtString, String sqlString) {
		String sqlString2 = "SELECT src_column_type,tgt_column_type FROM etl_type_convert WHERE src_db_type='"
				+ srcString + "' and tgt_db_type='" + tgtString + "';";
		HashMap<String, String> tableMap = getConvertTable();
		HashMap<String, String> columnMap = getConvertColumn();
		HashMap<String, String> scheaMap = getConvertTableSchema();

		HashMap<String, List<SourceField>> StringAndListSourceFieldMap = new HashMap<String, List<SourceField>>();
		ResultSet resultSet = null;
		ResultSet resultSet1 = null;
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();

		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			PreparedStatement preparedStatement1 = connection
					.prepareStatement(sqlString2);
			resultSet1 = preparedStatement1.executeQuery();
			while (resultSet1.next()) {
				String srctypeString1 = resultSet1.getString(1);
				String tgttypeString1 = resultSet1.getString(2);
				typeConvertMap.put(srctypeString1.trim().toLowerCase(),
						tgttypeString1.trim().toLowerCase());
			}
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String sysString = resultSet.getString("sys");
				String db_sidString = resultSet.getString("db_sid");
				String db_schemaString = resultSet.getString("db_schema");
				String table_nameString = resultSet.getString("table_name");
				int column_id = resultSet.getInt("column_id");
				String column_nameString = resultSet.getString("column_name");
				String column_typeString = resultSet.getString("column_type");
				String column_cn_nameString = resultSet
						.getString("column_cn_name");
				String is_pkString = resultSet.getString("is_pk");
				String not_nullString = resultSet.getString("not_null");
				String default_valueString = resultSet
						.getString("default_value");
				String is_dkString = resultSet.getString("is_dk");
				String break_flagString = resultSet.getString("break_flag");
				List<SourceField> sourceFields = new ArrayList<SourceField>();
				// 转换表和列
				// 转换表的校验语句
				String tablenamecheckString = table_nameString.toLowerCase()
						+ "," + sysString;
				// 转化列的校验语句
				String columncheckString = column_nameString + ","
						+ table_nameString + "," + sysString;
				// 转换单词校验srcTableSchema + "," + db_sid + "," + sys
				String schemacheckString = db_schemaString + "," + db_sidString
						+ "," + sysString;
				if (tableMap.containsKey(tablenamecheckString)) {
					table_nameString = tableMap.get(tablenamecheckString);
				}
				if (table_nameString.length() > 22) {
					System.out.println(table_nameString + "-----" + sysString);
				}
				String scheam = "";
				if (scheaMap.containsKey(schemacheckString)) {
					scheam = scheaMap.get(schemacheckString);
				}
				String keyString = table_nameString + "," + scheam;
				if (keyword.contains(column_nameString.toLowerCase())) {
					column_nameString = column_nameString + "_ora";
				}
				if (columnMap.containsKey(columncheckString)) {
					column_nameString = columnMap.get(columncheckString);
				}
				SourceField sourceField = new SourceField(sysString,
						db_sidString, db_schemaString, table_nameString,
						column_id, column_nameString, column_typeString,
						column_cn_nameString, is_pkString, not_nullString,
						default_valueString, is_dkString, break_flagString);
				if (StringAndListSourceFieldMap.containsKey(keyString)) {
					List<SourceField> sourceFieldList = StringAndListSourceFieldMap
							.get(keyString);
					for (SourceField field : sourceFieldList) {
						sourceFields.add(field);
					}
					sourceFields.add(sourceField);
					StringAndListSourceFieldMap.put(keyString, sourceFields);
				} else {
					sourceFields.add(sourceField);
					StringAndListSourceFieldMap.put(keyString, sourceFields);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			jdbcConn.closeDbConnection();
		}
		return StringAndListSourceFieldMap;
	}
	
	/**
	 * 	获取到一个表的描述包含列信息
	 * 	方法中包含源系统到目标系统的类型转换
	 * 
	 * @param srcString	源系统 
	 * @param tgtString	目标系统  
	 * @param sqlString	查询的sql语句createTableToday传进来的
	 * @return HashMap<String, List<SourceField>> 返回处理完毕的表和字段信息，导出excel只用到了表名
	 */
	public HashMap<String, List<SourceField>> getTableDesImpala(String srcString, String tgtString, String sqlString) {
		String sqlString2 = "SELECT src_column_type,tgt_column_type FROM etl_type_convert WHERE src_db_type='"
				+ srcString + "' and tgt_db_type='" + tgtString + "';";
		HashMap<String, String> hashMap = new HashMap<String, String>();
		HashMap<String, String> sysConvert = getSys();
		HashMap<String, List<SourceField>> StringAndListSourceFieldMap = new HashMap<String, List<SourceField>>();
		ResultSet resultSet = null;
		ResultSet resultSet1 = null;
		List<String> list = new ArrayList<String>();
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			PreparedStatement preparedStatement1 = connection
					.prepareStatement(sqlString2);
			resultSet1 = preparedStatement1.executeQuery();
			while (resultSet1.next()) {
				String srctypeString1 = resultSet1.getString(1);
				String tgttypeString1 = resultSet1.getString(2);
				hashMapImpalaConvert.put(srctypeString1.toLowerCase(),
						tgttypeString1.toLowerCase());
			}
			hashMapImpalaConvert.put("numeric", "double");
			hashMapImpalaConvert.put("varchar2", "string");
			hashMapImpalaConvert.put("tinyint", "SMALLINT");
			hashMapImpalaConvert.put("integer", "int");
			hashMapImpalaConvert.put("cprice", "double");
			hashMapImpalaConvert.put("cmoney", "double");
			hashMapImpalaConvert.put("faild", "string");
			hashMapImpalaConvert.put("xml", "string");
			hashMapImpalaConvert.put("string", "string");
			hashMapImpalaConvert.put("bigdecimal", "decimal");
			hashMapImpalaConvert.put("date", "string");
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String sysString = resultSet.getString("sys");
				String db_sidString = resultSet.getString("db_sid");
				String db_schemaString = resultSet.getString("db_schema");
				String table_nameString = resultSet.getString("table_name");
				int column_id = resultSet.getInt("column_id");
				String column_nameString = resultSet.getString("column_name");
				String column_typeString = resultSet.getNString("column_type");
				String column_cn_nameString = resultSet
						.getNString("column_cn_name");
				String is_pkString = resultSet.getNString("is_pk");
				String not_nullString = resultSet.getNString("not_null");
				String default_valueString = resultSet
						.getNString("default_value");
				String is_dkString = resultSet.getNString("is_dk");
				String break_flagString = resultSet.getString("break_flag");
				List<SourceField> sourceFields = new ArrayList<SourceField>();

				String syscheckString = sysString.toLowerCase()
						+ db_schemaString.toLowerCase();
				if (sysConvert.containsKey(syscheckString)) {
					String syscheck = sysConvert.get(syscheckString);
					// System.out.println(syscheck);
					String sys_num = syscheck.split(",")[0];
					String db_type = syscheck.split(",")[1];
					if (db_type.startsWith("mssql")) {
						if (db_schemaString.equals("dbo")) {
							table_nameString = sys_num + "_" + sysString + "_"
									+ db_sidString + "_" + table_nameString;
						} else {
							table_nameString = sys_num + "_" + sysString + "_"
									+ db_sidString + "_" + db_schemaString
									+ "_" + table_nameString;
						}
					}
					if (db_type.startsWith("mysql")) {
						table_nameString = sys_num + "_" + sysString + "_"
								+ db_schemaString + "_" + table_nameString;
					}
					if (db_type.startsWith("oracle")) {
						table_nameString = sys_num + "_" + sysString + "_"
								+ db_schemaString + "_" + table_nameString;
					}
				}

				column_nameString = OracleColumnInfoExcel.getConvertKeyWord(column_nameString);
				SourceField sourceField = new SourceField(sysString,
						db_sidString, db_schemaString, table_nameString,
						column_id, column_nameString, column_typeString,
						column_cn_nameString, is_pkString, not_nullString,
						default_valueString, is_dkString, break_flagString);
				if (StringAndListSourceFieldMap.containsKey(table_nameString)) {
					List<SourceField> sourceFieldList = StringAndListSourceFieldMap
							.get(table_nameString);
					for (SourceField field : sourceFieldList) {
						sourceFields.add(field);
					}
					sourceFields.add(sourceField);
					StringAndListSourceFieldMap.put(table_nameString,
							sourceFields);
				} else {
					sourceFields.add(sourceField);
					StringAndListSourceFieldMap.put(table_nameString,
							sourceFields);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			jdbcConn.closeDbConnection();
		}
		return StringAndListSourceFieldMap;
	}
	
	/**
	 * 用于拼接oracle表名
	 * 获取oracle表名中的序号 例 JZJY_1_xxx 中的1
	 * 
	 * @param map 包含序号信息的map
	 * @param afTableName 源表名 
	 * @return	序号
	 */
	public String gainTableNameIndex(HashMap<String, List<SourceField>> map, String afTableName) {
		Iterator<Map.Entry<String, List<SourceField>>> iterator = map
				.entrySet().iterator();
		String schema = "";
		while (iterator.hasNext()) {
			Map.Entry<String, List<SourceField>> entry = iterator.next();
			String key = entry.getKey();
			String[] arr = key.split(",");
			String tableName = arr[0];
			schema = "";
			if (arr.length > 1) {
				if (afTableName.toLowerCase().contains(tableName.toLowerCase())) {
					schema = arr[1];
					return schema + UNDERLINE;
				}
			}
		}
		return schema;
	}
	
	/**
	 * 根据源表名取到impala表名
	 * 
	 * @param tableDesImpala 包含需要转换成impala表名的相关信息  (序号，库名等)
	 * @param tableName 源表名
	 * @return impala表名
	 */
	public String getTableNameImpala(
			HashMap<String, List<SourceField>> tableDesImpala, String tableName) {
		Iterator<Map.Entry<String, List<SourceField>>> iterator = tableDesImpala
				.entrySet().iterator();
		String tableNameImpala = "";
		while (iterator.hasNext()) {
			Map.Entry<String, List<SourceField>> entry = iterator.next();
			tableNameImpala = entry.getKey();
			if (tableNameImpala.toLowerCase().contains(tableName.toLowerCase())) {
				return "ods_" + tableNameImpala;
			}
		}
		return tableName + "表名转换出错";
	}
	
	/**
	 * 获取数据库类型的数据并保存在map中
	 * @return
	 */
	public HashMap<String, String> getSys() {
		HashMap<String, String> sysConvert = new HashMap<String, String>();
		String sqlString = "SELECT sys,sys_num,db_type,db_sid,db_schema from src_system";
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String sys = resultSet.getString(1);
				String sys_num = resultSet.getString(2);
				String db_type = resultSet.getString(3);
				String db_sid = resultSet.getString(4);
				String db_scheam = resultSet.getString(5);
				sysConvert.put(sys.toLowerCase() + db_scheam.toLowerCase(),
						sys_num + "," + db_type);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
		return sysConvert;

	}
	
	/**
	 * 获取转换表中 关于表的数据 src_tablename_convert 注意对比的时候表名为小写
	 * 
	 * @return
	 */
	public HashMap<String, String> getConvertTable() {
		HashMap<String, String> tableConvert = new HashMap<String, String>();

		String sqlString = "SELECT * from src_tablename_convert";
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String srctable = resultSet.getString(1);
				String tgttable = resultSet.getString(2);
				String sys = resultSet.getString(3);
				tableConvert.put(srctable.toLowerCase() + "," + sys, tgttable);
				// System.out.println(srctable + "------" + tgttable +
				// "----------" + sys);
			}
			// srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
		return tableConvert;

	}

	/**
	 * 获取转换表中关于列的的数据并保存在map中 etl_column_convert
	 * 
	 * @return
	 */
	public HashMap<String, String> getConvertColumn() {
		HashMap<String, String> columnConvert = new HashMap<String, String>();

		String sqlString = "SELECT * from etl_column_convert";
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		ResultSet resultSet = null;
		
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String srccolumn = resultSet.getString(1);
				String tgtcolumn = resultSet.getString(2);
				String table = resultSet.getString(3);
				String sys = resultSet.getString(4);
				columnConvert.put(srccolumn + "," + table + "," + sys,
						tgtcolumn.toLowerCase());
			}
			// srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
		return columnConvert;

	}

	/**
	 * src_tabschema_convert
	 * 
	 * @return
	 */
	public HashMap<String, String> getConvertTableSchema() {
		HashMap<String, String> tableConvert = new HashMap<String, String>();
		// 查找语句
		String sqlString = "SELECT * from src_tabschema_convert";
		JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456",
				"192.10.30.15", "3306", "edw_dev", "mysql");
		Connection connection = jdbcConn.getDbConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlString);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String srcTableSchema = resultSet.getString(1);
				String db_sid = resultSet.getString(2);
				String sys = resultSet.getString(3);
				String tgtTableSchema = resultSet.getString(4);

				tableConvert.put(srcTableSchema + "," + db_sid + "," + sys,
						tgtTableSchema);
			}
			// srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jdbcConn.closeDbConnection();
		}
		return tableConvert;

	}

}
