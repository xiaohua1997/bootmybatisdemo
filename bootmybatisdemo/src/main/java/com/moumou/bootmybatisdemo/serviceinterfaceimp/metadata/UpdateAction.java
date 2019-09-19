package com.moumou.bootmybatisdemo.serviceinterfaceimp.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceFieldDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceFieldLogDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceSystemDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceTableDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.ColumnTest;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.DbcpUtil;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.MetaSrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;

public class UpdateAction {
	public static void main(String[] args) throws Exception {
		UpdateAction aUpdateAction = new UpdateAction();
		
		//MSSQL
//		String sys = "JZJY";
//		String sid = "run";
//		String schema = "dbo";
//		String tableName = "ITF_USERS";
//		//String tableName = "bb_contract";
		
		//MYSQL
//		String sys = "XTPB";
//		String sid = "ttmgrportal";
//		String schema = "ttmgrportal";
//		//String tableName = "idata_counterpartydetail"; //全新表
//		String tableName = "accountgroup_group"; //现存表
		
//		//ORACLE
//		String sys = "CPZX";
//		String sid = "pifdb1";
//		String schema = "KDETL";
//		String tableName = "ETL_WIND_PRO_NOTICE"; //全新表
//		//String tableName =  "ETL_OTC_TRD_ORDERS"; //现存表
//		
//		aUpdateAction.update(sys, sid, schema, tableName, null, null);
		
//		List<MetaSrcColumn> lst = aUpdateAction.getMetadata(sys, sid, schema, tableName);
//		System.out.println(lst.size());
//		for (MetaSrcColumn metaSrcColumn : lst) {
//			System.out.println(metaSrcColumn.toString());
//		}
		
		String sys = "HSTA";
		String sid = "tagzcs";
		String schema = "HSTA4";
		
		aUpdateAction.update(sys);
		System.out.println("完成");
	}
	
	public void update(String sys, String sid, String schema, String tableName, String tableCnName, String if_mark, String serialId) throws SQLException {		
		// 确定流水号，如果未传入流水号，构造流水号（TAB + 流水号）
		if (null == serialId) {
			serialId = "TAB" + getCurrentTimeTag();
		}
		
		// 获得该tableName在源系统中的元数据信息
		List<MetaSrcColumn> lstMetaCols =  this.getMetadata(sys, sid, schema, tableName);
		//-------------------DEBUG INFO---------------------------
//		System.out.println("源DBMS的元数据信息：");
//		for (MetaSrcColumn metaSrcColumn : lstMetaCols) {
//			System.out.println(metaSrcColumn.toString());
//		}
		
		//----------------------------------------------
		
		// 如果获得不到源系统中的元数据信息，就不应当继续往下做，否则会删除本来的信息
		if(null == lstMetaCols || lstMetaCols.size() == 0) {
			System.out.println("未获得源系统" + sys + "-" + sid + "-" + schema + "-" + tableName + "的元数据信息，跳过。");
			return;
		} else {
			System.out.println("获得源系统" + sys + "-" + sid + "-" + schema + "-" + tableName + "的元数据信息。");
		}
		
		// 获得该tableName在知识库中的元数据信息
		SourceTableDao aSourceTableDao = new SourceTableDao();
		boolean sourcetableExists = aSourceTableDao.exists_FromAll(sys, sid, schema, tableName); 
		
		SourceFieldDao aSourceFieldDao = new SourceFieldDao();
		List<SourceField> lstSourceField =  aSourceFieldDao.getFieldInfo(sys, sid, schema, tableName);
		//-------------------DEBUG INFO---------------------------
//		System.out.println("在知识库中的元数据信息：");
//		for (SourceField sourceField : lstSourceField) {
//			System.out.println(sourceField);
//		}
//		System.out.println();
		if (null != lstSourceField && lstSourceField.size() > 0) {
			System.out.println("获得知识库" + sys + "-" + sid + "-" + schema + "-" + tableName + "的元数据信息。");
		}
		//----------------------------------------------
		
		// 按照保留规则，生成出本次要更新的tableName元数据信息		
		// src_table：
		// 如果是新表，新增相关信息（带默认值）
		// 保留的 inc_cdt :增全量条件;if_mark：增全量标识(Y/N);is_put_to_etldb:是否采集 Y/N 
		// table_type：目前没使用; template_code ：目前未使用; 
		SourceTable newSourceTable = null;
		if (!sourcetableExists) {
			newSourceTable = new SourceTable();
			newSourceTable.setSyString(sys);
			newSourceTable.setDb_sidString(sid);
			newSourceTable.setTable_schemaString(schema);
			newSourceTable.setTable_nameString(tableName);
			newSourceTable.setTable_cn_nameString(tableCnName);
			newSourceTable.setInc_cdtString(null);
			newSourceTable.setIf_markString(if_mark);
			newSourceTable.setTable_typeString(null);
			newSourceTable.setTemplate_codeString(null);
			newSourceTable.setIs_put_to_etldbString("Y");
		}
		
		// src_column:
		// 对生成至关重要，一定会更新的：column_id, column_name, column_type
		// --不一定的：is_pk，not_null，default_value
		// 没用的：is_dk
		// 保留的：break_flag
		// 按照情况判断的：column_cn_name。获得的中文，如果现在知识库为空，则取得；
		//                 如果知识库不为空，判断获得的中文是否包含在知识库的字符串内，如果不包含的情况，拼接到知识库字符串后面
		List<SourceField> lstNewSourceField = mergeSourceFieldInfo(lstMetaCols, lstSourceField);
		if (null != lstNewSourceField && lstNewSourceField.size() > 0) {
			System.out.println("合成了新的" + sys + "-" + sid + "-" + schema + "-" + tableName + "的元数据信息。");
		}
		//-------------------DEBUG INFO---------------------------
//		System.out.println("合成的元数据信息：");
//		for (SourceField sourceField : lstNewSourceField) {
//			System.out.println(sourceField);
//		}
//		System.out.println();
//		System.out.println();
		//----------------------------------------------
		
		// 从连接池获取该系统连接
		Connection conn =  null;
		try {
			// 事务开始
			JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
			conn = jdbcConn.getDbConnection();
			conn.setAutoCommit(false);
			// 将该tableName在src_column中的元数据信息插入到src_column_log表中
			SourceFieldLogDao aSourceFieldLogDao = new SourceFieldLogDao();
			
			if(null != lstSourceField && lstSourceField.size() > 0) {
				aSourceFieldLogDao.insert(serialId, lstSourceField, conn);
			}
			
			// 如果是完全新增的table，增加记录到src_table表中
			if (null != newSourceTable) {
				aSourceTableDao.insert(newSourceTable, conn);
			}
		
			// 将该tableName在src_column中的元数据信息删除
			aSourceFieldDao.delete(sys, sid, schema, tableName, conn);
			
			// 插入本次tableName的新元数据信息到src_column
			aSourceFieldDao.insert(lstNewSourceField, conn);
			
			// 事务结束
			conn.commit();
		} catch (SQLException e) {
			if(null != conn) {
				System.out.println("ROLLBACK!!!");
				conn.rollback();
			}
			e.printStackTrace();
		} finally {
			if(null != conn && !conn.isClosed()) {
				conn.close();
			}
		}
		
	}
	
	private List<SourceField> mergeSourceFieldInfo(List<MetaSrcColumn> lstMetaCols, List<SourceField> lstSourceField) {
		// 按规则合并，得到新的源系统元数据信息
		List<SourceField> lstNewSourceField = new ArrayList<SourceField>();
		
		// lstSourceField 转换数据结构， key=字段名，value=对象
		Map<String, SourceField> dicSourceField = new HashMap<String, SourceField>();
		for (SourceField aSourceField : lstSourceField) {
			dicSourceField.put(aSourceField.getColumn_nameString(), aSourceField);
		}
		
		// src_column:
		// 对生成至关重要，一定会更新的：column_id, column_name, column_type
		// --更新：is_pk，not_null
		// 没用的：default_value，is_dk
		// 保留的：break_flag
		// 按照情况判断的：column_cn_name。获得的中文，如果现在知识库为空，则取得；
		//                 如果知识库不为空，判断获得的中文是否包含在知识库的字符串内，如果不包含的情况，拼接到知识库字符串后面
		for (MetaSrcColumn aMetaSrcColumn : lstMetaCols) {
			SourceField aNewSourceField = new SourceField();
			
			aNewSourceField.setSysString(aMetaSrcColumn.getSys());
			aNewSourceField.setDb_sidString(aMetaSrcColumn.getDbSid());
			aNewSourceField.setDb_schemaString(aMetaSrcColumn.getTableSchema());
			aNewSourceField.setTable_nameString(aMetaSrcColumn.getTableName());
			aNewSourceField.setColumn_nameString(aMetaSrcColumn.getColumnName());
			aNewSourceField.setColumn_id(Integer.parseInt(aMetaSrcColumn.getColumnId()));
			aNewSourceField.setColumn_typeString(aMetaSrcColumn.getColumnType());
			aNewSourceField.setIs_pkString(aMetaSrcColumn.getPk());
			aNewSourceField.setNot_nullString(aMetaSrcColumn.getNullable());
			
			aNewSourceField.setDefault_valueString(null);
			aNewSourceField.setIs_dkString(null);
			
			if(dicSourceField.containsKey(aMetaSrcColumn.getColumnName())) {
				SourceField oldSourceField = dicSourceField.get(aMetaSrcColumn.getColumnName());
				aNewSourceField.setBreak_flag(oldSourceField.getBreak_flag());
				
				String oldCnName = oldSourceField.getColumn_cn_nameString();
				if(null != oldCnName && !oldCnName.trim().isEmpty()) {
					if(oldCnName.contains(aMetaSrcColumn.getColumnDesc())) {
						aNewSourceField.setColumn_cn_nameString(oldCnName);
					} else {
						if(null != aMetaSrcColumn.getColumnDesc() && !aMetaSrcColumn.getColumnDesc().trim().isEmpty()) {
							aNewSourceField.setColumn_cn_nameString(oldCnName + "|" + aMetaSrcColumn.getColumnDesc());
						} else {
							aNewSourceField.setColumn_cn_nameString(oldCnName);
						}
					}
				} else {
					aNewSourceField.setColumn_cn_nameString(aMetaSrcColumn.getColumnDesc());
				}
			} else {
				aNewSourceField.setBreak_flag("N");
				aNewSourceField.setColumn_cn_nameString(aMetaSrcColumn.getColumnDesc());
			}
			
			
			lstNewSourceField.add(aNewSourceField);
		}
		
		
		return lstNewSourceField;
	}

	private String getCurrentTimeTag() {
		// 生成时间戳
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		return sdf.format(new Date());
	}

	private List<MetaSrcColumn> getMetadata(String sys, String sid, String schema, String tableName) throws SQLException {
		// 获得源系统元数据的方法，复用海涛代码
		List<MetaSrcColumn> metaSrcColumnList = new ArrayList<MetaSrcColumn>();
		
		Connection conn = null;
		try {
			conn = DbcpUtil.getConnection(sys, sid, schema);
			if (conn == null) {
				System.err.println("[ERROR]连接错误：" + sys + "-" + sid + "-" + schema);
				return metaSrcColumnList;
			}
	        DatabaseMetaData dmd = conn.getMetaData();
	        // 获得PK信息，并保存供后续比较使用
	        ResultSet rsPk = dmd.getPrimaryKeys(null, schema, tableName);
	        HashSet<String> hashPk = new HashSet<>();
	        while (rsPk.next()) {
	        	hashPk.add(rsPk.getString("COLUMN_NAME"));
	        }
	        rsPk.close();
	        // 获得其他信息
	        String key = sys + "," + sid + "," + schema;
	        SourceSystem sourceSystem = DbcpUtil.systemHashMaps.get(key);
	        if (sourceSystem.getDbType().equalsIgnoreCase("mssql")) {
	           ColumnTest.getColumnTypeMap(sys, sid, schema);
	            // TODO 这里有严重的连接性能问题，需改造
	        }
	        
	        ResultSet rs = dmd.getColumns(null, schema, tableName, null);
	        
	        int columnId = 1;
	        while (rs.next()) {
	            //每一条创建一个MetaSrcColumn对象存储
	            MetaSrcColumn metaSrcColumn = new MetaSrcColumn();
	            //sys
	            metaSrcColumn.setSys(sys);
	            //sid
	            String dbSid = rs.getString("TABLE_CAT");
	            if (dbSid == null) {
	                dbSid = sid;
	            }
	            metaSrcColumn.setDbSid(dbSid);
	            //表模式（可能为空）,在oracle中获取的是命名空间, mssql: db_schema
	            String tableSchema = rs.getString("TABLE_SCHEM");
	            if (tableSchema == null) {
	                tableSchema = schema;
	            }
	            metaSrcColumn.setTableSchema(tableSchema);
	            //表名
	            if (!DbcpUtil.systemHashMaps.get(sys + "," + sid + "," + schema).getDbSchema().equalsIgnoreCase(tableSchema)) {
	                continue;
	            } // TODO 这段if逻辑意义不明
	            String mt_TableName = rs.getString("TABLE_NAME");
	            if (mt_TableName == null) {
	            	mt_TableName = tableName;
	            }
	            metaSrcColumn.setTableName(mt_TableName);
	            //列名
	            String columnName = rs.getString("COLUMN_NAME");
	//            if (sourceSystem.getDbType().equalsIgnoreCase("mssql") || sourceSystem.getDbType().toLowerCase().contains("server")) {
	//                columnName = ColumnTest.getConvertType(columnName);
	//            } //这段if逻辑疑似没有意义，并且还可能导致错误的结果
	            metaSrcColumn.setColumnName(columnName);
	            //列描述
	            String columnDesc = rs.getString("REMARKS");
	            metaSrcColumn.setColumnDesc((null == columnDesc || columnDesc.trim().isEmpty()) ? "" : columnDesc);
	            
	            //java.sql.Types类型名称(列类型名称)
	            String columnType = rs.getString("TYPE_NAME");
	            //列大小
	            int columnSize = rs.getInt("COLUMN_SIZE");
	            //小数位数
	            int decimalDigits = rs.getInt("DECIMAL_DIGITS");
	
	            if (columnType.equalsIgnoreCase("text") || columnType.equalsIgnoreCase("xml")) {
	                metaSrcColumn.setColumnType(columnType);
	            } else{
	            	// add by wsy 除了SQLserver之外不应该进行转换
	            	if (sourceSystem.getDbType().equalsIgnoreCase("mssql")) {
	            		columnType = ColumnTest.getConvertType(columnType);
	            		// TODO 这里有严重的连接性能问题，需改造
	            	}
	            	
		            if (columnType.split(" ").length > 1) {
	                    columnType = columnType.split(" ")[0];
	                }
	                if (columnType.toLowerCase().contains("char")) {
	                    //字符类型需要带长度信息
	                    metaSrcColumn.setColumnType(columnType + "(" + columnSize + ")");
	                } else if (columnType.toLowerCase().contains("decimal")
	                        || columnType.toLowerCase().contains("number")
	                        || columnType.toLowerCase().contains("numeric")) {
	                    metaSrcColumn.setColumnType(columnType + "(" + columnSize + "," + decimalDigits + ")");
	
	                } else {
	                    //其余的直接放
	                    metaSrcColumn.setColumnType(columnType);
	                }
	            }
	            /**
	             *  0 (columnNoNulls) - 该列不允许为空
	             *  1 (columnNullable) - 该列允许为空
	             *  2 (columnNullableUnknown) - 不确定该列是否为空
	             */
	            int nullAble = rs.getInt("NULLABLE");  //是否允许为null
	            if (nullAble == 1) {
	                metaSrcColumn.setNullable("Y");
	            } else if (nullAble == 0) {
	                metaSrcColumn.setNullable("N");
	            }
	            
	            // 主键
	            boolean isPk = false;
	            if (hashPk.contains(columnName)) {
	                isPk = true;
	            }
	            if (isPk) {
	                metaSrcColumn.setPk("Y");
	            } else {
	            	metaSrcColumn.setPk("N");
	            }
	            
	            metaSrcColumn.setColumnId(columnId + "");
	            columnId++;
	            
	            metaSrcColumnList.add(metaSrcColumn);
	        }
		} finally {
			if (null != conn && !conn.isClosed()) {
				conn.close();
			}
		}
      
		return metaSrcColumnList;
	}

	public void update(String sys, String sid, String schema, String serialId) throws SQLException {
		// 获取该schema在知识库中的所有table列表
		SourceTableDao aSourceTableDao = new SourceTableDao();
		List<SourceTable> lstSourceTable = aSourceTableDao.getTableInfo_FromAll(sys, sid, schema);
		
		// 确定流水号，如果未传入流水号则生成流水号（SCM + 时间戳）
		if (null == serialId) {
			serialId = "SCM" + this.getCurrentTimeTag();
		} 
		
		for (SourceTable aSourceTable : lstSourceTable) {
			String tableName = aSourceTable.getTable_nameString();
			// 循环该列表，调用update(sys,sid,schema,tableName, serialId)
			update(sys, sid, schema, tableName, null, null, serialId);
		}

	}
	
	public void update(String sys) throws SQLException {
		// 获取该sys在知识库中的所有schema列表
		SourceSystemDao aSourceSystemDao = new SourceSystemDao();
		List<SourceSystem> lstSourceSystem = aSourceSystemDao.getSourceSystem(sys);
		// 生成流水号（SYS+时间戳）
		String serialId = "SYS" + this.getCurrentTimeTag();
		
		for (SourceSystem aSourceSystem : lstSourceSystem) {
			// 循环，调用update(sys, sid, schema, serialId)
			update(aSourceSystem.getSys(), aSourceSystem.getDbSid(), aSourceSystem.getDbSchema(), serialId);
		}
	}
	
	public void update() throws SQLException {
		// 获取所有schema列表
		SourceSystemDao aSourceSystemDao = new SourceSystemDao();
		List<SourceSystem> lstSourceSystem = aSourceSystemDao.getSourceSystem();
		// 生成流水号（ALL+时间戳）
		String serialId = "ALL" + this.getCurrentTimeTag();
		
		for (SourceSystem aSourceSystem : lstSourceSystem) {
			// 循环，调用update(sys, sid, schema, serialId)
			update(aSourceSystem.getSys(), aSourceSystem.getDbSid(), aSourceSystem.getDbSchema(), serialId);
		}
	}
}
