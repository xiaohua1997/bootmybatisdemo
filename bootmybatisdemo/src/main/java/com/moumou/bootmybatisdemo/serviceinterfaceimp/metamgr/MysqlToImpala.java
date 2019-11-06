package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;
import com.moumou.bootmybatisdemo.dataAlignment.model.ColumnDescInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;
import com.moumou.bootmybatisdemo.dataAlignment.model.TableInfo;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;

public class MysqlToImpala {


    HashSet<String> colset = new HashSet<String>();

    static final String STRING = "${src_schema}";

    private String ddlDir;

    private String dmlDir;

    
    HashMap<String, String> hashMap1 = new HashMap<String, String>();
    String tableString = "";

    public MysqlToImpala(String ddlDir, String dmlDir) {
    	this.ddlDir = ddlDir;
    	this.dmlDir = dmlDir;
    }

    //（全部系统）循环创建hive建表语句  内部调用createTableScript("mysql", "hive", srctypeString1,tgttypeString1);
    public void sqlCreateFile(String level) {
        String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        //MysqlToImpala mysqlToImpala = new MysqlToImpala();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String srctypeString1 = resultSet.getString(1);
                String tgttypeString1 = resultSet.getString(2);
                //System.out.println("\"" + srctypeString1 + "," + tgttypeString1 +"\"");
                this.createTableScriptAll("mysql", "hive", srctypeString1, tgttypeString1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }
    //(指定系统)循环创建hive建表语句  内部调用createTableScript("mysql", "hive", srctypeString1,tgttypeString1);
    public void sqlCreateFile(String level, String sys) {
    	String sqlString = "SELECT sys, db_schema from src_column where sys='"+sys+"' GROUP BY sys,db_schema";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//MysqlToImpala mysqlToImpala = new MysqlToImpala();
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			//System.out.println("\"" + srctypeString1 + "," + tgttypeString1 +"\"");
    			this.createTableScript("mysql", "hive", srctypeString1, tgttypeString1);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    //（指定schema）循环创建hive建表语句  内部调用createTableScript("mysql", "hive", srctypeString1,tgttypeString1);
    public void sqlCreateFile(String level, String sys, String sid, String schema) {
    	String sqlString = "SELECT sys, db_schema, db_sid from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' GROUP BY sys,db_schema,db_sid";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//MysqlToImpala mysqlToImpala = new MysqlToImpala();
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			//System.out.println("\"" + srctypeString1 + "," + tgttypeString1 +"\"");
    			this.createTableScript("mysql", "hive", srctypeString1, tgttypeString1, sidtypeString1);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    //（指定表名）循环创建hive建表语句  内部调用createTableScript("mysql", "hive", srctypeString1,tgttypeString1);
    public void sqlCreateFile(String level, String sys, String sid, String schema, String tableName) {
    	String sqlString = "SELECT sys, db_schema, db_sid, table_name from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' and table_name='"+tableName+"' GROUP BY sys,db_schema,db_sid,table_name";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//MysqlToImpala mysqlToImpala = new MysqlToImpala();
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			String tabletypeString1 = resultSet.getString(4);
    			//System.out.println("\"" + srctypeString1 + "," + tgttypeString1 +"\"");
    			this.createTableScript("mysql", "hive", srctypeString1, tgttypeString1, sidtypeString1, tabletypeString1);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }

    //(指定全部系统)循环创建sqoop语句  内部调用createTableLoadScript(srctypeString1, tgttypeString1);
    public void sqlSelectFile(String level) {
        String sqlString = "select sys, db_sid, db_schema from src_system";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        //MysqlToImpala mysqlToImpala = new MysqlToImpala();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String sys = resultSet.getString(1);
                String sid = resultSet.getString(2);
                String schema = resultSet.getString(3);
                this.createTableLoadScriptAll(sys, sid, schema);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }
    //(指定系统)循环创建sqoop语句  内部调用createTableLoadScript(srctypeString1, tgttypeString1);
    public void sqlSelectFile(String level, String sys) {
    	String sqlString = "select sys, db_sid, db_schema from src_system where sys='"+sys+"'";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//MysqlToImpala mysqlToImpala = new MysqlToImpala();
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			String sys1 = resultSet.getString(1);
    			String sid = resultSet.getString(2);
    			String schema = resultSet.getString(3);
    			this.createTableLoadScriptSid(sys1, sid, schema);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    //(指定schema)循环创建sqoop语句  内部调用createTableLoadScript(srctypeString1, tgttypeString1);
    public void sqlSelectFile(String level, String sys, String sid, String schema) {
    	String sqlString = "select sys, db_sid, db_schema from src_system where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"'";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//MysqlToImpala mysqlToImpala = new MysqlToImpala();
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			String sys1 = resultSet.getString(1);
    			String sid1 = resultSet.getString(2);
    			String schema1 = resultSet.getString(3);
    			this.createTableLoadScriptSchema(sys1, sid1, schema1);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    //(指定表名)循环创建sqoop语句  内部调用createTableLoadScript(srctypeString1, tgttypeString1);
    public void sqlSelectFile(String level, String sys, String sid, String schema, String tableName) {
    	String sqlString = "SELECT sys, db_schema, db_sid, table_name from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' and table_name='"+tableName+"' GROUP BY sys,db_schema,db_sid,table_name";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//MysqlToImpala mysqlToImpala = new MysqlToImpala();
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			String sys1 = resultSet.getString(1);
    			String schema1 = resultSet.getString(2);
    			String sid1 = resultSet.getString(3);
    			String table_name1 = resultSet.getString(4);
    			this.createTableLoadScriptTable(sys1, schema1, sid1, table_name1);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }

    //生成指定prefixname和schemaName 的文件
    //内部调用getTableDes(srcString,tgtString,sqlString);
    //getSqlString(list,prefixName);（指定全部）
    private void createTableScriptAll(String srcString, String tgtString, String prefixName, String schemaName) {
        String sqlString = "select * from src_column t1"
        				+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
        				+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
        				+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
        //String sqlString = "SELECT * from src_column where sys='HSPB' and db_schema='trade' ORDER BY sys,db_schema,column_id desc;";

        HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, tgtString, sqlString);
        //System.out.println(list);
        List<TableInfo> tableInfos = getSqlString(stringListHashMap, prefixName);
//		for (Object object : createTableSql) {
//			System.out.println(object.toString());
//		}
        BufferedWriter bw = null;
        try {


            for (TableInfo tableInfo : tableInfos) {
                String tableName = tableInfo.getTableName();
                String sql = tableInfo.getInfo();
                String pathName = ddlDir + "/" + StringExtension.toStyleString(prefixName.trim())
                        + "/" + "ods" + "_" + StringExtension.toStyleString(tableName) + ".sql";
                File file = new File(pathName);
                File fileParent = file.getParentFile();

                if (!fileParent.exists()) {
                    fileParent.mkdirs();
                }


                // if file doesnt exists, then create it
                if (!file.exists()) {
                    //System.out.println(file.getParent());
                    //System.out.println(file.getName());
                    file.createNewFile();
                }

                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                // System.out.println(object.toString());
                bw = new BufferedWriter(fw);
                bw.write(sql);
                bw.close();

            }


            System.out.println("Done");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }
    //(指定系统)
    private void createTableScript(String srcString, String tgtString, String prefixName, String schemaName) {
    	String sqlString = "select * from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	//String sqlString = "SELECT * from src_column where sys='HSPB' and db_schema='trade' ORDER BY sys,db_schema,column_id desc;";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, tgtString, sqlString);
    	//System.out.println(list);
    	List<TableInfo> tableInfos = getSqlString(stringListHashMap, prefixName);
//		for (Object object : createTableSql) {
//			System.out.println(object.toString());
//		}
    	BufferedWriter bw = null;
    	try {
    		
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String tableName = tableInfo.getTableName();
    			String sql = tableInfo.getInfo();
    			String pathName = ddlDir + "/" + StringExtension.toStyleString(prefixName.trim())
    			+ "/" + "ods" + "_" + StringExtension.toStyleString(tableName) + ".sql";
    			File file = new File(pathName);
    			File fileParent = file.getParentFile();
    			
    			if (!fileParent.exists()) {
    				fileParent.mkdirs();
    			}
    			
    			
    			// if file doesnt exists, then create it
    			if (!file.exists()) {
    				//System.out.println(file.getParent());
    				//System.out.println(file.getName());
    				file.createNewFile();
    			}
    			
    			FileWriter fw = new FileWriter(file.getAbsoluteFile());
    			// System.out.println(object.toString());
    			bw = new BufferedWriter(fw);
    			bw.write(sql);
    			bw.close();
    			
    		}
    		
    		
    		System.out.println("Done");
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    //(指定schema)
    private void createTableScript(String srcString, String tgtString, String prefixName, String schemaName, String sidtypeString1) {
    	String sqlString = "select * from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='"+sidtypeString1+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	//String sqlString = "SELECT * from src_column where sys='HSPB' and db_schema='trade' ORDER BY sys,db_schema,column_id desc;";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, tgtString, sqlString);
    	//System.out.println(list);
    	List<TableInfo> tableInfos = getSqlString(stringListHashMap, prefixName);
//		for (Object object : createTableSql) {
//			System.out.println(object.toString());
//		}
    	BufferedWriter bw = null;
    	try {
    		
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String tableName = tableInfo.getTableName();
    			String sql = tableInfo.getInfo();
    			String pathName = ddlDir + "/" + StringExtension.toStyleString(prefixName.trim())
    			+ "/" + "ods" + "_" + StringExtension.toStyleString(tableName) + ".sql";
    			File file = new File(pathName);
    			File fileParent = file.getParentFile();
    			
    			if (!fileParent.exists()) {
    				fileParent.mkdirs();
    			}
    			
    			
    			// if file doesnt exists, then create it
    			if (!file.exists()) {
    				//System.out.println(file.getParent());
    				//System.out.println(file.getName());
    				file.createNewFile();
    			}
    			
    			FileWriter fw = new FileWriter(file.getAbsoluteFile());
    			// System.out.println(object.toString());
    			bw = new BufferedWriter(fw);
    			bw.write(sql);
    			bw.close();
    			
    		}
    		
    		
    		System.out.println("Done");
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    private void createTableScript(String srcString, String tgtString, String prefixName, String schemaName, String sidtypeString1, String tabletypeString1) {
    	String sqlString = "select * from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='"+sidtypeString1+"' and t1.table_name='"+tabletypeString1+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	//String sqlString = "SELECT * from src_column where sys='HSPB' and db_schema='trade' ORDER BY sys,db_schema,column_id desc;";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, tgtString, sqlString);
    	//System.out.println(list);
    	List<TableInfo> tableInfos = getSqlString(stringListHashMap, prefixName);
//		for (Object object : createTableSql) {
//			System.out.println(object.toString());
//		}
    	BufferedWriter bw = null;
    	try {
    		
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String tableName = tableInfo.getTableName();
    			String sql = tableInfo.getInfo();
    			String pathName = ddlDir + "/" + StringExtension.toStyleString(prefixName.trim())
    			+ "/" + "ods" + "_" + StringExtension.toStyleString(tableName) + ".sql";
    			File file = new File(pathName);
    			File fileParent = file.getParentFile();
    			
    			if (!fileParent.exists()) {
    				fileParent.mkdirs();
    			}
    			
    			
    			// if file doesnt exists, then create it
    			if (!file.exists()) {
    				//System.out.println(file.getParent());
    				//System.out.println(file.getName());
    				file.createNewFile();
    			}
    			
    			FileWriter fw = new FileWriter(file.getAbsoluteFile());
    			// System.out.println(object.toString());
    			bw = new BufferedWriter(fw);
    			bw.write(sql);
    			bw.close();
    			
    		}
    		
    		
    		System.out.println("Done");
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }


    //生成指定prefixName和schemaName
    //调用getListto(prefixName, schemaName, sqlString);
    //(指定全部系统)
    private void createTableLoadScriptAll(String sys, String sid, String schema) {
        String sqlString = "select t1.sys, t1.table_name, t1.db_sid, t1.db_schema, t2.inc_cdt, t1.column_name"
                +" from src_column t1"
                + " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
                + " where t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.db_schema='" + schema + "'"
                + " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
        BufferedWriter bw = null;
        List<TableInfo> tableInfos = getListto(sqlString);
        try {
            for (TableInfo tableInfo : tableInfos) {
                String key = tableInfo.getSchema();
                String sql = tableInfo.getInfo();
                String pathName = dmlDir + "/" + StringExtension.toStyleString(sys)
                        + "/" + "ods" + "_" + StringExtension.toStyleString(key) + ".sql";
                File file = new File(pathName);
                File fileParent = file.getParentFile();

                if (!fileParent.exists()) {
                    fileParent.mkdirs();
                }

                // if file doesnt exists, then create it
                if (!file.exists()) {
                    file.createNewFile();
                }

                FileWriter fw = new FileWriter(file.getAbsoluteFile());

                bw = new BufferedWriter(fw);
                bw.write(sql);
                bw.close();
            }
            System.out.println("Done");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }

    }
    //(指定sys系统)
    private void createTableLoadScriptSid(String sys, String sid, String schema) {
    	String sqlString = "select t1.sys, t1.table_name, t1.db_sid, t1.db_schema, t2.inc_cdt, t1.column_name"
    			+" from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.db_schema='" + schema + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	BufferedWriter bw = null;
    	List<TableInfo> tableInfos = getListto(sqlString);
    	try {
    		for (TableInfo tableInfo : tableInfos) {
    			String key = tableInfo.getSchema();
    			String sql = tableInfo.getInfo();
    			String pathName = dmlDir + "/" + StringExtension.toStyleString(sys)
    			+ "/" + "ods" + "_" + StringExtension.toStyleString(key) + ".sql";
    			File file = new File(pathName);
    			File fileParent = file.getParentFile();
    			
    			if (!fileParent.exists()) {
    				fileParent.mkdirs();
    			}
    			
    			// if file doesnt exists, then create it
    			if (!file.exists()) {
    				file.createNewFile();
    			}
    			
    			FileWriter fw = new FileWriter(file.getAbsoluteFile());
    			
    			bw = new BufferedWriter(fw);
    			bw.write(sql);
    			bw.close();
    		}
    		System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    	
    }
    //(指定schema系统)
    private void createTableLoadScriptSchema(String sys, String sid, String schema) {
    	String sqlString = "select t1.sys, t1.table_name, t1.db_sid, t1.db_schema, t2.inc_cdt, t1.column_name"
    			+" from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.db_schema='" + schema + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	BufferedWriter bw = null;
    	List<TableInfo> tableInfos = getListto(sqlString);
    	
    	try {
    		for (TableInfo tableInfo : tableInfos) {
    			String key = tableInfo.getSchema();
    			String sql = tableInfo.getInfo();
    			String pathName = dmlDir + "/" + StringExtension.toStyleString(sys)
    			+ "/" + "ods" + "_" + StringExtension.toStyleString(key) + ".sql";
    			File file = new File(pathName);
    			File fileParent = file.getParentFile();
    			
    			if (!fileParent.exists()) {
    				fileParent.mkdirs();
    			}
    			
    			// if file doesnt exists, then create it
    			if (!file.exists()) {
    				file.createNewFile();
    			}
    			
    			FileWriter fw = new FileWriter(file.getAbsoluteFile());
    			
    			bw = new BufferedWriter(fw);
    			bw.write(sql);
    			bw.close();
    		}
    		System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    	
    }
    //(指定表名)
    private void createTableLoadScriptTable(String sys, String schema, String sid, String tableName) {
    	String sqlString = "select t1.sys, t1.table_name, t1.db_sid, t1.db_schema, t2.inc_cdt, t1.column_name"
    			+" from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + sys + "' and t1.db_sid='" + sid + "' and t1.db_schema='" + schema + "' and t1.table_name='"+tableName+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	BufferedWriter bw = null;
    	//System.out.println(sqlString);
    	List<TableInfo> tableInfos = getListto(sqlString);
    	try {
    		for (TableInfo tableInfo : tableInfos) {
    			String key = tableInfo.getSchema();
    			String sql = tableInfo.getInfo();
    			String pathName = dmlDir + "/" + StringExtension.toStyleString(sys)
    			+ "/" + "ods" + "_" + StringExtension.toStyleString(key) + ".sql";
    			File file = new File(pathName);
    			File fileParent = file.getParentFile();
    			
    			if (!fileParent.exists()) {
    				fileParent.mkdirs();
    			}
    			
    			// if file doesnt exists, then create it
    			if (!file.exists()) {
    				file.createNewFile();
    			}
    			
    			FileWriter fw = new FileWriter(file.getAbsoluteFile());
    			
    			bw = new BufferedWriter(fw);
    			bw.write(sql);
    			bw.close();
    		}
    		System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    	
    }


    //获取转换表的数据
    public HashMap<String, String> getConvertTable() {
        HashMap<String, String> tableConvert = new HashMap<String, String>();
        String sqlString = "SELECT * from src_tablename_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String srctable = resultSet.getString(1);
                String tgttable = resultSet.getString(2);
                String sys = resultSet.getString(3);
                tableConvert.put(srctable.toLowerCase() + "," + sys, tgttable);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return tableConvert;
    }


    //获取转换列的数据并保存在map中
    public HashMap<String, String> getConvertColumn() {
        HashMap<String, String> columnConvert = new HashMap<String, String>();
        String sqlString = "SELECT * from etl_column_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String srccolumn = resultSet.getString(1);
                String tgtcolumn = resultSet.getString(2);
                String table = resultSet.getString(3);
                String sys = resultSet.getString(4);
                columnConvert.put(srccolumn + "," + table + "," + sys, tgtcolumn.toLowerCase());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return columnConvert;

    }


    //获取数据库类型的数据并保存在map中
    public HashMap<String, String> getSys() {
        HashMap<String, String> sysConvert = new HashMap<String, String>();
        String sqlString = "SELECT sys,sys_num,db_type,db_sid,db_schema from src_system";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String sys = resultSet.getString(1);
                String sys_num = resultSet.getString(2);
                String db_type = resultSet.getString(3);
                String db_sid = resultSet.getString(4);
                String db_scheam = resultSet.getString(5);
                sysConvert.put(sys.toLowerCase() + db_scheam.toLowerCase(), sys_num + "," + db_type);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return sysConvert;

    }


    //生成sqoop语句
    private List<TableInfo> getListto(String sqlString) {
        HashMap<String, String> sysConvert = getSys();
        HashMap<String, List<ColumnDescInfo>> StringAndListColumnDescInfoMap = new HashMap<String, List<ColumnDescInfo>>();
        List<TableInfo> tableInfos = new ArrayList<>();
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        HashMap<String, String> hashMap = new HashMap<String, String>();
        List<String> list = new ArrayList<String>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            //System.out.println(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String sysString = resultSet.getString(1);
                String tablename = resultSet.getString(2);
                String oldtablename = tablename;
                String db_sid = resultSet.getString(3);
                String db_schema = resultSet.getString(4);
                String inc_cdtString = resultSet.getString(5);
                String column_nameString = resultSet.getString(6);
                String syscheckString = sysString.toLowerCase() + db_schema.toLowerCase();
                if (sysConvert.containsKey(syscheckString)) {
                    String syscheck = sysConvert.get(syscheckString);
                    String sys_num = syscheck.split(",")[0];
                    String db_type = syscheck.split(",")[1];
                    if (db_type.startsWith("mssql")) {
                        if (db_schema.equals("dbo")) {
                            tablename = sys_num + "_" + sysString + "_" + db_sid + "_" + tablename;
                        } else {
                            tablename = sys_num + "_" + sysString + "_" + db_sid + "_" + db_schema + "_" + tablename;
                        }
                    }
                    if (db_type.startsWith("mysql")) {
                        tablename = sys_num + "_" + sysString + "_" + db_schema + "_" + tablename;
                    }
                    if (db_type.startsWith("oracle")) {
                        tablename = sys_num + "_" + sysString + "_" + db_schema + "_" + tablename;
                    }
                }
                String convertKey = SrcdbToTgtdb.getConvertKeyWord(column_nameString);
                if (convertKey.contains("_ora")) {
                    column_nameString = column_nameString + " as " + convertKey;
                }
                List<ColumnDescInfo> columnDescInfos = new ArrayList<ColumnDescInfo>();
                if (oldtablename.toUpperCase().contains("T_PROJECT_REPORT")) {
                    System.out.println("111111");
                }
                ColumnDescInfo columnDescInfo = new ColumnDescInfo();
                columnDescInfo.setSysString(sysString);
                columnDescInfo.setDb_sidString(db_sid);
                columnDescInfo.setTable_nameString(oldtablename);
                columnDescInfo.setColumn_naString(column_nameString);
                columnDescInfo.setIncCdt(inc_cdtString);
                if (StringAndListColumnDescInfoMap.containsKey(tablename)) {
                    List<ColumnDescInfo> columnDescInfoList = StringAndListColumnDescInfoMap.get(tablename);
                    for (ColumnDescInfo column : columnDescInfoList) {
                        columnDescInfos.add(column);
                    }
                    columnDescInfos.add(columnDescInfo);
                    StringAndListColumnDescInfoMap.put(tablename, columnDescInfos);
                } else {
                    columnDescInfos.add(columnDescInfo);
                    StringAndListColumnDescInfoMap.put(tablename, columnDescInfos);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            jdbcConn.closeDbConnection();
        }

        Iterator<Map.Entry<String, List<ColumnDescInfo>>> iterator = StringAndListColumnDescInfoMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<ColumnDescInfo>> entry = iterator.next();
            String key = entry.getKey();
            List<ColumnDescInfo> columnDescInfos = entry.getValue();
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("select\n '${data_date}' as data_date\n");
            String whereString = "";
            String oldName = "";
            for (ColumnDescInfo columnDescInfo : columnDescInfos) {
                if (columnDescInfo.getIncCdt() == null || columnDescInfo.getIncCdt().trim().equals("")) {
                    whereString = "1 = 1";
                } else {
                    whereString = columnDescInfo.getIncCdt();
                }
                oldName = columnDescInfo.getTable_nameString();
                if (oldName.equalsIgnoreCase("STK_ACCTBIZ_LOGEX")) {
                    stringBuffer.append("," + oldName
                            + "." + columnDescInfo.getColumn_nameString())
                            .append("\n");
                } else {
                    stringBuffer.append("," + columnDescInfo.getColumn_nameString())
                            .append("\n");
                }

            }
            if (oldName.equalsIgnoreCase("STK_ACCTBIZ_LOGEX")) {
                stringBuffer.append(",'${etl_time}' as etl_time ")
                        .append("\nfrom ").append(STRING).append(".").append(oldName)
                        .append(",").append(STRING).append(".").append("STK_ACCTBIZ_LOG")
                        .append(" \nwhere ").append(whereString);
            } else {
                stringBuffer.append(",'${etl_time}' as etl_time ")
                        .append("\nfrom ").append(STRING).append(".").append(oldName)
                        .append(" \nwhere ").append(whereString);
            }

            TableInfo tableInfo = new TableInfo(stringBuffer.toString(), oldName, key);
            tableInfos.add(tableInfo);
        }
        return tableInfos;
    }


    //创建建表语句核心
    private List<TableInfo> getSqlString(HashMap<String, List<SourceField>> map, String prefixName) {
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        Iterator<Map.Entry<String, List<SourceField>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            StringBuffer sqlBuffer = new StringBuffer();
            Map.Entry<String, List<SourceField>> entry = iterator.next();
            String key = entry.getKey();
            String[] keys = key.split("_");
            sqlBuffer.append("drop table if exists datawarehouse01." + "ods_" + key.toLowerCase() + ";\n"
            		+ "create table datawarehouse01." + "ods" + "_" + key.toLowerCase() + " (\n" 
            		+ " data_date string" + "\n");
            List<SourceField> sourceFields = entry.getValue();
            for (SourceField sourceField : sourceFields) {
                sqlBuffer.append("," + sourceField.getColumn_nameString().toLowerCase() + " ");
                String columnType = sourceField.getColumn_typeString().toLowerCase();
                if (columnType == null) {
                    System.out.println("我空了" + sourceField);
                }
                int start = columnType.lastIndexOf('(');
                int end = columnType.lastIndexOf(')');
                if (columnType.toLowerCase().contains("decimal")) {
                    sqlBuffer.append(columnType + " ");
                } else {
                    if (start > 0 && end > 0) {
                        String beforColumnType = columnType.substring(0, start);
                        String afterColumnType = getTrString(beforColumnType).toLowerCase();
                        if (afterColumnType.contains("decimal")) {
                            sqlBuffer.append(afterColumnType + "(" + columnType.substring(start + 1, end) + ") ");
                        } else {
                            sqlBuffer.append(afterColumnType.toLowerCase() + " ");
                        }
                    } else {
                        sqlBuffer.append(getTrString(columnType).toLowerCase() + " ");
                    }
                }
                String columnCnName = sourceField.getColumn_cn_nameString();
                if (columnCnName != null && !columnCnName.isEmpty()) {
                    //Modify by wangshunye: 修正判断空字符串的错误表达式
                    String newComment = columnCnName.replaceAll("\n"," ");
                    if (newComment.length() > 256) {
                        newComment = newComment.substring(0, 255);
                    }
                    sqlBuffer.append("comment \"" +  newComment + "\"");
                }
                sqlBuffer.append("\n");
            }
            sqlBuffer.append(",etl_time string\n").append(")")
            		.append(" partitioned by (part_ymd string comment 'partition_by_year_month_day') \nstored as parquet;")
                    .append(" \ninvalidate metadata datawarehouse01.").append("ods_").append(key.toLowerCase()).append(";\nrefresh datawarehouse01.ods_").append(key.toLowerCase()).append(";");
            TableInfo tableInfo = new TableInfo(sqlBuffer.toString(), key, "");
            tableInfos.add(tableInfo);
        }
        return tableInfos;
    }


    public String getTrString(String str) {
        if (hashMap1.containsKey(str.toLowerCase())) {
            return hashMap1.get(str);
        } else {
            System.out.println("代码中不存在数据类型映射："+str);
            System.exit(0);
            return str;
        }
    }


    private HashMap<String, List<SourceField>> getTableDes(String srcString, String tgtString, String sqlString) {
        String sqlString2 = "SELECT src_column_type,tgt_column_type FROM etl_type_convert WHERE src_db_type='" + srcString + "' and tgt_db_type='" + tgtString + "';";
        HashMap<String, String> hashMap = new HashMap<String, String>();
        HashMap<String, String> sysConvert = getSys();
        HashMap<String, List<SourceField>> StringAndListSourceFieldMap = new HashMap<String, List<SourceField>>();
        ResultSet resultSet = null;
        ResultSet resultSet1 = null;
        List<String> list = new ArrayList<String>();
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            PreparedStatement preparedStatement1 = connection.prepareStatement(sqlString2);
            resultSet1 = preparedStatement1.executeQuery();
            while (resultSet1.next()) {
                String srctypeString1 = resultSet1.getString(1);
                String tgttypeString1 = resultSet1.getString(2);
                hashMap1.put(srctypeString1.toLowerCase(), tgttypeString1.toLowerCase());
            }
            hashMap1.put("numeric", "decimal"); //mod
            hashMap1.put("varchar2", "string");
            hashMap1.put("nvarchar2", "string"); // add by wsy
            hashMap1.put("clob", "string"); // add by wsy
            hashMap1.put("uniqueidentifier", "string"); // add by wsy
            hashMap1.put("tinyint", "tinyint"); //mod
            hashMap1.put("integer", "int");
            hashMap1.put("cprice", "double");
            hashMap1.put("real", "real"); // add by wsy
            hashMap1.put("cmoney", "double");
            hashMap1.put("faild", "string");
            hashMap1.put("xml", "string");
            hashMap1.put("string", "string");
            hashMap1.put("bigdecimal", "decimal");
            hashMap1.put("date", "string");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String sysString = resultSet.getString("sys");
                String db_sidString = resultSet.getString("db_sid");
                String db_schemaString = resultSet.getString("db_schema");
                String table_nameString = resultSet.getString("table_name");
                int column_id = resultSet.getInt("column_id");
                String column_nameString = resultSet.getString("column_name");
                String column_typeString = resultSet.getNString("column_type");
                String column_cn_nameString = resultSet.getNString("column_cn_name");
                String is_pkString = resultSet.getNString("is_pk");
                String not_nullString = resultSet.getNString("not_null");
                String default_valueString = resultSet.getNString("default_value");
                String is_dkString = resultSet.getNString("is_dk");
                String break_flagString = resultSet.getString("break_flag");
                List<SourceField> sourceFields = new ArrayList<SourceField>();

                String syscheckString = sysString.toLowerCase() + db_schemaString.toLowerCase();
                if (sysConvert.containsKey(syscheckString)) {
                    String syscheck = sysConvert.get(syscheckString);
                    //System.out.println(syscheck);
                    String sys_num = syscheck.split(",")[0];
                    String db_type = syscheck.split(",")[1];
                    if (db_type.startsWith("mssql")) {
                        if (db_schemaString.equals("dbo")) {
                            table_nameString = sys_num + "_" + sysString + "_" + db_sidString + "_" + table_nameString;
                        } else {
                            table_nameString = sys_num + "_" + sysString + "_" + db_sidString + "_" + db_schemaString + "_" + table_nameString;
                        }
                    }
                    if (db_type.startsWith("mysql")) {
                        table_nameString = sys_num + "_" + sysString + "_" + db_schemaString + "_" + table_nameString;
                    }
                    if (db_type.startsWith("oracle")) {
                        table_nameString = sys_num + "_" + sysString + "_" + db_schemaString + "_" + table_nameString;
                    }
                }

                column_nameString = SrcdbToTgtdb.getConvertKeyWord(column_nameString);
                SourceField sourceField = new SourceField(sysString, db_sidString, db_schemaString, table_nameString, column_id, column_nameString, column_typeString
                        , column_cn_nameString, is_pkString, not_nullString, default_valueString, is_dkString, break_flagString);
                if (StringAndListSourceFieldMap.containsKey(table_nameString)) {
                    List<SourceField> sourceFieldList = StringAndListSourceFieldMap.get(table_nameString);
                    for (SourceField field : sourceFieldList) {
                        sourceFields.add(field);
                    }
                    sourceFields.add(sourceField);
                    StringAndListSourceFieldMap.put(table_nameString, sourceFields);
                } else {
                    sourceFields.add(sourceField);
                    StringAndListSourceFieldMap.put(table_nameString, sourceFields);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            jdbcConn.closeDbConnection();
        }
        return StringAndListSourceFieldMap;
    }

}
