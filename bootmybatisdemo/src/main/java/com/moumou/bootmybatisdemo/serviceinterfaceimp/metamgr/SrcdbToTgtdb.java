package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceTableDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;
import com.moumou.bootmybatisdemo.dataAlignment.model.TableInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;


public class SrcdbToTgtdb {


    static final String UNDERLINE = "_";
    static final String CURRENTDBNAME = "ODS";
    static final String HISTORYDBNAME = "ODS";

    static final String PERSTRING = "-- 0.1 alter parallel\n" +
            "alter session force parallel query parallel 4;\n" +
            "alter session force parallel dml parallel 4;\n" +
            "alter session force parallel ddl parallel 4;\n" +
            "\n" +
            "-- 1.1 create table for exchage and add partition\n" +
            "whenever sqlerror continue none ;\n";

    //ORACLE 和 impala 关键字
    private static HashSet<String> keyword = new HashSet<String>();

    static {
        String sqlString = "SELECT lower(keyword) as keyword FROM src_column,etl_keyword where lower(src_column.column_name) = lower(etl_keyword.keyword) GROUP BY lower(keyword)";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String keyword = resultSet.getString(1);
                SrcdbToTgtdb.keyword.add(keyword.toLowerCase());

            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }

    private String ddlSnapshotTableDir;

    private String ddlPartitionTableDir;

    private String dmlSnapShotToPartitionDir;

    //存储表名和列名
    HashMap<String, String> typeConvertMap = new HashMap<String, String>();

    public SrcdbToTgtdb() {}
    
    public SrcdbToTgtdb(String ddlSnapshotTableDir, String ddlPartitionTableDir, String dmlSnapShotToPartitionDir) {
    	this.ddlSnapshotTableDir = ddlSnapshotTableDir;
    	this.ddlPartitionTableDir = ddlPartitionTableDir;
    	this.dmlSnapShotToPartitionDir = dmlSnapShotToPartitionDir;
    }
    
    /**
     * 转换关键字
     *
     * @param keyword
     * @return
     */
    public static String getConvertKeyWord(String keyword) {
        if (SrcdbToTgtdb.keyword.contains(keyword.toLowerCase())) {
            keyword = keyword + "_ora";
        }
        return keyword;
    }

    /**
     * 生成所有的快照表（m表）DDL文件（全部ddl文件）
     */
    public void generateDDL_Snapshot(String level) {

    	 // 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t2.db_type from "+ 
    	"(SELECT sys, db_schema,db_sid from src_column GROUP BY sys,db_schema,db_sid ) t1 "+
         " inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        //SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String srctypeString1 = resultSet.getString(1);
                String tgttypeString1 = resultSet.getString(2);
                // 调整代码增加 查询源表的数据库类型 20190809 by cm
                //this.createTableToday("mysql", srctypeString1, tgttypeString1);
                String srcDbType = resultSet.getString(4);
                this.createTableTodayAll(srcDbType, srctypeString1, tgttypeString1);
                
                //srcdbToTgtdb.createTableToday("mysql",srctypeString1, tgttypeString1);
                //srcdbToTgtdb.createTableTodayInOneFile("mysql",srctypeString1, tgttypeString1);

            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }
    
    /**
     * 生成所有的快照表（m表）DDL文件（按系统生成ddl文件）
     */
    public void generateDDL_Snapshot(String level,String sys) {
    	
    	// 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t2.db_type from "+ 
    			"(SELECT sys, db_schema,db_sid from src_column WHERE sys='"+sys+"' GROUP BY sys,db_schema,db_sid ) t1 "+
    			" inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			// 调整代码增加 查询源表的数据库类型 20190809 by cm
    			//this.createTableToday("mysql", srctypeString1, tgttypeString1);
    			String srcDbType = resultSet.getString(4);
    			this.createTableToday(srcDbType, srctypeString1, tgttypeString1);
    			
    			//srcdbToTgtdb.createTableToday("mysql",srctypeString1, tgttypeString1);
    			//srcdbToTgtdb.createTableTodayInOneFile("mysql",srctypeString1, tgttypeString1);
    			
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    /**
     * 生成所有的快照表（m表）DDL文件（按schema生成ddl文件）
     */
    public void generateDDL_Snapshot(String level, String sys, String sid, String schema) {
    	
    	// 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t2.db_type from "+ 
    			"(SELECT sys, db_schema,db_sid from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' GROUP BY sys,db_schema,db_sid ) t1 "+
    			" inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String dbsidString1 = resultSet.getString(3);
    			// 调整代码增加 查询源表的数据库类型 20190809 by cm
    			//this.createTableToday("mysql", srctypeString1, tgttypeString1);
    			String srcDbType = resultSet.getString(4);
    			this.createTableToday(srcDbType, srctypeString1, tgttypeString1,dbsidString1);
    			
    			//srcdbToTgtdb.createTableToday("mysql",srctypeString1, tgttypeString1);
    			//srcdbToTgtdb.createTableTodayInOneFile("mysql",srctypeString1, tgttypeString1);
    			
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    /**
     * 生成所有的快照表（m表）DDL文件（按表名生成ddl文件）
     */
    public void generateDDL_Snapshot(String level, String sys, String sid, String schema, String tableName) {
    	
    	// 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t1.table_name,t2.db_type from "+ 
    			"(SELECT sys, db_schema,db_sid,table_name from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' and table_name='"+tableName+"' GROUP BY sys,db_schema,db_sid,table_name) t1 "+
    			" inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			String tabletypeString1 = resultSet.getString(4);
    			// 调整代码增加 查询源表的数据库类型 20190809 by cm
    			//this.createTableToday("mysql", srctypeString1, tgttypeString1);
    			String srcDbType = resultSet.getString(5);
    			this.createTableToday(srcDbType, srctypeString1, tgttypeString1,sidtypeString1,tabletypeString1);
    			
    			//srcdbToTgtdb.createTableToday("mysql",srctypeString1, tgttypeString1);
    			//srcdbToTgtdb.createTableTodayInOneFile("mysql",srctypeString1, tgttypeString1);
    			
    		}
    		System.out.println();
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }

    /**
     * 生成所有的分区表DDL文件(全部ddl文件)
     */
    public void generateDDL_Partition(String level) {

   	 // 调整代码增加 查询源表的数据库类型 20190809 by cm
   	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
   	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t2.db_type from "+ 
   	"(SELECT sys, db_schema,db_sid from src_column GROUP BY sys,db_schema,db_sid ) t1 "+
        " inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;

        //SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String srctypeString1 = resultSet.getString(1);
                String tgttypeString1 = resultSet.getString(2);
                // 调整代码增加 查询源表的数据库类型 20190809 by cm
                //this.createTableHistory("mysql", srctypeString1, tgttypeString1);
                String srcDbType = resultSet.getString(4);
                this.createTableHistoryAll(srcDbType, srctypeString1, tgttypeString1);
            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }
    /**
     * 生成所有的分区表DDL文件(按sys生成ddl文件)
     */
    public void generateDDL_Partition(String level, String sys) {
    	
    	// 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t2.db_type from "+ 
    			"(SELECT sys, db_schema,db_sid from src_column where sys='"+sys+"' GROUP BY sys,db_schema,db_sid ) t1 "+
    			" inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			// 调整代码增加 查询源表的数据库类型 20190809 by cm
    			//this.createTableHistory("mysql", srctypeString1, tgttypeString1);
    			String srcDbType = resultSet.getString(4);
    			this.createTableHistory(srcDbType, srctypeString1, tgttypeString1);
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    /**
     * 生成所有的分区表DDL文件(按schema生成ddl文件)
     */
    public void generateDDL_Partition(String level, String sys, String sid, String schema) {
    	
    	// 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t2.db_type from "+ 
    			"(SELECT sys, db_schema,db_sid from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' GROUP BY sys,db_schema,db_sid ) t1 "+
    			" inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			// 调整代码增加 查询源表的数据库类型 20190809 by cm
    			//this.createTableHistory("mysql", srctypeString1, tgttypeString1);
    			String srcDbType = resultSet.getString(4);
    			this.createTableHistory(srcDbType, srctypeString1, tgttypeString1, sidtypeString1);
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    /**
     * 生成所有的分区表DDL文件(按表名生成ddl文件)
     */
    public void generateDDL_Partition(String level, String sys, String sid, String schema, String tableName) {
    	
    	// 调整代码增加 查询源表的数据库类型 20190809 by cm
    	//String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
    	String sqlString = "select t1.sys,t1.db_schema,t1.db_sid,t1.table_name,t2.db_type from "+ 
    			"(SELECT sys, db_schema,db_sid,table_name from src_column where sys='"+sys+"' and db_sid='"+sid+"' and db_schema='"+schema+"' and table_name='"+tableName+"' GROUP BY sys,db_schema,db_sid,table_name ) t1 "+
    			" inner join  src_system t2 on t1.sys=t2.sys and t1.db_schema = t2.db_schema and t1.db_sid = t2.db_sid";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			String tabletypeString1 = resultSet.getString(4);
    			// 调整代码增加 查询源表的数据库类型 20190809 by cm
    			//this.createTableHistory("mysql", srctypeString1, tgttypeString1);
    			String srcDbType = resultSet.getString(5);
    			this.createTableHistory(srcDbType, srctypeString1, tgttypeString1, sidtypeString1, tabletypeString1);
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }

    /**
     * 生成所有的Oracle DML脚本文件（快照表到分区表）（按全部生成）
     */
    public void generateDML_SnapshotToPartition(String level) {

        String sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;

        //SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String srctypeString1 = resultSet.getString(1);
                String tgttypeString1 = resultSet.getString(2);

                this.createTableHistoryToTodayAll("mysql", srctypeString1, tgttypeString1);
            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }
    /**
     * 生成所有的Oracle DML脚本文件（快照表到分区表）(按系统生成)
     */
    public void generateDML_SnapshotToPartition(String level, String sys) {
    	
    	String sqlString = "SELECT sys, db_schema from src_column where sys='"+sys+"' GROUP BY sys,db_schema";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			
    			this.createTableHistoryToToday("mysql", srctypeString1, tgttypeString1);
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    /**
     * 生成所有的Oracle DML脚本文件（快照表到分区表）(按schema生成)
     */
    public void generateDML_SnapshotToPartition(String level, String sys, String sid, String schema) {
    	
    	String sqlString = "SELECT sys, db_schema, db_sid from src_column where sys='"+sys+"' and db_schema='"+schema+"' and db_sid='"+sid+"' GROUP BY sys,db_schema,db_sid";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			
    			this.createTableHistoryToToday("mysql", srctypeString1, tgttypeString1, sidtypeString1);
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }
    /**
     * 生成所有的Oracle DML脚本文件（快照表到分区表）
     */
    public void generateDML_SnapshotToPartition(String level, String sys, String sid, String schema, String tableName) {
    	
    	String sqlString = "SELECT sys, db_schema, db_sid, table_name from src_column where sys='"+sys+"' and db_schema='"+schema+"' and db_sid='"+sid+"' and table_name='"+tableName+"' GROUP BY sys,db_schema,db_sid,table_name";
    	JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
    	Connection connection = jdbcConn.getDbConnection();
    	ResultSet resultSet = null;
    	
    	//SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
    	
    	try {
    		PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
    		resultSet = preparedStatement.executeQuery();
    		
    		while (resultSet.next()) {
    			
    			String srctypeString1 = resultSet.getString(1);
    			String tgttypeString1 = resultSet.getString(2);
    			String sidtypeString1 = resultSet.getString(3);
    			String tabletypeString1 = resultSet.getString(4);
    			
    			this.createTableHistoryToToday("mysql", srctypeString1, tgttypeString1, sidtypeString1, tabletypeString1);
    		}
    		//srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		jdbcConn.closeDbConnection();
    	}
    }

    /**
     * 获取转换表中 关于表的数据  src_tablename_convert
     * 注意对比的时候表名为小写
     * @return
     */
    public HashMap<String, String> getConvertTable() {
        HashMap<String, String> tableConvert = new HashMap<String, String>();

        String sqlString = "SELECT * from src_tablename_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        //SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String srctable = resultSet.getString(1);
                String tgttable = resultSet.getString(2);
                String sys = resultSet.getString(3);
                tableConvert.put(srctable.toLowerCase() + "," + sys, tgttable);
                //System.out.println(srctable + "------" + tgttable + "----------" + sys);
            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return tableConvert;

    }


    /**
     * 获取转换表中关于列的的数据并保存在map中    etl_column_convert
     * @return
     */
    public HashMap<String, String> getConvertColumn() {
        HashMap<String, String> columnConvert = new HashMap<String, String>();

        String sqlString = "SELECT * from etl_column_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        //SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb();
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
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return columnConvert;

    }


    /**
     *src_tabschema_convert
     * @return
     */
    public HashMap<String, String> getConvertTableSchema() {
        HashMap<String, String> tableConvert = new HashMap<String, String>();
        //查找语句
        String sqlString = "SELECT * from src_tabschema_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String srcTableSchema = resultSet.getString(1);
                String db_sid = resultSet.getString(2);
                String sys = resultSet.getString(3);
                String tgtTableSchema = resultSet.getString(4);

                tableConvert.put(srcTableSchema + "," + db_sid + "," + sys, tgtTableSchema);
            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return tableConvert;

    }


    /**
     * 当日数据表（生成所有系统）
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    public void createTableTodayAll(String srcString, String prefixName, String schemaName) {
        HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
        String sqlString = "select t1.* from src_column t1"
        			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
        			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
        			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
        			
        HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
        List<TableInfo> tableInfos = getsqlListToday(stringListHashMap, prefixName);
        BufferedWriter bw = null;
        try {
            for (TableInfo tableInfo : tableInfos) {
                String sql = tableInfo.getInfo();
                String tableName = tableInfo.getTableName();
                String schema = tableInfo.getSchema();
                if (schema == null || schema == "") {
                    schema = "";
                }
                // String schemaString = object.toString().split("&&")[2];
                File file = null;
                if (schema.length() > 0) {
                    String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
                            + "/" + StringExtension.toStyleString(CURRENTDBNAME)
                            + "." + StringExtension.toStyleString(prefixName)
                            + UNDERLINE + StringExtension.toStyleString(schema)
                            + UNDERLINE + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
                    file = new File(pathName);
                } else {
                    String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
                            + "/" + StringExtension.toStyleString(CURRENTDBNAME)
                            + "." + StringExtension.toStyleString(prefixName)
                            + "_" + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
                    file = new File(pathName);
                }
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
                bw.write(sql + "\n");
                bw.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }
    
    /**
     * 当日数据表（指定sys）
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    public void createTableToday(String srcString, String prefixName, String schemaName) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
    	List<TableInfo> tableInfos = getsqlListToday(stringListHashMap, prefixName);
    	BufferedWriter bw = null;
    	try {
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			if (schema.length() > 0) {
    				String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(CURRENTDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ UNDERLINE + StringExtension.toStyleString(schema)
    				+ UNDERLINE + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
    				file = new File(pathName);
    			} else {
    				String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(CURRENTDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ "_" + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
    				file = new File(pathName);
    			}
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    
    /**
     * 当日数据表（指定sys，sid，schema）
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    public void createTableToday(String srcString, String prefixName, String schemaName, String dbsidString1) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='"+ dbsidString1+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
    	List<TableInfo> tableInfos = getsqlListToday(stringListHashMap, prefixName);
    	BufferedWriter bw = null;
    	try {
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			if (schema.length() > 0) {
    				String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(CURRENTDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ UNDERLINE + StringExtension.toStyleString(schema)
    				+ UNDERLINE + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
    				file = new File(pathName);
    			} else {
    				String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(CURRENTDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ "_" + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
    				file = new File(pathName);
    			}
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    
    /**
     * 当日数据表（指定sys，sid，schema，tablename）
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    public void createTableToday(String srcString, String prefixName, String schemaName, String dbsidString1,String tabletypeString1) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='"+ dbsidString1+"' and t1.table_name='"+ tabletypeString1+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
    	List<TableInfo> tableInfos = getsqlListToday(stringListHashMap, prefixName);
    	BufferedWriter bw = null;
    	try {
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			if (schema.length() > 0) {
    				String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(CURRENTDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ UNDERLINE + StringExtension.toStyleString(schema)
    				+ UNDERLINE + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
    				file = new File(pathName);
    			} else {
    				String pathName = ddlSnapshotTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(CURRENTDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ "_" + StringExtension.toStyleString(tableName) + UNDERLINE + "m.sql";
    				file = new File(pathName);
    			}
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }

    /**
     * 历史数据表
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistoryAll(String srcString, String prefixName, String schemaName) {
        HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
        String sqlString = "select t1.* from src_column t1"
        		+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
        		+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
        		+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
      
        HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);       
        List<TableInfo> tableInfos = getsqlListHistory(stringListHashMap, prefixName,schemaName);

        BufferedWriter bw = null;
        try {

            for (TableInfo tableInfo : tableInfos) {
                String sql = tableInfo.getInfo();
                String tableName = tableInfo.getTableName();
                String schema = tableInfo.getSchema();
                if (schema == null || schema == "") {
                    schema = "";
                }
                // String schemaString = object.toString().split("&&")[2];
                File file = null;
                if (schema.length() > 0) {
                    String pathName = ddlPartitionTableDir
                            + "/" + StringExtension.toStyleString(prefixName)
                            + "/" + StringExtension.toStyleString(HISTORYDBNAME)
                            + "." + StringExtension.toStyleString(prefixName)
                            + UNDERLINE + StringExtension.toStyleString(schema)
                            + UNDERLINE + StringExtension.toStyleString(tableName) + ".sql";
                    file = new File(pathName);
                } else {
                    String pathName = ddlPartitionTableDir + "/" + StringExtension.toStyleString(prefixName)
                            + "/" + StringExtension.toStyleString(HISTORYDBNAME)
                            + "." + StringExtension.toStyleString(prefixName)
                            + "_" + StringExtension.toStyleString(tableName) + ".sql";
                    file = new File(pathName);
                }


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
                bw.write(sql + "\n");
                bw.close();
            }

            //System.out.println("Done");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }
    /**
     * 历史数据表（指定sys）
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistory(String srcString, String prefixName, String schemaName) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);       
    	List<TableInfo> tableInfos = getsqlListHistory(stringListHashMap, prefixName,schemaName);
    	
    	BufferedWriter bw = null;
    	try {
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			if (schema.length() > 0) {
    				String pathName = ddlPartitionTableDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(HISTORYDBNAME)
    						+ "." + StringExtension.toStyleString(prefixName)
    						+ UNDERLINE + StringExtension.toStyleString(schema)
    						+ UNDERLINE + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			} else {
    				String pathName = ddlPartitionTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(HISTORYDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			}
    			
    			
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    		//System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    /**
     * 历史数据表(指定schema)
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistory(String srcString, String prefixName, String schemaName, String sidtypeString1 ) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='"+ sidtypeString1+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);       
    	List<TableInfo> tableInfos = getsqlListHistory(stringListHashMap, prefixName,schemaName);
    	
    	BufferedWriter bw = null;
    	try {
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			if (schema.length() > 0) {
    				String pathName = ddlPartitionTableDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(HISTORYDBNAME)
    						+ "." + StringExtension.toStyleString(prefixName)
    						+ UNDERLINE + StringExtension.toStyleString(schema)
    						+ UNDERLINE + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			} else {
    				String pathName = ddlPartitionTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(HISTORYDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			}
    			
    			
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    		//System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    /**
     * 历史数据表(指定表名)
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistory(String srcString, String prefixName, String schemaName, String sidtypeString1, String tabletypeString1) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='" + sidtypeString1 + "' and t1.table_name='" + tabletypeString1 + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc";
    	
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);       
    	List<TableInfo> tableInfos = getsqlListHistory(stringListHashMap, prefixName,schemaName);
    	
    	BufferedWriter bw = null;
    	try {
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			if (schema.length() > 0) {
    				String pathName = ddlPartitionTableDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(HISTORYDBNAME)
    						+ "." + StringExtension.toStyleString(prefixName)
    						+ UNDERLINE + StringExtension.toStyleString(schema)
    						+ UNDERLINE + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			} else {
    				String pathName = ddlPartitionTableDir + "/" + StringExtension.toStyleString(prefixName)
    				+ "/" + StringExtension.toStyleString(HISTORYDBNAME)
    				+ "." + StringExtension.toStyleString(prefixName)
    				+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			}
    			
    			
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    		//System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }


    /**
     * m表到分区表（全部）
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistoryToTodayAll(String srcString, String prefixName, String schemaName) {
        HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
        String sqlString = "select t1.* from src_column t1"
        			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
        			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
        			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc"; 
        HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
        List<TableInfo> tableInfos = getsqlListHistoryToToday(stringListHashMap, prefixName);


        BufferedWriter bw = null;
        try {


            for (TableInfo tableInfo : tableInfos) {
                String sql = tableInfo.getInfo();
                String tableName = tableInfo.getTableName();
                String schema = tableInfo.getSchema();
                if (schema == null || schema == "") {
                    schema = "";
                }
                // String schemaString = object.toString().split("&&")[2];
                File file = null;
             //   System.out.println(schema + "------" + tableName);
                if (schema.length() > 0) {
                    String pathName = dmlSnapShotToPartitionDir
                            + "/" + StringExtension.toStyleString(prefixName)
                            + "/" + StringExtension.toStyleString(prefixName)
                            + "_" + StringExtension.toStyleString(schema)
                            + "_" + StringExtension.toStyleString(tableName) + ".sql";
                    file = new File(pathName);
            //        System.out.println("----------------------------" + file.getPath() + "----------" + schema + "------" + tableName);
                } else {
                    String pathName = dmlSnapShotToPartitionDir
                            + "/" + StringExtension.toStyleString(prefixName)
                            + "/" + StringExtension.toStyleString(prefixName)
                            + "_" + StringExtension.toStyleString(tableName) + ".sql";
                    file = new File(pathName);
                }


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
                bw.write(sql + "\n");
                bw.close();
            }


            System.out.println("Done");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }
    /**
     * m表到分区表(指定sys)
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistoryToToday(String srcString, String prefixName, String schemaName) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc"; 
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
    	List<TableInfo> tableInfos = getsqlListHistoryToToday(stringListHashMap, prefixName);
    	
    	
    	BufferedWriter bw = null;
    	try {
    		
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			//   System.out.println(schema + "------" + tableName);
    			if (schema.length() > 0) {
    				String pathName = dmlSnapShotToPartitionDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "_" + StringExtension.toStyleString(schema)
    						+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    				//        System.out.println("----------------------------" + file.getPath() + "----------" + schema + "------" + tableName);
    			} else {
    				String pathName = dmlSnapShotToPartitionDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			}
    			
    			
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    		
    		System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    /**
     * m表到分区表(指定schema)
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistoryToToday(String srcString, String prefixName, String schemaName, String sidtypeString1) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='" + sidtypeString1 + "'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc"; 
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
    	List<TableInfo> tableInfos = getsqlListHistoryToToday(stringListHashMap, prefixName);
    	
    	
    	BufferedWriter bw = null;
    	try {
    		
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			//   System.out.println(schema + "------" + tableName);
    			if (schema.length() > 0) {
    				String pathName = dmlSnapShotToPartitionDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "_" + StringExtension.toStyleString(schema)
    						+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    				//        System.out.println("----------------------------" + file.getPath() + "----------" + schema + "------" + tableName);
    			} else {
    				String pathName = dmlSnapShotToPartitionDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			}
    			
    			
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    		
    		System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }
    /**
     * m表到分区表(指定表名)
     * @param srcString
     * @param prefixName
     * @param schemaName
     */
    private void createTableHistoryToToday(String srcString, String prefixName, String schemaName, String sidtypeString1, String tabletypeString1) {
    	HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
    	String sqlString = "select t1.* from src_column t1"
    			+ " inner join src_table t2 on t1.sys = t2.sys and t1.db_sid = t2.db_sid and t1.db_schema = t2.table_schema and t1.table_name = t2.table_name and upper(t2.is_put_to_etldb) = 'Y'"
    			+ " where t1.sys='" + prefixName + "' and t1.db_schema='" + schemaName + "' and t1.db_sid='"+sidtypeString1+"' and t1.table_name='"+tabletypeString1+"'"
    			+ " order by t1.sys asc, t1.db_sid asc, t1.db_schema asc, t1.table_name asc, t1.column_id asc"; 
    	HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
    	List<TableInfo> tableInfos = getsqlListHistoryToToday(stringListHashMap, prefixName);
    	
    	
    	BufferedWriter bw = null;
    	try {
    		
    		
    		for (TableInfo tableInfo : tableInfos) {
    			String sql = tableInfo.getInfo();
    			String tableName = tableInfo.getTableName();
    			String schema = tableInfo.getSchema();
    			if (schema == null || schema == "") {
    				schema = "";
    			}
    			// String schemaString = object.toString().split("&&")[2];
    			File file = null;
    			//   System.out.println(schema + "------" + tableName);
    			if (schema.length() > 0) {
    				String pathName = dmlSnapShotToPartitionDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "_" + StringExtension.toStyleString(schema)
    						+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    				//        System.out.println("----------------------------" + file.getPath() + "----------" + schema + "------" + tableName);
    			} else {
    				String pathName = dmlSnapShotToPartitionDir
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "/" + StringExtension.toStyleString(prefixName)
    						+ "_" + StringExtension.toStyleString(tableName) + ".sql";
    				file = new File(pathName);
    			}
    			
    			
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
    			bw.write(sql + "\n");
    			bw.close();
    		}
    		
    		
    		System.out.println("Done");
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		
    	}
    }


    /**
     * 获取当前的sqllist
     * @param map
     * @param prefixName
     * @return
     */
    private List<TableInfo> getsqlListToday(HashMap map, String prefixName) {

        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        Iterator<Map.Entry<String, List<SourceField>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            StringBuffer sqlBuffer = new StringBuffer();
            Map.Entry<String, List<SourceField>> entry = iterator.next();
            String key = entry.getKey();
            String[] arr = key.split(",");
            String tableName = arr[0];
            String schema = "";
            if (arr.length > 1) {
                schema = arr[1];
            }
            String pkString = "";
            String full_name = "";
            if (schema.length() > 0) {
                //allString = CURRENTDBNAME + "." + prefixName + "_" + schema + "_" + tableName + "_m";
            	full_name = (CURRENTDBNAME + "." + prefixName + "_" + schema + "_" + tableName + "_m").toLowerCase();
   
            } else {
                //allString = CURRENTDBNAME + "." + prefixName + "_" + tableName + "_m";
            	full_name = (CURRENTDBNAME + "." + prefixName + "_" + tableName + "_m").toLowerCase();
            }
            sqlBuffer.append("prompt creating table ").append(full_name).append(" \n");
            sqlBuffer.append("whenever sqlerror continue none;\n");
            sqlBuffer.append("drop table ").append(full_name).append(" purge;\n");
            sqlBuffer.append("whenever sqlerror exit sql.sqlcode;\n");           
            sqlBuffer.append("create table " + full_name + " (" + "\n" 
    				+ " data_date date not null" + "\n");
            
            List<SourceField> sourceFields = entry.getValue();
           // System.out.println(tableName + "-------" + sourceFields.size());
            for (SourceField sourceField : sourceFields) {
                //添加columnName ，源系统存在字段命名有后空格情况，增加trim处理
                sqlBuffer.append(",").append(sourceField.getColumn_nameString().toLowerCase().trim()).append(" ");
                //添加columnType
                String columnType = sourceField.getColumn_typeString().toLowerCase();
                int start = columnType.lastIndexOf('(');
                int end = columnType.lastIndexOf(')');
                String typeSize = "";
//                if (columnType.lastIndexOf("-") > 0) {
//                    sqlBuffer.append(getTrString(columnType).toUpperCase()).append(" ");
//                }

                if (start > 0 && end > 0) {
                    if (columnType.split(",").length > 1) {
                        sqlBuffer.append(getTrString(columnType).toLowerCase());
                    } else {
                        int size = Integer.parseInt(columnType.substring(start + 1, end));
                        String beforeConvertType = columnType.substring(0, start);
                        String afterConvertType = getTrString(beforeConvertType);
                        if (beforeConvertType.toLowerCase().contains("double") && afterConvertType.toLowerCase().contains("number")){
                            sqlBuffer.append(afterConvertType.toLowerCase());
                        }
                        else if (afterConvertType.contains("(")) {
                            sqlBuffer.append(afterConvertType.toLowerCase());
                        } else if (size > 2000 && afterConvertType.toLowerCase().contains("nvarchar2")) {
                            typeSize = "(2000)";
                        } else if (size > 4000 || size < 0) {
                            typeSize = "(4000)";
                        } else if (size > 38 && afterConvertType.toLowerCase().contains("number")) {
                            typeSize = "(38)";
                        } else {
                            typeSize = columnType.substring(start, end + 1);
                        }
                        sqlBuffer.append(afterConvertType.toLowerCase()).append(typeSize);
                    }
                } else {
                    sqlBuffer.append(getTrString(columnType).toLowerCase());
                }
                sqlBuffer.append("\n");
            }
            sqlBuffer.append(",etl_time date not null\n").append(")").append(" compress nologging \n").append(";\n");
            
            //添加生成字段注释的逻辑
            String fullTableName;
            if (schema.length() > 0) {
            	fullTableName = (CURRENTDBNAME + "." + prefixName + "_" + schema + "_" + tableName + "_m").toLowerCase();
            } else {
            	fullTableName = (CURRENTDBNAME + "." + prefixName + "_" + tableName + "_m").toLowerCase();
            }
            for (SourceField aSourceField : sourceFields) {
            	if(null != aSourceField.getColumn_cn_nameString() && !aSourceField.getColumn_cn_nameString().isEmpty()) {
	            	String sql = "comment on column "+ fullTableName + "." + aSourceField.getColumn_nameString().toLowerCase() 
	            				+ " is '" + aSourceField.getColumn_cn_nameString().replaceAll("\n"," ").replaceAll("'", "").replaceAll("\"", "") + "';\n";
	            	sqlBuffer.append(sql);
            	}
            }
            
            TableInfo tableInfo = new TableInfo(sqlBuffer.toString(), tableName, schema);
            tableInfos.add(tableInfo);
            //addpkString + "&&" + tablename + "&&" + schema
        }
        return tableInfos;

    }


    /**
     * 根据表名和schema名称查找表信息
     * @param tableName
     * @param tableSchema
     * @param temp_tables_list
     * @return
     */
    private SourceTable fetchTabByNameAndSchema(String tableName,String tableSchema,List<SourceTable> temp_tables_list ){
    	
    	SourceTable tab  = null;
    	for(SourceTable srcTab : temp_tables_list){
    		if(tableName.equals(srcTab.getTable_nameString())&&tableSchema.equals(srcTab.getTable_schemaString())){
    			tab = srcTab;
    			break;
    		}
    	}
    	return tab;
    	
    }
    
    /**
     * 获取历史的sqllist
     * @param map
     * @param prefixName
     * @return
     */
    private List<TableInfo> getsqlListHistory(HashMap map, String prefixName,String shcema_name) {
    	//todo 临时增加判断 是否拉链表判断  20190809 cm
        SourceTableDao sourceTabDao = new SourceTableDao();
        List<SourceTable> temp_tables_list = sourceTabDao.getConvertedTableInfo_Filter(prefixName);
    	//todo 临时增加判断 是否拉链表判断  20190809 cm
    	
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        Iterator<Map.Entry<String, List<SourceField>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            StringBuffer sqlBuffer = new StringBuffer();
            Map.Entry<String, List<SourceField>> entry = iterator.next();
            String key = entry.getKey();
            String[] arr = key.split(",");
            String tableName = arr[0];
            SourceTable src_table = fetchTabByNameAndSchema(tableName,shcema_name,temp_tables_list);
            if(src_table==null){
            	System.out.println("ERRO:未找到对应的表信息:"+shcema_name+"."+tableName);
            	continue;
            }
            boolean createZiperTab = "1".equals(src_table.getTemplate_codeString());
            String schema = "";
            if (arr.length > 1) {
                schema = arr[1];
            }
            String fullName = "";
            if (schema.length() > 0) {
            	fullName = (HISTORYDBNAME + "." + prefixName + "_" + schema + "_" + tableName).toLowerCase();           	           	
            } else {
            	fullName = (HISTORYDBNAME + "." + prefixName + "_" + tableName).toLowerCase();
            }
            
        	sqlBuffer.append("prompt creating table ").append(fullName).append("\n");
        	sqlBuffer.append("whenever sqlerror continue none;\n");
        	sqlBuffer.append("drop table ").append(fullName).append(" purge;\n");
        	sqlBuffer.append("whenever sqlerror exit sql.sqlcode;\n");
            sqlBuffer.append("create table ").append(fullName).append(" (\n");
            
        	if(!createZiperTab){
                sqlBuffer.append(" data_date date not null \n");
        	}
        	
        	
            List<SourceField> sourceFields = entry.getValue();
            int startIdx = 0;
            for (SourceField sourceField : sourceFields) {
            	if(!createZiperTab){
            		sqlBuffer.append(",");
            	}else if(startIdx!=0){
            		sqlBuffer.append(",");
            	}
                //添加columnName 源系统存在字段命名 增加后空格的情况，通过trim过滤
                sqlBuffer.append(sourceField.getColumn_nameString().toLowerCase().trim()).append(" ");
                //添加columnType
                String columnType = sourceField.getColumn_typeString().toLowerCase();
                int start = columnType.lastIndexOf('(');
                int end = columnType.lastIndexOf(')');
                String typeSize = "";
//                if (columnType.lastIndexOf("-") > 0) {
//                    sqlBuffer.append(getTrString(columnType).toUpperCase()).append(" ");
//                }
                if (start > 0 && end > 0) {
                    if (columnType.split(",").length > 1) {
                        //System.out.println(columnType + "---------" + getTrString(columnType) + "(*,*)");
                        sqlBuffer.append(getTrString(columnType).toLowerCase());
                    } else {
                        int size = Integer.parseInt(columnType.substring(start + 1, end));
                        String beforeConvertType = columnType.substring(0, start);
                        String afterConvertType = getTrString(beforeConvertType);
                        //System.out.println(beforeConvertType + "--------" + afterConvertType);
                        if (beforeConvertType.toLowerCase().contains("double") && afterConvertType.toLowerCase().contains("number")){
                            sqlBuffer.append(afterConvertType.toLowerCase());
                        }
                        else if (afterConvertType.contains("(")) {
                            sqlBuffer.append(afterConvertType.toLowerCase());
                        } else if (size > 2000 && afterConvertType.toLowerCase().contains("nvarchar2")) {
                            typeSize = "(2000)";
                        } else if (size > 4000 || size < 0) {
                            typeSize = "(4000)";
                        } else if (size > 38 && afterConvertType.toLowerCase().contains("number")) {
                            typeSize = "(38)";
                        } else {
                            typeSize = columnType.substring(start, end + 1);
                        }
                        sqlBuffer.append(afterConvertType.toLowerCase()).append(typeSize);
                    }
                } else {
                    sqlBuffer.append(getTrString(columnType).toLowerCase());
                }
                sqlBuffer.append("\n");
                startIdx++;
            }
            
            if(createZiperTab){
            	sqlBuffer.append(",start_dt date\n");
            	sqlBuffer.append(",end_dt date\n");
            }
            sqlBuffer.append(",etl_time date not null\n").append(")\n");
            if(createZiperTab){
            	sqlBuffer.append("partition by range(end_dt)(\n");
            	sqlBuffer.append("    partition p_19000101 values less than (to_date('20991231','yyyymmdd'))\n");
            	sqlBuffer.append("   ,partition p_20991231 values less than (maxvalue)\n");
            	sqlBuffer.append("   )\n");
            	
            }else{
	            sqlBuffer.append("partition by list(data_date) (\n")
	                    .append("  partition p_19000101 values (to_date('19000101','yyyymmdd'))\n").append(")\n");
            }
            sqlBuffer.append("compress nologging\n").append(";\n");
            
            //添加生成字段注释的逻辑
            String fullTableName;
            if (schema.length() > 0) {
            	fullTableName = (HISTORYDBNAME + "." + prefixName + "_" + schema + "_" + tableName).toLowerCase();
            } else {
            	fullTableName = (HISTORYDBNAME + "." + prefixName + "_" + tableName).toLowerCase();
            }
            for (SourceField aSourceField : sourceFields) {
            	if(null != aSourceField.getColumn_cn_nameString() && !aSourceField.getColumn_cn_nameString().isEmpty()) {
	            	String sql = "comment on column "+ fullTableName + "." + aSourceField.getColumn_nameString().toLowerCase() 
	            				+ " is '" + aSourceField.getColumn_cn_nameString().replaceAll("\n"," ").replaceAll("'", "").replaceAll("\"", "") + "';\n";
	            	sqlBuffer.append(sql);
            	}
            }
            
            TableInfo tableInfo = new TableInfo(sqlBuffer.toString(), tableName, schema);
            tableInfos.add(tableInfo);
        }

        return tableInfos;
    }


    /**
     * 获取历史转到当前的sqllist
     * @param map
     * @param prefixName
     * @return List<TableInfo>
     */
    private List<TableInfo> getsqlListHistoryToToday(HashMap<String,List<SourceField>> map, String prefixName) {

        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        Iterator<Map.Entry<String, List<SourceField>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            StringBuffer sqlBuffer = new StringBuffer();
            Map.Entry<String, List<SourceField>> entry = iterator.next();
            String key = entry.getKey();
            String[] arr = key.split(",");
            String tableName = arr[0];
            String schema = "";
            if (arr.length > 1) {
                schema = arr[1];
            }
            if (schema.length() > 0) {
                sqlBuffer.append(PERSTRING)
                        .append("drop table ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_zz")
                                +" purge;").append("\n")
                        .append("alter table ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName)
                                + " add partition p_${batch_date} values (to_date('${batch_date}','${sql_date_format}'));").append("\n")
                        .append("\n")
                        .append("-- 2.1 truncate target table batch_date partition").append("\n")
                        .append("whenever sqlerror exit sql.sqlcode;").append("\n")
                        .append("alter table ods." + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName)
                                + " truncate partition p_${batch_date};").append("\n")
                        .append("\n")
                        .append("-- 2.2 insert data to ex table").append("\n")
                        .append("create table ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_zz")).append("\n")
                        .append("as").append("\n")
                        .append("select * from ods." + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_m")
                                + " where 1=0;").append("\n")
                        .append("\n")
                        .append("insert into ods." + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_zz")).append("\n")
                        .append("select").append("\n")
                        .append(" data_date").append("\n");
            } else {
                sqlBuffer.append(PERSTRING)
                        .append("drop table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName + "_zz")
                                + " purge;").append("\n")
                        .append("alter table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName)
                                + " add partition p_${batch_date} values (to_date('${batch_date}','${sql_date_format}'));").append("\n")
                        .append("\n")
                        .append("-- 2.1 truncate target table batch_date partition").append("\n")
                        .append("whenever sqlerror exit sql.sqlcode;").append("\n")
                        .append("alter table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName)
                                + " truncate partition p_${batch_date};").append("\n")
                        .append("\n")
                        .append("-- 2.2 insert data to ex table").append("\n")
                        .append("create table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName + "_zz")).append("\n")
                        .append("as").append("\n")
                        .append("select * from ods." + StringExtension.toStyleString(prefixName + "_" + tableName + "_m")
                                + " where 1=0;").append("\n")
                        .append("\n")
                        .append("insert into ods." + StringExtension.toStyleString(prefixName + "_" + tableName + "_zz")).append("\n")
                        .append("select").append("\n")
                        .append(" data_date").append("\n");
            }
            List<SourceField> sourceFields = entry.getValue();
            //System.out.println(tableName + "-------" + sourceFields.size());
            for (SourceField sourceField : sourceFields) {
                //添加columnName
                sqlBuffer.append(",").append(StringExtension.toStyleString(sourceField.getColumn_nameString())).append("\n");
            }
            if (schema.length() > 0) {
                sqlBuffer.append(",sysdate as etl_time").append("\n")
                        .append("from ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_m")).append("\n")
                        .append("where data_date = to_date('${batch_date}', '${sql_date_format}')").append("\n")
                        .append(";").append("\n")
                        .append("\n")
                        .append("-- 2.3 exchage ex table and target table").append("\n")
                        .append("alter table ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName)
                                + " exchange partition p_${batch_date} with table ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_zz;"))
                        .append("\n").append("\n")
                        .append("-- 3.1 drop ex table").append("\n")
                        .append("drop table ods."
                                + StringExtension.toStyleString(prefixName + "_" + schema + "_" + tableName + "_zz")
                                + " purge;");
            } else {
                sqlBuffer.append(",sysdate as etl_time").append("\n")
                        .append("from ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName + "_m")).append("\n")
                        .append("where data_date = to_date('${batch_date}', '${sql_date_format}')").append("\n")
                        .append(";").append("\n")
                        .append("\n")
                        .append("-- 2.3 exchage ex table and target table").append("\n")
                        .append("alter table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName)
                                + " exchange partition p_${batch_date} with table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName + "_zz;"))
                        .append("\n").append("\n")
                        .append("-- 3.1 drop ex table").append("\n")
                        .append("drop table ods."
                                + StringExtension.toStyleString(prefixName + "_" + tableName + "_zz")
                                + " purge;");
            }

            TableInfo tableInfo = new TableInfo(sqlBuffer.toString(), tableName, schema);
            tableInfos.add(tableInfo);
        }
        return tableInfos;
    }

    public static HashSet<String> errHashSet = new HashSet<String>();
    public static List<String> errlist = new ArrayList<String>();

    /**
     * 转换类型
     * @param str 转换的字符串
     * @return 转换后的字符串
     */
    public String getTrString(String str) {

        if (typeConvertMap.containsKey(str.trim().toLowerCase())) {
            return typeConvertMap.get(str);
        } else {
            errlist.add(str);
            //System.exit(0);
            return str;
        }
    }

    /**
     * 获取到一个表的描述包含列信息
     * @param srcString  源系统
     * @param tgtString  目标系统
     * @param sqlString  查询的sql语句createTableToday传进来的
     * @return HashMap<String, List<SourceField>>  key : String  value: List<SourceField>
     */

    private HashMap<String, List<SourceField>> getTableDes(String srcString, String tgtString, String sqlString) {
        String sqlString2 = "SELECT src_column_type,tgt_column_type FROM etl_type_convert WHERE src_db_type='" + srcString + "' and tgt_db_type='" + tgtString + "';";
        HashMap<String, String> tableMap = getConvertTable();
        HashMap<String, String> columnMap = getConvertColumn();
        HashMap<String, String> scheaMap = getConvertTableSchema();
        String table = "";
        HashMap<String, List<SourceField>> StringAndListSourceFieldMap = new HashMap<String, List<SourceField>>();
        ResultSet resultSet = null;
        ResultSet resultSet1 = null;
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            PreparedStatement preparedStatement1 = connection.prepareStatement(sqlString2);
            resultSet1 = preparedStatement1.executeQuery();
            while (resultSet1.next()) {
                String srctypeString1 = resultSet1.getString(1);
                String tgttypeString1 = resultSet1.getString(2);
                typeConvertMap.put(srctypeString1.trim().toLowerCase(), tgttypeString1.trim().toLowerCase());
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
                String column_cn_nameString = resultSet.getString("column_cn_name");
                String is_pkString = resultSet.getString("is_pk");
                String not_nullString = resultSet.getString("not_null");
                String default_valueString = resultSet.getString("default_value");
                String is_dkString = resultSet.getString("is_dk");
                String break_flagString = resultSet.getString("break_flag");
                List<SourceField> sourceFields = new ArrayList<SourceField>();
                //转换表和列
                //转换表的校验语句
                String tablenamecheckString = table_nameString.toLowerCase() + "," + sysString;
                //转化列的校验语句
                String columncheckString = column_nameString + "," + table_nameString + "," + sysString;
                //转换单词校验srcTableSchema + "," + db_sid + "," + sys
                String schemacheckString = db_schemaString + "," + db_sidString + "," + sysString;
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
                SourceField sourceField = new SourceField(sysString, db_sidString, db_schemaString, table_nameString, column_id, column_nameString, column_typeString
                        , column_cn_nameString, is_pkString, not_nullString, default_valueString, is_dkString, break_flagString);
                if (StringAndListSourceFieldMap.containsKey(keyString)) {
                    List<SourceField> sourceFieldList = StringAndListSourceFieldMap.get(keyString);
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
