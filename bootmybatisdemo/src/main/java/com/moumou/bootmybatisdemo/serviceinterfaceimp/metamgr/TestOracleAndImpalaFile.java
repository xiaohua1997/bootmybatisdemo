package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;

public class TestOracleAndImpalaFile {
	/*
	 * @param level 目前可能存在如下几种传入值：all，ods，ods_m（仅限oracle），chk
	 * @param dbtype 目前可能存在如下几种传入值：impala，oracle
	 */
	public void generate(String level, String dbtype) 
			throws Exception {
			if (dbtype.equals("impala")) {
				//impalaFile(level, null, null, null, null);
			} else if (dbtype.equals("oracle")) {
				oracleFile(level);
			}
		
	}
	
	public void generate(String level, String dbtype, String sys) 
			throws Exception {
		if (dbtype.equals("impala")) {
			//impalaFile(level, null, null, null, null);
		} else if (dbtype.equals("oracle")) {
			oracleFile(level,sys);
		}
		
	}
	
	public void generate(String level, String dbtype, String sys, String sid, String schema) 
			throws Exception {
		if (dbtype.equals("impala")) {
			//impalaFile(level, null, null, null, null);
		} else if (dbtype.equals("oracle")) {
			oracleFile(level,sys,sid,schema);
		}
		
	}
	
	public void generate(String level, String dbtype, String sys, String sid, String schema, String tableName) 
			throws Exception {
		if (dbtype.equals("impala")) {
			//impalaFile(level, null, null, null, null);
		} else if (dbtype.equals("oracle")) {
			oracleFile(level,sys,sid,schema,tableName);
		}
		
	}

    public void oracleFile(String level) throws Exception {
    	
    	String ddlSnapshotTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020301 Oracle_当日_建表";
    	String ddlPartitionTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020302 Oracle_历史_建表";
    	String dmlSnapShotToPartitionDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/kettle_project/oracle";
        FileUtils.forceMkdir(new File(ddlSnapshotTableDir));
        FileUtils.forceMkdir(new File(ddlPartitionTableDir));
        FileUtils.forceMkdir(new File(dmlSnapShotToPartitionDir));

        SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb(ddlSnapshotTableDir, ddlPartitionTableDir, dmlSnapShotToPartitionDir);
        srcdbToTgtdb.generateDDL_Snapshot(level); //快照表DDL
        srcdbToTgtdb.generateDDL_Partition(level);  //分区表DDL
        /**
         * oracle dml  生成调整到 OracleDmlCreator
         */
       srcdbToTgtdb.generateDML_SnapshotToPartition(); //Oracle DML脚本文件（快照表-> 分区表》）
        System.out.println("---- Oracle OK----");
    }
    public void oracleFile(String level, String sys) throws Exception {
    	
    	String ddlSnapshotTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020301 Oracle_当日_建表";
    	String ddlPartitionTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020302 Oracle_历史_建表";
    	String dmlSnapShotToPartitionDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/kettle_project/oracle";
    	FileUtils.forceMkdir(new File(ddlSnapshotTableDir));
    	FileUtils.forceMkdir(new File(ddlPartitionTableDir));
    	FileUtils.forceMkdir(new File(dmlSnapShotToPartitionDir));
    	
    	SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb(ddlSnapshotTableDir, ddlPartitionTableDir, dmlSnapShotToPartitionDir);
    	srcdbToTgtdb.generateDDL_Snapshot(level,sys); //快照表DDL
    	srcdbToTgtdb.generateDDL_Partition();  //分区表DDL
    	/**
    	 * oracle dml  生成调整到 OracleDmlCreator
    	 */
    	srcdbToTgtdb.generateDML_SnapshotToPartition(); //Oracle DML脚本文件（快照表-> 分区表》）
    	System.out.println("---- Oracle OK----");
    }
    public void oracleFile(String level, String sys, String sid, String schema) throws Exception {
    	
    	String ddlSnapshotTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020301 Oracle_当日_建表";
    	String ddlPartitionTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020302 Oracle_历史_建表";
    	String dmlSnapShotToPartitionDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/kettle_project/oracle";
    	FileUtils.forceMkdir(new File(ddlSnapshotTableDir));
    	FileUtils.forceMkdir(new File(ddlPartitionTableDir));
    	FileUtils.forceMkdir(new File(dmlSnapShotToPartitionDir));
    	
    	SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb(ddlSnapshotTableDir, ddlPartitionTableDir, dmlSnapShotToPartitionDir);
    	srcdbToTgtdb.generateDDL_Snapshot(level,sys,sid,schema); //快照表DDL
    	srcdbToTgtdb.generateDDL_Partition();  //分区表DDL
    	/**
    	 * oracle dml  生成调整到 OracleDmlCreator
    	 */
    	srcdbToTgtdb.generateDML_SnapshotToPartition(); //Oracle DML脚本文件（快照表-> 分区表》）
    	System.out.println("---- Oracle OK----");
    }
    public void oracleFile(String level, String sys, String sid, String schema, String tableName) throws Exception {
    	
    	String ddlSnapshotTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020301 Oracle_当日_建表";
    	String ddlPartitionTableDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/020302 Oracle_历史_建表";
    	String dmlSnapShotToPartitionDir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
    			+ "/kettle_project/oracle";
    	FileUtils.forceMkdir(new File(ddlSnapshotTableDir));
    	FileUtils.forceMkdir(new File(ddlPartitionTableDir));
    	FileUtils.forceMkdir(new File(dmlSnapShotToPartitionDir));
    	
    	SrcdbToTgtdb srcdbToTgtdb = new SrcdbToTgtdb(ddlSnapshotTableDir, ddlPartitionTableDir, dmlSnapShotToPartitionDir);
    	srcdbToTgtdb.generateDDL_Snapshot(level,sys,sid,schema,tableName); //快照表DDL
    	srcdbToTgtdb.generateDDL_Partition();  //分区表DDL
    	/**
    	 * oracle dml  生成调整到 OracleDmlCreator
    	 */
    	srcdbToTgtdb.generateDML_SnapshotToPartition(); //Oracle DML脚本文件（快照表-> 分区表》）
    	System.out.println("---- Oracle OK----");
    }
    
    /*
     * 用来测试的主函数
     */
    public static void main(String[] args) {
    	TestOracleAndImpalaFile s = new TestOracleAndImpalaFile();
    	try {
			s.oracleFile("all","JZJY","his","dbo","h_logmateno");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
     public void impalaFile() throws IOException{
        	
        String impala_ddl_dir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
        		+ "/020303 Impala_当日_建表";
        String impala_dml_dir = URLDecoder.decode(TestOracleAndImpalaFile.class.getClassLoader().getResource("").getPath(), "utf-8")
        		+ "/sqoop_project/sqoop";
        FileUtils.forceMkdir(new File(impala_ddl_dir));
        FileUtils.forceMkdir(new File(impala_dml_dir));

        System.out.println("-------------------------");
        MysqlToImpala mysqlToImpala = new MysqlToImpala(impala_ddl_dir, impala_dml_dir);
        mysqlToImpala.sqlCreateFile();
        mysqlToImpala.sqlSelectFile();
        System.out.println("----Hadoop OK----");
    }
}

