package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import java.io.File;
import java.net.URLDecoder;
import org.apache.commons.io.FileUtils;

public class TestOracleAndImpalaFile {

    public static void main(String[] args) throws Exception {
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
        srcdbToTgtdb.generateDDL_Snapshot(); //快照表DDL
        srcdbToTgtdb.generateDDL_Partition();  //分区表DDL
        /**
         * oracle dml  生成调整到 OracleDmlCreator
         */
        //srcdbToTgtdb.generateDML_SnapshotToPartition(); //Oracle DML脚本文件（快照表-> 分区表》）
        System.out.println("---- Oracle OK----");

        /*
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
        */
        
    }
}

