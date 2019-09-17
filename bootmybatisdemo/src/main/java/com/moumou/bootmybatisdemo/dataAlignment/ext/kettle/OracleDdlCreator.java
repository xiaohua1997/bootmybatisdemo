package com.moumou.bootmybatisdemo.dataAlignment.ext.kettle;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;

import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.dataAlignment.metamgr.SrcdbToTgtdb;
import com.moumou.bootmybatisdemo.dataAlignment.metamgr.TestOracleAndImpalaFile;

/**
 * oracle 数据库 ddl 生成
 * @author simon
 *
 */
public class OracleDdlCreator {


    
    public static void main(String[] args) throws Exception {
    	
    }   
    
    
   
    /**
     * 按层次生成 
     * @param level all:所有 ;ods_m:当日数据层;ods:历史数据层
      * @throws IOException
     */
    public void createAllOracleDdl(String level) throws IOException{
    	
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
        System.out.println("-------------Oracle DDL Create Start------------------");
        if("ods_m".equals(level)){
        	srcdbToTgtdb.generateDDL_Snapshot(); //快照表DDL
        }else if("ods".equals(level)){
        	srcdbToTgtdb.generateDDL_Partition();  //分区表DDL
        }else if("all".equals(level)){
        	srcdbToTgtdb.generateDDL_Snapshot(); //快照表DDL
        	srcdbToTgtdb.generateDDL_Partition();  //分区表DDL
        }
        System.out.println("-------------Oracle DDL Create End------------------");
    } 
    
    
}
