package com.moumou.bootmybatisdemo.dataAlignment.ext.kettle;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import com.moumou.bootmybatisdemo.dataAlignment.common.StringExtension;
import com.moumou.bootmybatisdemo.dataAlignment.dao.SourceFieldDao;
import com.moumou.bootmybatisdemo.dataAlignment.dao.SourceSystemDao;
import com.moumou.bootmybatisdemo.dataAlignment.dao.SourceTableDao;
import com.moumou.bootmybatisdemo.dataAlignment.metamgr.SrcdbToTgtdb;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;


public class OracleDmlCreator {
	
    //shcema转化配置 ，key：srcTableSchema + "," + db_sid + "," + sys
    private HashMap<String, String> schemaMap = null;
    //tablename转换配置 key: srcTableName.toLowerCase() + "," + sys
    private HashMap<String, String> tableNameMap = null;
    //字段转换配置 key:srccolumn + "," + table + "," +  sys
    private HashMap<String, String> ColumnNameMap = null;
    
    private final String sql_date_format = "YYYYMMDD";
    private final String query_parallel_num = "4";
    private final String dml_parallel_num = "2";
    private final String ddl_parallel_num = "1";
    private final String ods_schema = "ODS";
    private final String max_dt = "20991231";
    private final String min_dt = "19000101";
    
    public OracleDmlCreator(){
        SrcdbToTgtdb _db = new SrcdbToTgtdb();
        schemaMap = _db.getConvertTableSchema();
        tableNameMap = _db.getConvertTable();
        ColumnNameMap = _db.getConvertColumn();
    }
    
    
    /**
     * 生成所有oracle ods dml 脚本 
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public boolean createAllOdsDml() throws SQLException, IOException {
        boolean ret = true;
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        java.util.List<SourceSystem> sysList = sourceSysDao.getSourceSystem();
        for(SourceSystem sys : sysList){
        	createOdsDml(sys.getSys(),sys.getDbSid(),sys.getDbSchema());
        }
        return ret;
    }
    
    /**
     * 
     * @param sysCode
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public boolean createOdsDml(String sysCode) throws SQLException, IOException {
        boolean ret = true;
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        List<SourceSystem> srcSysList = sourceSysDao.getSourceSystem(sysCode);

        for (SourceSystem sys : srcSysList) {      	
        	System.out.println("INFO:开始"+sysCode+"生成");
        	createOdsDml(sysCode,sys.getDbSid(),sys.getDbSchema());
        	System.out.println("INFO:完成"+sysCode+"生成");
        }
        return ret;
    }
    

    /**
     * 
     * @param sysCode
     * @param sid
     * @param schema
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public boolean createOdsDml(String sysCode,String sid,String schema) throws SQLException, IOException {
    	boolean ret = true;
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        //key:schema + "," + sid + "," + sys
        java.util.HashMap<String, SourceSystem> sourceSysMap = sourceSysDao.getSourceSystemToMap();

        SourceTableDao sourceTabDao = new SourceTableDao();
        java.util.List<SourceTable> list = sourceTabDao.getTableInfo_Filter(sysCode, sid, schema);
       
        SourceFieldDao sourceFieldDao = new SourceFieldDao();
        List<SourceField> fieldlist = sourceFieldDao.getFieldInfo(sysCode, sid, schema);
        for (SourceTable tab : list) {
        	if(!tab.getIs_put_to_etldbString().equalsIgnoreCase("Y")) { //添加是否入仓表的判断逻辑
        		continue;
        	}
        	
            String sysKey = tab.getTable_schemaString() + "," + tab.getDb_sidString() + "," + tab.getSyString();
            SourceSystem srcSys = sourceSysMap.get(sysKey);
            if (srcSys == null) {
                System.out.println("ERROR:" + tab.getSyString() + "." + tab.getTable_nameString() + "未找到源系统配置信息！");
                continue;
            }
            
            String tabKey = tab.getSyString()+tab.getDb_sidString()+tab.getTable_schemaString()+tab.getTable_nameString();
            List<SourceField>  oneTableColums = new java.util.ArrayList<SourceField>();
            for(SourceField field:fieldlist){
            	String _tabKey = field.getSysString()+field.getDb_sidString()+field.getDb_schemaString()+field.getTable_nameString();
            	if(tabKey.equals(_tabKey)){
            		oneTableColums.add(field);
            	}
            }
            if(oneTableColums.size()==0){
            	 System.out.println("ERROR:" + tab.getSyString() + "." + tab.getTable_nameString() + "未找到字段配置信息！");
                 continue;	
            }
            if("1".equals(tab.getTemplate_codeString())){
	            createZipperDml(srcSys,tab,oneTableColums);
            }else{//todo 增加增量模板算法
            	createSnapshotDml(srcSys,tab,oneTableColums);
            }
        }
        return ret;
    }

    
	/**
	 * 
	 * @param sysCode
	 * @param sid
	 * @param schema
	 * @param tableName
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
    public boolean createOdsDml(String sysCode,String sid,String schema,String tableName) throws SQLException, IOException {
    	boolean ret = true;
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        //key:schema + "," + sid + "," + sys
        java.util.HashMap<String, SourceSystem> sourceSysMap = sourceSysDao.getSourceSystemToMap();

        SourceTableDao sourceTabDao = new SourceTableDao();
        SourceTable tab = sourceTabDao.getTableInfo_Filter(sysCode, sid, schema, tableName);
       	if(tab.getIs_put_to_etldbString().equalsIgnoreCase("Y")) { //添加是否入仓表的判断逻辑
       	 String sysKey = tab.getTable_schemaString() + "," + tab.getDb_sidString() + "," + tab.getSyString();
         SourceSystem srcSys = sourceSysMap.get(sysKey);
         if (srcSys == null) {
             System.out.println("ERROR:" + tab.getSyString() + "." + tab.getTable_nameString() + "未找到源系统配置信息！");
             ret = false;
         }else{
		        SourceFieldDao sourceFieldDao = new SourceFieldDao();
		        List<SourceField> list = sourceFieldDao.getFieldInfo(tab.getSyString(), tab.getTable_nameString());
	            if("1".equals(tab.getTemplate_codeString())){
	            	createZipperDml(srcSys,tab,list);
	            }else {
	            	createSnapshotDml(srcSys,tab,list);
	            }
                System.out.println("[INFO]:完成" + sysCode + " 作业：" + tab.getTable_nameString() + "/" );
         }

        }
 
        return ret;
    }
    
    
    
	/**
	 * 生成拉链算法dml
	 * @param srcSys 源系统信息
	 * @param tab 表信息
	 * @param fieldList字段信息
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	 private void createZipperDml(SourceSystem srcSys, SourceTable tab, List<SourceField> fieldList) throws SQLException, IOException {
	        boolean ret = true;

	        SourceFieldDao sourceFieldDao = new SourceFieldDao();
	       
	        //目标名命名：源系统简称_(schmea代码)
	        String tgtTableName = tab.getSyString();
	        String srcSchemaKey = tab.getTable_schemaString() + "," + tab.getDb_sidString() + "," + tab.getSyString();
	        if (schemaMap != null && schemaMap.containsKey(srcSchemaKey)) {
	            tgtTableName = tgtTableName + "_" + schemaMap.get(srcSchemaKey);
	        }
	        String srcTabKey = tab.getTable_nameString().toLowerCase() + "," + tab.getSyString();
	        if (tableNameMap != null && tableNameMap.containsKey(srcTabKey)) {
	            tgtTableName = tgtTableName + "_" + tableNameMap.get(srcTabKey);
	        } else {
	            tgtTableName = tgtTableName + "_" + tab.getTable_nameString();
	        }

	        //当天数据来源表_m
	        String srcTableName = tgtTableName+"_m";
	        //备份中间表
	        String tgtBakTableName = tgtTableName+"_bk";
	        
	        //拉链算法临时使用表
	        String tgtOpTempTableName = tgtTableName+"_op";
	        String tgtCpTempTableName = tgtTableName+"_cp";
	        
	        
	        //目标表所有字段
	        java.util.List<String> ods_all_columns = new java.util.ArrayList<String>();
	        //目标表的主键字段
	        java.util.List<String> ods_is_pk_columns = new java.util.ArrayList<String>();
	        //目标表的非主键字段
	        java.util.List<String> ods_not_pk_columns = new java.util.ArrayList<String>();	        

	        for (int idx = 0; fieldList != null && idx < fieldList.size(); idx++) {
	            SourceField sourceField = fieldList.get(idx);
	            //判断字段超长转换
	            String srcColumnName = sourceField.getColumn_naString();
	            String tgtColumnName = srcColumnName;
	            String columnKey = srcColumnName.toLowerCase() + "," + sourceField.getTable_nameString() + "," + sourceField.getSysString();
	            if (ColumnNameMap != null && ColumnNameMap.containsKey(columnKey)) {
	                tgtColumnName = ColumnNameMap.get(columnKey);
	            }
	            //判断是否为oracle 数据库关键 ,比字段超长优先级高
	            String keywordConvert = SrcdbToTgtdb.getConvertKeyWord(srcColumnName);
	            boolean oracleKeyWordConvert = false;
	            if (!keywordConvert.equals(srcColumnName)) {
	                oracleKeyWordConvert = true;
	                tgtColumnName = keywordConvert;
	            }

	            ods_all_columns.add(tgtColumnName);
	            if("Y".equals(sourceField.getIs_pkString())){
	            	ods_is_pk_columns.add(tgtColumnName);	            	
	            }else{
	            	ods_not_pk_columns.add(tgtColumnName);
	            }

	        Velocity.init();
	        VelocityContext context = new VelocityContext();
	        //
	        context.put("query_parallel_num", query_parallel_num);
	        context.put("dml_parallel_num", dml_parallel_num);
	        context.put("ddl_parallel_num", ddl_parallel_num);	        
	        context.put("ods_schema", ods_schema);	      
	        context.put("ods_table_name_bk", tgtBakTableName);	     
	        context.put("ods_table_name", tgtTableName);	     
	        context.put("ods_table_name_op", tgtOpTempTableName);	   
	        context.put("ods_table_name_cl", tgtCpTempTableName);	
	        context.put("ods_m_table_name", srcTableName);	
	        context.put("sql_date_format", sql_date_format);	
	        context.put("max_dt", max_dt);	
	        context.put("min_dt", min_dt);	
	        
	        context.put("ods_is_pk_columns", ods_is_pk_columns);	
	        context.put("ods_all_columns", ods_all_columns);	
	        context.put("ods_not_pk_columns", ods_not_pk_columns);	
	        
	        Template template = Velocity.getTemplate("ods_oracle_ziper_sql_template.sql");

	        //生成kettle作业存放目录
	        String ziperDmlDir = URLDecoder.decode(OracleDmlCreator.class.getClassLoader().getResource("").getPath(), "utf-8")
					+"/kettle_project/oracle/";
	        String filePah = ziperDmlDir+ StringExtension.toStyleString(srcSys.getSys()) + "/";
	        java.io.File _file = new java.io.File(filePah);
	        if (!_file.exists()) {
	            _file.mkdirs();
	        }

	        String fileName = tgtTableName + ".sql";	        
	        fileName = StringExtension.toStyleString(fileName);

	        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePah + fileName), "UTF-8")));
	        template.merge(context, writer);
	        writer.close();
	        }
	    }

	 /**
	  * 
	  * @param srcSys
	  * @param tab
	  * @param fieldList
	  * @throws SQLException
	  * @throws IOException
	  */
	 private void createSnapshotDml(SourceSystem srcSys, SourceTable tab, List<SourceField> fieldList) throws SQLException, IOException {
	        boolean ret = true;

	        SourceFieldDao sourceFieldDao = new SourceFieldDao();
	       
	        //目标名命名：源系统简称_(schmea代码)
	        String tgtTableName = tab.getSyString();
	        String srcSchemaKey = tab.getTable_schemaString() + "," + tab.getDb_sidString() + "," + tab.getSyString();
	        if (schemaMap != null && schemaMap.containsKey(srcSchemaKey)) {
	            tgtTableName = tgtTableName + "_" + schemaMap.get(srcSchemaKey);
	        }
	        String srcTabKey = tab.getTable_nameString().toLowerCase() + "," + tab.getSyString();
	        if (tableNameMap != null && tableNameMap.containsKey(srcTabKey)) {
	            tgtTableName = tgtTableName + "_" + tableNameMap.get(srcTabKey);
	        } else {
	            tgtTableName = tgtTableName + "_" + tab.getTable_nameString();
	        }

	        String srcTabName = tgtTableName+"_m";
	        //临时使用表
	        String tgtOpTempName = tgtTableName+"_zz";
	        
	        
	        //目标表所有字段
	        java.util.List<String> ods_all_columns = new java.util.ArrayList<String>();

	        for (int idx = 0; fieldList != null && idx < fieldList.size(); idx++) {
	            SourceField sourceField = fieldList.get(idx);
	            //判断字段超长转换
	            String srcColumnName = sourceField.getColumn_naString();
	            String tgtColumnName = srcColumnName;
	            String columnKey = srcColumnName.toLowerCase() + "," + sourceField.getTable_nameString() + "," + sourceField.getSysString();
	            if (ColumnNameMap != null && ColumnNameMap.containsKey(columnKey)) {
	                tgtColumnName = ColumnNameMap.get(columnKey);
	            }
	            //判断是否为oracle 数据库关键 ,比字段超长优先级高
	            String keywordConvert = SrcdbToTgtdb.getConvertKeyWord(srcColumnName);
	            boolean oracleKeyWordConvert = false;
	            if (!keywordConvert.equals(srcColumnName)) {
	                oracleKeyWordConvert = true;
	                tgtColumnName = keywordConvert;
	            }

	            ods_all_columns.add(tgtColumnName);


	        Velocity.init();
	        VelocityContext context = new VelocityContext();
	        //
	        context.put("query_parallel_num", query_parallel_num);
	        context.put("dml_parallel_num", dml_parallel_num);
	        context.put("ddl_parallel_num", ddl_parallel_num);	        
	        context.put("ods_schema", ods_schema);	      
	        context.put("ods_table_name_zz", tgtOpTempName);
	        context.put("ods_table_name_m",srcTabName );
	        
	        context.put("ods_table_name", tgtTableName);	     
	        context.put("sql_date_format", sql_date_format);	
	        context.put("ods_all_columns", ods_all_columns);	
	        
	        Template template = Velocity.getTemplate("ods_oracle_snapshot_sql_template.sql");

	        //生成kettle作业存放目录
	        String ziperDmlDir = URLDecoder.decode(OracleDmlCreator.class.getClassLoader().getResource("").getPath(), "utf-8")
					+"/kettle_project/oracle/";
	        String filePah = ziperDmlDir+ StringExtension.toStyleString(srcSys.getSys()) + "/";
	        java.io.File _file = new java.io.File(filePah);
	        if (!_file.exists()) {
	            _file.mkdirs();
	        }

	        String fileName = tgtTableName + ".sql";	        
	        fileName = StringExtension.toStyleString(fileName);

	        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePah + fileName), "UTF-8")));

	        //java.io.Writer writer = new FileWriter(filePah+fileName);
	        template.merge(context, writer);
	        writer.close();
	        System.out.println("INFO:完成"+tab.getTable_nameString()+"生成");
	        }
	    }
	 
		public static void main(String[] args) throws SQLException, IOException {
			 
		      	OracleDmlCreator odsTest4Oracle = new OracleDmlCreator();
		      	String[] sysArray = {
//		      			"JZJY",
//		      			"RZRQ",
//		      			"GPQQ",
//		      			"OTC",
//		      			"CPZX",
//		      			"ZJGL",
//		      			"TYZH",
//		      			"O32",
//		      			"HYZJ",
//		      			"GZZY",
//		      			"HSTA",
//		      			"CRM",
//		      			"THXT",
//		      			"FRQS",
//		      			"GZZG"
		      			"JZJY",
		      			"CPZX"
		      			};
		      	for(int idx=0;idx<sysArray.length;idx++){
		      		odsTest4Oracle.createOdsDml(sysArray[idx]);
		      	}
			   
	}


}
