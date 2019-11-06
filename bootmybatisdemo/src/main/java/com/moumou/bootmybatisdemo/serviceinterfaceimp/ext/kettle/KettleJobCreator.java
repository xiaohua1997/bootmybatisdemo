package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.kettle;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.net.URLDecoder;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceFieldDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceSystemDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.SourceTableDao;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr.SrcdbToTgtdb;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;

public class KettleJobCreator {
	
    private final String edw_db_ip = "192.10.40.169";
    private final String edw_db_type = "oracle";
    private final String edw_db_port = "1521";
    private final String edw_db_username = "ods";
    private final String edw_db_sid = "edwdb";
    private final String edw_db_encrpwd = "Encrypted 2be98afc86aa7f2e4cb79ce10be9dabc9";

    //shcema转化配置 ，key：srcTableSchema + "," + db_sid + "," + sys
    private HashMap<String, String> schemaMap = null;
    //tablename转换配置 key: srcTableName.toLowerCase() + "," + sys
    private HashMap<String, String> tableNameMap = null;
    //字段转换配置 key:srccolumn + "," + table + "," +  sys
    private HashMap<String, String> ColumnNameMap = null;


    public KettleJobCreator() {
        SrcdbToTgtdb _db = new SrcdbToTgtdb();
        schemaMap = _db.getConvertTableSchema();
        tableNameMap = _db.getConvertTable();
        ColumnNameMap = _db.getConvertColumn();

    }

    /**
     * 生成所有kettlejob 
     * @param
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public void createAllKettleJob() throws SQLException, IOException {
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        java.util.List<SourceSystem> systemList = sourceSysDao.getSourceSystem();
        for(SourceSystem sys:systemList){
        	createKettleJob(sys.getSys());
        }
    }
    
    /**
     * @param sysCode
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public boolean createKettleJob(String sysCode) throws SQLException, IOException {
        boolean ret = true;
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        //key:schema + "," + sid + "," + sys
        java.util.HashMap<String, SourceSystem> sourceSysMap = sourceSysDao.getSourceSystemToMap();

        SourceTableDao sourceTabDao = new SourceTableDao();
        java.util.List<SourceTable> list = sourceTabDao.getTableInfo_Filter(sysCode);
        int tmp_cnt = 0;
        int job_cnt = list.size();
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
            createKettleJob(srcSys, tab);
            tmp_cnt++;
//            System.out.println("[INFO]:完成" + sysCode + " 作业：" + tmp_cnt + "/" + job_cnt);
        }
        System.out.println("[INFO]:完成" + sysCode + " 作业");
        return ret;
    }
    /**
     *  获取源表信息  
     * @param sysCode 系统代码
     * @param sid  数据库sid/名称
     * @param schema 表模式名
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public boolean createKettleJob(String sysCode,String sid,String schema) throws SQLException, IOException {
    	boolean ret = true;
        SourceSystemDao sourceSysDao = new SourceSystemDao();
        //key:schema + "," + sid + "," + sys
        java.util.HashMap<String, SourceSystem> sourceSysMap = sourceSysDao.getSourceSystemToMap();

        SourceTableDao sourceTabDao = new SourceTableDao();
        java.util.List<SourceTable> list = sourceTabDao.getTableInfo_Filter(sysCode, sid, schema);
        int tmp_cnt = 0;
        int job_cnt = list.size();
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
            createKettleJob(srcSys, tab);
            tmp_cnt++;
            System.out.println("[INFO]:完成" + sysCode + " 作业：" + tmp_cnt + "/" + job_cnt);
        }
        return ret;
    }

    /**
     * 获取源表信息
     * @param sysCode
     * @param sid
     * @param schema
     * @param tableName
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public boolean createKettleJob(String sysCode,String sid,String schema,String tableName) throws SQLException, IOException {
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
             createKettleJob(srcSys, tab);
             System.out.println("[INFO]:完成" + sysCode + " 作业：" + tab.getTable_schemaString() + "/" );
         }

        }
 
        return ret;
    }

    
    /**
     * 创建一个table 的采集作业
     *
     * @param tab
     * @return
     * @throws SQLException
     * @throws IOException
     */
    private boolean createKettleJob(SourceSystem srcSys, SourceTable tab) throws SQLException, IOException {
        boolean ret = true;

        SourceFieldDao sourceFieldDao = new SourceFieldDao();
        List<SourceField> list = sourceFieldDao.getFieldInfo(tab.getSyString(),tab.getDb_sidString(),tab.getTable_schemaString(),tab.getTable_nameString());
        String etl_dt = "to_date('${batch_date}','yyyymmdd') as data_date";
        String etl_time = "sysdate as etl_time";
        String src_db_type = srcSys.getDbType();
        if (null != src_db_type) {
            src_db_type = src_db_type.toLowerCase();
            if ("mysql".equals(src_db_type)) {
                etl_dt = "str_to_date('${batch_date}','%Y%m%d') as data_date";
                etl_time = "current_timestamp as etl_time";
            } else if ("sqlserver".equals(src_db_type) || "mssql".equals(src_db_type)) {
                etl_dt = "cast('${batch_date}' as datetime) as data_date";
                etl_time = "GETDATE() as etl_time";
            }
        }

        String sql = "select " + etl_dt;


        //目标名命名：源系统简称_(schmea代码)_表名_m
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
        tgtTableName += "_m";


        //目标表字段
        java.util.List<String> columns = new java.util.ArrayList<String>();
        columns.add("data_date");

        for (int idx = 0; list != null && idx < list.size(); idx++) {
            SourceField sourceField = list.get(idx);
            //判断字段超长转换
            String srcColumnName = sourceField.getColumn_naString();
            String tgtColumnName = srcColumnName;
            boolean tgtColumnNameConvert = false;//字段超长转换
            String columnKey = srcColumnName.toLowerCase() + "," + sourceField.getTable_nameString() + "," + sourceField.getSysString();
            if (ColumnNameMap != null && ColumnNameMap.containsKey(columnKey)) {
                tgtColumnName = ColumnNameMap.get(columnKey);
                tgtColumnNameConvert = true;
            }
            sql += ",";

            //判断是否为sqlserver的datetime类型，如果是需要转为不带毫秒的date的格式
            boolean sqlServerDateTimeCovert = false;
            if ("sqlserver".equals(src_db_type) || "mssql".equals(src_db_type)) {
                if (sourceField.getColumn_typeString().toLowerCase().indexOf("date") >= 0 || sourceField.getColumn_typeString().toLowerCase().indexOf("time") >= 0) {
                    sqlServerDateTimeCovert = true;
                }
            }
            //判断类型是否在bit
            boolean contionsBitCovert = false;
            if (sourceField.getColumn_typeString().toLowerCase().indexOf("bit") >= 0) {
                contionsBitCovert = true;
            }

            boolean outOfRange = false;
            if (sourceField.getTable_nameString().equalsIgnoreCase("product_trend_value") && sourceField.getColumn_nameString().equalsIgnoreCase("total_otherright")) {
                outOfRange = true;
            }
            //判断是否为oracle 数据库关键 ,比字段超长优先级高
            String keywordConvert = SrcdbToTgtdb.getConvertKeyWord(srcColumnName);
            boolean oracleKeyWordConvert = false;
            if (!keywordConvert.equals(srcColumnName)) {
                oracleKeyWordConvert = true;
                tgtColumnName = keywordConvert;
            }

            columns.add(tgtColumnName.trim());
            /**
             * delete by cm 20190627 通过exists 方式在增量条件里处理，不在程序逻辑中处理
             */
//            if (tab.getTable_nameString().equalsIgnoreCase("STK_ACCTBIZ_LOGEX")) {
//                srcColumnName = tab.getTable_nameString() + "." + srcColumnName;
//            }
            
            //kettle采集，设计源目标字段不一致下，需要通过别名方式处理
            if (oracleKeyWordConvert) {
                if ("Y".equals(sourceField.getBreak_flag())) {//判断是否需要采集,Y不需要采集 @todo 目前此类场景针对大字段
                    sql = sql + "' ' as " + tgtColumnName;
                } else if (sqlServerDateTimeCovert) {
                    sql = sql + " cast(" + srcColumnName + " as datetime) as " + tgtColumnName;
                } else if (contionsBitCovert) {
                    sql = sql + " cast(" + srcColumnName + " as char) as " + tgtColumnName;
                } else {
                	//临时增加判断是否为CURRENT_DATE
                	String _tempsrcColumnName = srcColumnName;
                	if("CURRENT_DATE".equals(_tempsrcColumnName.toUpperCase())){
                		sql = sql + "\""+srcColumnName+"\"" + " as " + tgtColumnName;         		
                	}else{
                		sql = sql + srcColumnName + " as " + tgtColumnName;
                	}
                }

            } else {
                if ("Y".equals(sourceField.getBreak_flag())) {//判断是否需要采集,Y不需要采集 @todo 目前此类场景针对大字段
                    sql = sql + "' ' as " + tgtColumnName;
                } else if (sqlServerDateTimeCovert) {
                    sql = sql + " cast(" + srcColumnName + " as datetime) as " + tgtColumnName;
                } else if (contionsBitCovert) {
                    sql = sql + " cast(" + srcColumnName + " as char) as " + tgtColumnName;
                } else if (tgtColumnNameConvert) {
                    sql = sql + " " + srcColumnName + " as " + tgtColumnName;
                } else if (outOfRange) {
                    //CASE WHEN total_otherright >9.99E125 THEN 9.99E125 ELSE total_otherright END AS total_otherright
                    sql = sql + "CASE WHEN " + srcColumnName + " >9.99E125 THEN 9.99E125 ELSE " + srcColumnName + " END as " + tgtColumnName;
                } else {
                    sql = sql + srcColumnName;
                }
            }


        }
        sql = sql + "," + etl_time;
        columns.add("etl_time");
        /**
         * delete by cm 20190627 通过exists 方式在增量条件里处理，不在程序逻辑中处理
         */
//        if (tab.getTable_nameString().equalsIgnoreCase("STK_ACCTBIZ_LOGEX")) {
//            sql = sql + " from " + tab.getTable_nameString() + ",STK_ACCTBIZ_LOG" + " where 1=1 ";
//        } else {
//            sql = sql + " from " + tab.getTable_nameString() + " where 1=1 ";
//        }
        sql = sql + " from " +tab.getTable_schemaString()+"."+ tab.getTable_nameString() + " where 1=1 ";
        if (tab.getInc_cdtString() != null && !tab.getInc_cdtString().trim().equals("")) {
            sql = sql + " and " + tab.getInc_cdtString();
        }

        Velocity.init();
        VelocityContext context = new VelocityContext();
        //目标表名
        context.put("table_name", tgtTableName);
        //临时增加 oracle rac 连接方式修改
        context.put("db_name_out", srcSys.getDbType() + "_" + srcSys.getSys() + "_" + srcSys.getDbIp());
        context.put("sqltype_out", srcSys.getDbType());
        if("O32".equals(srcSys.getSys())){
        	context.put("ip_out", "");
            context.put("port_out", "");
            String connStr = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST="+srcSys.getDbIp()+")(PORT="+srcSys.getDbPort()+")))(CONNECT_DATA=(SERVICE_NAME="+srcSys.getDbSid()+")))";
            context.put("sid_out", connStr);
            
        }else {
	     	context.put("ip_out", srcSys.getDbIp());
	        context.put("port_out", srcSys.getDbPort());
	        context.put("sid_out", srcSys.getDbSid());
        }
        context.put("user_out", srcSys.getUserName());
        context.put("pswd_out", srcSys.getEncrPassword());
        
        context.put("db_name_in", "ODS");//ods
        context.put("sqltype_in", "oracle");
        context.put("ip_in", edw_db_ip);
        context.put("port_in", edw_db_port);
        context.put("user_in", edw_db_username);
        context.put("pswd_in", edw_db_encrpwd);
        context.put("sid_in", edw_db_sid);

        context.put("job_name", srcSys.getDbType() + "_" + srcSys.getSys() + "_" + srcSys.getDbIp());
        context.put("attr_table", "");
        context.put("sql", sql);
        context.put("allcolumns", columns);
        Template template = Velocity.getTemplate("kettle.vi");

        //生成kettle作业存放目录
        String kettleDir = URLDecoder.decode(KettleJobCreator.class.getClassLoader().getResource("").getPath(), "utf-8")
				+"/kettle_project";
        String filePah = kettleDir+"/kettle/" + StringExtension.toStyleString(srcSys.getSys()) + "/";
        java.io.File _file = new java.io.File(filePah);
        if (!_file.exists()) {
            _file.mkdirs();
        }

        String fileName = tgtTableName.substring(0, tgtTableName.length() - 2) + ".xml";
        fileName = StringExtension.toStyleString(fileName);

        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePah + fileName), "UTF-8")));

        //java.io.Writer writer = new FileWriter(filePah+fileName);
        template.merge(context, writer);
        writer.close();

        return ret;
    }

    /*public void oracleKettle() throws SQLException, IOException {
    	List<String> s = srcSystemMapper.querySys();
    	for(String sys:s) {
    		new KettleJobCreator().createKettleJob(sys);
    	}*/
            /*new KettleJobCreator().createKettleJob("CPZX");
            new KettleJobCreator().createKettleJob("CRM");
            new KettleJobCreator().createKettleJob("CWNC");
            new KettleJobCreator().createKettleJob("FRQS");
            new KettleJobCreator().createKettleJob("FXC");
            new KettleJobCreator().createKettleJob("FXQ");
            new KettleJobCreator().createKettleJob("GPQQ");
            new KettleJobCreator().createKettleJob("GZZG");
            new KettleJobCreator().createKettleJob("GZZY");
            new KettleJobCreator().createKettleJob("HSTA");
            new KettleJobCreator().createKettleJob("HYZJ");
            new KettleJobCreator().createKettleJob("JZB");
            new KettleJobCreator().createKettleJob("JZJY");
            new KettleJobCreator().createKettleJob("O32");
            new KettleJobCreator().createKettleJob("OTC");
            new KettleJobCreator().createKettleJob("RZRQ");
            new KettleJobCreator().createKettleJob("THXT");
            new KettleJobCreator().createKettleJob("TYZH");
            new KettleJobCreator().createKettleJob("YCJY");
            new KettleJobCreator().createKettleJob("ZJGL");
            new KettleJobCreator().createKettleJob("FXC");*/
    /*}*/
    /*public static void main(String[] args) throws SQLException, IOException {
    	new KettleJobCreator().oracleKettle();
    }*/
   
}
