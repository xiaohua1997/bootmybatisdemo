package com.moumou.bootmybatisdemo.dataAlignment.metamgr;

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

import com.moumou.bootmybatisdemo.dataAlignment.db.JdbcConnection;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;

public class ImlapaColumnInfoExcel {

    HashSet<String> colset = new HashSet<String>();
    HashMap<String, String> hashMap1 = new HashMap<String, String>();
    
    //循环创建hive建表语句  内部调用createTableScript("mysql", "hive", srctypeString1,tgttypeString1);
    public List<String[]> qryColumnInfoGather(String sys) {
        String sqlString;
        if("%" != sys){
            sqlString = "SELECT sys, db_schema from src_column WHERE sys = \'"+ sys +"\' GROUP BY sys,db_schema order by column_id asc";
        }else{
            sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema order by column_id asc";
        }
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        ImlapaColumnInfoExcel imlapaColumnInfoExcel = new ImlapaColumnInfoExcel();
        List<String[]> dataListImpalaAll = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String srctypeString1 = resultSet.getString(1);
                String tgttypeString1 = resultSet.getString(2);
                //System.out.println("\"" + srctypeString1 + "," + tgttypeString1 +"\"");
                List<String[]> dataList = imlapaColumnInfoExcel.qryColumnInfo("mysql", "hive", srctypeString1, tgttypeString1);
                dataListImpalaAll.addAll(dataList);
            }  
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return dataListImpalaAll;
    }

    //生成指定prefixname和schemaName 的文件
    //内部调用getTableDes(srcString,tgtString,sqlString);
    //getSqlString(list,prefixName);  入参  mysql,hive,sys,schema
    private List<String[]> qryColumnInfo(String srcString, String tgtString, String prefixName, String schemaName) {
        String sqlString = "SELECT case when stb.sys is null then scl.sys else stb.sys end 'sys', scl.db_sid, scl.db_schema, scl.table_name, scl.column_id, scl.column_name, scl.column_type, scl.column_cn_name, scl.is_pk, scl.not_null, scl.default_value, scl.is_dk, scl.break_flag "
        		+ ", stb.table_cn_name "
        		+ " from src_column scl left join src_table stb on scl.sys=stb.sys and stb.table_name=scl.table_name where scl.sys='" + prefixName + "' and scl.db_schema='" + schemaName + "' ORDER BY scl.sys,scl.db_schema,scl.column_id asc;";
        //String sqlString = "SELECT * from src_column where sys='HSPB' and db_schema='trade' ORDER BY sys,db_schema,column_id desc;";

        HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, tgtString, sqlString);
        //System.out.println(list);
        List<String[]> dataList = getSqlString(stringListHashMap, prefixName);
        return dataList;
    }

    //获取转换表的数据
    public HashMap<String, String> getConvertTable() {
        HashMap<String, String> tableConvert = new HashMap<String, String>();
        String sqlString = "SELECT * from src_tablename_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
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

    //获取数据库类型的数据并保存在map中
    public HashMap<String, String> getSys() {
        HashMap<String, String> sysConvert = new HashMap<String, String>();
        String sqlString = "SELECT sys,sys_num,db_type,db_sid,db_schema from src_system";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
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

    //将字段信息转换 装入list  供输出到excel文件
    private List<String[]> getSqlString(HashMap<String, List<SourceField>> map, String prefixName) {
        List<String[]> dataList = new ArrayList<String[]>();
        Iterator<Map.Entry<String, List<SourceField>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<SourceField>> entry = iterator.next();
            String key = entry.getKey();
            String afterTableName = "ods_" + key;
            String tableCnName = "";
            List<SourceField> sourceFields = entry.getValue();
            //第一个字段 data_tate
            
            int columnId = 2;
            for (SourceField sourceField : sourceFields) {
            	
            	tableCnName = sourceField.getTable_cn_nameString();
            	columnId = sourceField.getColumn_id()+1;
            	String columnName = sourceField.getColumn_nameString().toLowerCase();
            	String columnCnName = sourceField.getColumn_cn_nameString();
            	if("null".equals(columnCnName) || null == columnCnName){
            		columnCnName = "";
            	}
            	if("0.0".equals(columnCnName)){
            		columnCnName = "";
            	}
            	String columnType = sourceField.getColumn_typeString().toLowerCase();
                if (columnType == null) {
                    System.out.println("我空了" + sourceField);
                }
                int start = columnType.lastIndexOf('(');
                int end = columnType.lastIndexOf(')');
                if (columnType.toLowerCase().contains("decimal")) {
                	columnType = columnType.toLowerCase();
                } else {
                    if (start > 0 && end > 0) {
                        String beforColumnType = columnType.substring(0, start);
                        String afterColumnType = getTrString(beforColumnType).toLowerCase();
                        if (afterColumnType.contains("decimal")) {
                        	columnType = afterColumnType + "(" + columnType.substring(start + 1, end) + ")";
                        } else {
//                        	columnType = afterColumnType.toUpperCase();
                        	columnType = afterColumnType.toLowerCase();
                        }
                    } else {
//                        columnType = getTrString(columnType).toUpperCase();
                        columnType = getTrString(columnType).toLowerCase();
                    }
                }
            	
            	String isPk = sourceField.getIs_pkString();
            	String isNull = sourceField.getNot_nullString();
            	if(sourceField.getColumn_id() == 1){
            	dataList.add(new String[] { "数据仓库", "EDW", "ODS", afterTableName, tableCnName, "1", "data_date",
            			"", "string", "是", "否" });
            	}
            	dataList.add(new String[] { "数据仓库", "EDW", "ODS", afterTableName, tableCnName, Integer.toString(columnId), columnName,
            			columnCnName, columnType, isPk, isNull });
            }
            dataList.add(new String[] { "数据仓库", "EDW", "ODS", afterTableName, tableCnName, Integer.toString(columnId+1), "etl_time",
        			"", "string", "是", "否" });
        }
        
        return dataList;
    }


    public String getTrString(String str) {
        if (hashMap1.containsKey(str.toLowerCase())) {
            return hashMap1.get(str);
        } else {
            System.out.println(str);
            System.exit(0);
            return str;
        }
    }

    //入参  mysql,hive 查询src_column所有信息的sql
    //猜测是转换mysql和hive字段类型的方法
    private HashMap<String, List<SourceField>> getTableDes(String srcString, String tgtString, String sqlString) {
        String sqlString2 = "SELECT src_column_type,tgt_column_type FROM etl_type_convert WHERE src_db_type='" + srcString + "' and tgt_db_type='" + tgtString + "';";
        HashMap<String, String> hashMap = new HashMap<String, String>();
        HashMap<String, String> sysConvert = getSys();
        HashMap<String, List<SourceField>> StringAndListSourceFieldMap = new HashMap<String, List<SourceField>>();
        ResultSet resultSet = null;
        ResultSet resultSet1 = null;
        List<String> list = new ArrayList<String>();
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
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
            hashMap1.put("numeric", "double");
            hashMap1.put("varchar2", "string");
            hashMap1.put("tinyint", "SMALLINT");
            hashMap1.put("integer", "int");
            hashMap1.put("cprice", "double");
            hashMap1.put("cmoney", "double");
            hashMap1.put("faild", "string");
            hashMap1.put("xml", "string");
            hashMap1.put("string", "string");
            hashMap1.put("bigdecimal", "decimal");
            hashMap1.put("date", "string");
            hashMap1.put("clob", "string"); // add by wsy
            hashMap1.put("real", "real"); // add by wsy
            hashMap1.put("nvarchar2", "string"); // add by wsy
            hashMap1.put("uniqueidentifier", "c"); // add by wsy
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
                if("Y".equals(is_pkString) || "y".equals(is_pkString)){
                	is_pkString = "是";
                }else if("n".equals(is_pkString) || "N".equals(is_pkString)){
                	is_pkString = "";
                }
                String not_nullString = resultSet.getNString("not_null");
                String default_valueString = resultSet.getNString("default_value");
                String is_dkString = resultSet.getNString("is_dk");
                String break_flagString = resultSet.getString("break_flag");
                String table_cn_nameString = resultSet.getString("table_cn_name");
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

                column_nameString = OracleColumnInfoExcel.getConvertKeyWord(column_nameString);
                SourceField sourceField = new SourceField(sysString, db_sidString, db_schemaString, table_nameString, column_id, column_nameString, column_typeString
                        , column_cn_nameString, is_pkString, not_nullString, default_valueString, is_dkString, break_flagString);
                //表中文名
                sourceField.setTable_cn_nameString(table_cn_nameString);
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

}
