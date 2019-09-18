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
import com.moumou.bootmybatisdemo.dataAlignment.model.TableInfo;


public class OracleColumnInfoExcel {


    static final String UNDERLINE = "_";
    static final String CURRENTDBNAME = "ODS";

    //ORACLE 和 impala 关键字
    private static HashSet<String> keyword = new HashSet<String>();

    static {
        String sqlString = "SELECT keyword FROM src_column,etl_keyword where src_column.column_name = etl_keyword.keyword GROUP BY keyword";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String keyword = resultSet.getString(1);
                OracleColumnInfoExcel.keyword.add(keyword.toLowerCase());

            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }

    //存储表名和列名
    HashMap<String, String> typeConvertMap = new HashMap<String, String>();

    /**
     * 转换关键字
     *
     * @param keyword
     * @return
     */
    public static String getConvertKeyWord(String keyword) {
        if (OracleColumnInfoExcel.keyword.contains(keyword.toLowerCase())) {
            keyword = keyword + "_ora";
        }
        return keyword;
    }

    /**
     * 返回所有的表的字段信息
     * @return 各系统查询出的表字段信息集合
     */
    public List<String[]> qryColumnInfoGather(String sys) {
        String sqlString;
        if("%" != sys){
            sqlString = "SELECT sys, db_schema from src_column WHERE sys = \'"+ sys +"\' GROUP BY sys,db_schema order by column_id asc";
        }else{
            sqlString = "SELECT sys, db_schema from src_column GROUP BY sys,db_schema";
        }
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        OracleColumnInfoExcel srcdbToTgtdb = new OracleColumnInfoExcel();
        List<String[]> dataListOracleAll = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String srctypeString1 = resultSet.getString(1);
                String tgttypeString1 = resultSet.getString(2);
                List<String[]> dataList = srcdbToTgtdb.qryColumnInfo("mysql", srctypeString1, tgttypeString1);
                dataListOracleAll.addAll(dataList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
        return dataListOracleAll;
    }

    /**
     * 获取转换表中 关于表的数据  src_tablename_convert
     * 注意对比的时候表名为小写
     * @return
     */
    public HashMap<String, String> getConvertTable() {
        HashMap<String, String> tableConvert = new HashMap<String, String>();

        String sqlString = "SELECT * from src_tablename_convert";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        OracleColumnInfoExcel srcdbToTgtdb = new OracleColumnInfoExcel();
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
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        OracleColumnInfoExcel srcdbToTgtdb = new OracleColumnInfoExcel();
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
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
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
     *  返回单个系统的表字段信息
     * @param srcString  mysql
     * @param prefixName sys 
     * @param schemaName db_schema
     */
    public List<String[]> qryColumnInfo(String srcString, String prefixName, String schemaName) {
        HashMap<String, String> tableConvertTableScheMap = getConvertTableSchema();
        String sqlString = "SELECT case when stb.sys is null then scl.sys else stb.sys end 'sys', scl.db_sid, scl.db_schema, scl.table_name, scl.column_id, scl.column_name, scl.column_type, scl.column_cn_name, scl.is_pk, scl.not_null, scl.default_value, scl.is_dk, scl.break_flag "
        		+ ", stb.table_cn_name "
        		+ " from src_column scl left join src_table stb on scl.sys=stb.sys and stb.table_name=scl.table_name where scl.sys='" + prefixName + "' and scl.db_schema='" + schemaName + "' ORDER BY scl.sys,scl.db_schema,scl.column_id asc;";
        HashMap<String, List<SourceField>> stringListHashMap = getTableDes(srcString, "oracle", sqlString);
        List<String[]> dataList = getsqlListToday(stringListHashMap, prefixName);
        return dataList  ; 
    }

    /**
     *  将所有字段的信息全部处理完毕
     * @param map
     * @param prefixName
     * @return
     */
    private List<String[]> getsqlListToday(HashMap map, String prefixName) {
    	List<String[]> dataList = new ArrayList<String[]>();
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        Iterator<Map.Entry<String, List<SourceField>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            StringBuffer sqlBuffer = new StringBuffer();
            Map.Entry<String, List<SourceField>> entry = iterator.next();
            String key = entry.getKey();
            String[] arr = key.split(",");
            String tableName = arr[0];
            String schema = "";
            String afterTableName = "";
            if (arr.length > 1) {
                schema = arr[1];
            }
            if (schema.length() > 0) {
               //表名=======================
                afterTableName = prefixName + "_" + schema + "_" + tableName;
            } else {
              //表名=======================
                afterTableName = prefixName + "_" + tableName;
            }
            String table_cn_nameString = "";
            List<SourceField> sourceFields = entry.getValue();
            System.out.println(tableName + "-------" + sourceFields.size());
            
            int column_id = 2;
            for (SourceField sourceField : sourceFields) {
            	//拿出表中文名
            	table_cn_nameString = sourceField.getTable_cn_nameString();
            	//字段序号
            	column_id = sourceField.getColumn_id()+1;
            	//拿出字段英文名
            	String column_nameString = sourceField.getColumn_nameString().toLowerCase();
            	if("null".equals(column_nameString) || null == column_nameString){
            		column_nameString = "";
            	}
            	if("0.0".equals(column_nameString)){
            		column_nameString = "";
            	}
            	//字段中文名
            	String column_cn_nameString = sourceField.getColumn_cn_nameString();
            	//字段类型
            	String column_typeString = sourceField.getColumn_typeString().toLowerCase();
            	//主键
            	String is_pkString = sourceField.getIs_pkString();
            	//是否为空
            	String not_nullString = sourceField.getNot_nullString();
            	
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
                        
                        //拿出字段类型
//                        column_typeString = getTrString(columnType).toUpperCase();
                        column_typeString = getTrString(columnType).toLowerCase();
                    } else {
                        int size = Integer.parseInt(columnType.substring(start + 1, end));
                        String beforeConvertType = columnType.substring(0, start);
                        String afterConvertType = getTrString(beforeConvertType);
                        if (beforeConvertType.toLowerCase().contains("double") && afterConvertType.toLowerCase().contains("number")){
                            
                          //拿出字段类型
//                            column_typeString = afterConvertType.toUpperCase();
                            column_typeString = afterConvertType.toLowerCase();
                        }
                        else if (afterConvertType.contains("(")) {
                            
//                            column_typeString = afterConvertType.toUpperCase();
                            column_typeString = afterConvertType.toLowerCase();
                        } else if (size > 2000 && afterConvertType.toLowerCase().contains("nvarchar2")) {
                            typeSize = "(2000)";
                        } else if (size > 4000 || size < 0) {
                            typeSize = "(4000)";
                        } else if (size > 38 && afterConvertType.toLowerCase().contains("number")) {
                            typeSize = "(38)";
                        } else {
                            typeSize = columnType.substring(start, end + 1);
                        }
                        
//                        column_typeString = afterConvertType.toUpperCase()+typeSize;
                        column_typeString = afterConvertType.toLowerCase()+typeSize;
                    }
                } else {
                     
//                     column_typeString = getTrString(columnType).toUpperCase();
                     column_typeString = getTrString(columnType).toLowerCase();
                }
                if(sourceField.getColumn_id() == 1){
                	dataList.add(new String[] { "数据仓库", "EDW", "ODS", afterTableName, table_cn_nameString, "1", "data_date",
                    		"", "date", "是", "否"  });
                }
                dataList.add(new String[] { "数据仓库", "EDW", "ODS", afterTableName, table_cn_nameString, Integer.toString(column_id), column_nameString,
                		column_cn_nameString, column_typeString, is_pkString, not_nullString });
            }
            sqlBuffer.append("etl_time date not null \n").append(")").append("compress nologging \n").append(";");
            dataList.add(new String[] { "数据仓库", "EDW", "ODS", afterTableName, table_cn_nameString, Integer.toString(column_id+1), "etl_time",
            		"", "date", "是", "否"  });
        }
        return dataList;

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
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edwassisdb", "mysql");
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
                if("Y".equals(is_pkString) || "y".equals(is_pkString)){
                	is_pkString = "是";
                }else if("n".equals(is_pkString) || "N".equals(is_pkString)){
                	is_pkString = "";
                }
                String not_nullString = resultSet.getString("not_null");
                String default_valueString = resultSet.getString("default_value");
                String is_dkString = resultSet.getString("is_dk");
                String break_flagString = resultSet.getString("break_flag");
                String table_cn_nameString = resultSet.getString("table_cn_name");
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
                //加入表中文名
                sourceField.setTable_cn_nameString(table_cn_nameString);
                
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
