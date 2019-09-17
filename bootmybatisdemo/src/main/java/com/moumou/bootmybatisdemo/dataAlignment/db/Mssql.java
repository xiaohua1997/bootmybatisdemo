package com.moumou.bootmybatisdemo.dataAlignment.db;

import com.moumou.bootmybatisdemo.dataAlignment.dao.SourceFieldDao;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Mssql {
    public static void main(String[] args) {
        String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String url = "jdbc:sqlserver://192.10.30.105:2433;DatabaseName=opt_stds";
        String userName = "sa";
        String pwd = "Wkzq1234567890";
        Statement sm = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url,userName,pwd);

                try {
                    SourceFieldDao sourceFieldDao = new SourceFieldDao();
                    List<SourceField> sourceFieldList = sourceFieldDao.getFieldInfo("GPQQ");

                    conn = DbcpUtil.getConnection("GPQQ","opt_stds","dbo");

                    DatabaseMetaData dmd = conn.getMetaData();
                    for (SourceField sourceField : sourceFieldList) {
                        rs =dmd.getColumns(null, null, sourceField.getTable_nameString(), null);
                        System.out.println(rs.getString(1));
//                        while(true){
//                            System.out.println("111");
////                            String tableCat = rs.getString("TABLE_CAT");  //表类别（可能为空）
////                            String tableSchemaName = rs.getString("TABLE_SCHEM");  //表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
////                            String tableName_ = rs.getString("TABLE_NAME");  //表名
//                            String columnName = rs.getString("COLUMN_NAME");  //列名
//                            int dataType = rs.getInt("DATA_TYPE");     //对应的java.sql.Types的SQL类型(列类型ID)
//                            String dataTypeName = rs.getString("TYPE_NAME");  //java.sql.Types类型名称(列类型名称)
//                            int columnSize = rs.getInt("COLUMN_SIZE");  //列大小
//                            int decimalDigits = rs.getInt("DECIMAL_DIGITS");  //小数位数
//                            int numPrecRadix = rs.getInt("NUM_PREC_RADIX");  //基数（通常是10或2） --未知
//                            /**
//                             *  0 (columnNoNulls) - 该列不允许为空
//                             *  1 (columnNullable) - 该列允许为空
//                             *  2 (columnNullableUnknown) - 不确定该列是否为空
//                             */
//                            int nullAble = rs.getInt("NULLABLE");  //是否允许为null
//                            String remarks = rs.getString("REMARKS");  //列描述
//                            String columnDef = rs.getString("COLUMN_DEF");  //默认值
//                            int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");    // 对于 char 类型，该长度是列中的最大字节数
//                            int ordinalPosition = rs.getInt("ORDINAL_POSITION");   //表中列的索引（从1开始）
//                            /**
//                             * ISO规则用来确定某一列的是否可为空(等同于NULLABLE的值:[ 0:'YES'; 1:'NO'; 2:''; ])
//                             * YES -- 该列可以有空值;
//                             * NO -- 该列不能为空;
//                             * 空字符串--- 不知道该列是否可为空
//                             */
//                            String isNullAble = rs.getString("IS_NULLABLE");
//
//                            /**
//                             * 指示此列是否是自动递增
//                             * YES -- 该列是自动递增的
//                             * NO -- 该列不是自动递增
//                             * 空字串--- 不能确定该列是否自动递增
//                             */
//                            //String isAutoincrement = rs.getString("IS_AUTOINCREMENT");   //该参数测试报错
//
//
//                            System.out.println(  columnName +
//                                    " - " + dataType + " - " + dataTypeName + " - " + columnSize + " - " + decimalDigits + " - "
//                                    + numPrecRadix + " - " + nullAble + " - " + remarks + " - " + columnDef + " - " + charOctetLength
//                                    + " - " + ordinalPosition + " - " + isNullAble );
//
//                        }
                    }



                } catch (SQLException e) {
                    e.printStackTrace();
                }finally {
                    conn.close();
                }

            System.out.println("Connection Successful");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
