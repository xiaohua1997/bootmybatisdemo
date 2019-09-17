package com.moumou.bootmybatisdemo.dataAlignment.db;


import com.moumou.bootmybatisdemo.dataAlignment.dao.SourceFieldDao;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceField;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MssqlTest {
    private static DataSource myDataSource = null;

//    private JdbcUtils() {
//    }

    static {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Properties prop = new Properties();
            InputStream is = MssqlTest.class.getClassLoader().getResourceAsStream("dbcp.properties");
            prop.load(is);
            myDataSource = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static DataSource getDataSource() {
        return myDataSource;
    }

    public static Connection getConnection() throws SQLException {
        // return DriverManager.getConnection(url, user, password);
        return myDataSource.getConnection();

    }

    public static void free(ResultSet rs, Statement st, Connection conn) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (conn != null)
                    try {
                        conn.close();
                        // myDataSource.free(conn);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        }
    }

    public static void main(String[] args) {
        try {
            SourceFieldDao sourceFieldDao = new SourceFieldDao();
            sourceFieldDao.getFieldInfo("HSTA");
            List<SourceField> sourceFieldList = new ArrayList<>();
            DatabaseMetaData dmd = MssqlTest.getConnection().getMetaData();
            for (SourceField sourceField : sourceFieldList) {
                ResultSet rs =dmd.getColumns(null, null, sourceField.getTable_nameString(), null);
                while(rs.next()){
                    String tableCat = rs.getString("TABLE_CAT");  //表类别（可能为空）
                    String tableSchemaName = rs.getString("TABLE_SCHEM");  //表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
                    String tableName_ = rs.getString("TABLE_NAME");  //表名
                    String columnName = rs.getString("COLUMN_NAME");  //列名
                    int dataType = rs.getInt("DATA_TYPE");     //对应的java.sql.Types的SQL类型(列类型ID)
                    String dataTypeName = rs.getString("TYPE_NAME");  //java.sql.Types类型名称(列类型名称)
                    int columnSize = rs.getInt("COLUMN_SIZE");  //列大小
                    int decimalDigits = rs.getInt("DECIMAL_DIGITS");  //小数位数
                    int numPrecRadix = rs.getInt("NUM_PREC_RADIX");  //基数（通常是10或2） --未知
                    /**
                     *  0 (columnNoNulls) - 该列不允许为空
                     *  1 (columnNullable) - 该列允许为空
                     *  2 (columnNullableUnknown) - 不确定该列是否为空
                     */
                    int nullAble = rs.getInt("NULLABLE");  //是否允许为null
                    String remarks = rs.getString("REMARKS");  //列描述
                    String columnDef = rs.getString("COLUMN_DEF");  //默认值
                    int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");    // 对于 char 类型，该长度是列中的最大字节数
                    int ordinalPosition = rs.getInt("ORDINAL_POSITION");   //表中列的索引（从1开始）
                    /**
                     * ISO规则用来确定某一列的是否可为空(等同于NULLABLE的值:[ 0:'YES'; 1:'NO'; 2:''; ])
                     * YES -- 该列可以有空值;
                     * NO -- 该列不能为空;
                     * 空字符串--- 不知道该列是否可为空
                     */
                    String isNullAble = rs.getString("IS_NULLABLE");

                    /**
                     * 指示此列是否是自动递增
                     * YES -- 该列是自动递增的
                     * NO -- 该列不是自动递增
                     * 空字串--- 不能确定该列是否自动递增
                     */
                    //String isAutoincrement = rs.getString("IS_AUTOINCREMENT");   //该参数测试报错


                    System.out.println(tableCat + " - " + tableSchemaName + " - " + tableName_ + " - " + columnName +
                            " - " + dataType + " - " + dataTypeName + " - " + columnSize + " - " + decimalDigits + " - "
                            + numPrecRadix + " - " + nullAble + " - " + remarks + " - " + columnDef + " - " + charOctetLength
                            + " - " + ordinalPosition + " - " + isNullAble );

                }
            }



        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
