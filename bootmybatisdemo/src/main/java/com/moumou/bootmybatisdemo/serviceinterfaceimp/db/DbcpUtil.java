package com.moumou.bootmybatisdemo.serviceinterfaceimp.db;

import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class DbcpUtil {
    // 声明DBCP
//    static BasicDataSource bds = new BasicDataSource();
    public static HashMap<String, SourceSystem> systemHashMaps = new HashMap<>();
    public static HashMap<String, BasicDataSource> systemBasicDataSourceMaps = new HashMap<>();

//    static {
//        // 一步一步设置配置，根据需求自主设置,只需set对应的属性就可以，配置基本的核心属性
//        bds.setInitialSize(5);
//        bds.setMaxTotal(50);
//        bds.setMaxIdle(20);
//        bds.setMinIdle(5);
//        bds.setMaxWaitMillis(10000);
//        bds.setMaxOpenPreparedStatements(50);
//        bds.setPoolPreparedStatements(true);
//    }

    static {
        String sqlString = "SELECT * from src_system";
        JdbcConnection jdbcConn = new JdbcConnection("edw", "edw123456", "192.10.30.15", "3306", "edw_dev", "mysql");
        Connection connection = jdbcConn.getDbConnection();
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlString);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String sys = resultSet.getString("sys");
                String sysNum = resultSet.getString("sys_num");
                String dbType = resultSet.getString("db_type");
                String dbVersion = resultSet.getString("db_version");
                String dbSid = resultSet.getString("db_sid");
                String dbSchema = resultSet.getString("db_schema");
                String dbCharset = resultSet.getString("db_charset");
                String dbIp = resultSet.getString("db_ip");
                String dbPort = resultSet.getString("db_port");
                String userName = resultSet.getString("username");
                String passWord = resultSet.getString("password");
                String encrPassWord = resultSet.getString("encrpassword");
                String reMark = resultSet.getString("remark");
                SourceSystem sourceSystem = new SourceSystem(sys, sysNum, dbType, dbVersion, dbSid, dbSchema,
                        dbCharset, dbIp, dbPort, userName, passWord, encrPassWord, reMark);
                String key = sys + "," + dbSid + "," + dbSchema;
                systemHashMaps.put(key, sourceSystem);
            }
            //srcdbToTgtdb.createTableScript("mysql","KFM", "dbo");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcConn.closeDbConnection();
        }
    }
//    public static HashSet<Connection> init() {
//        HashSet<Connection> connectionHashSet = new HashSet<>();
//        Iterator iter = DbcpUtil.systemHashMaps.entrySet().iterator();
//        while (iter.hasNext()) {
//            Map.Entry entry = (Map.Entry) iter.next();
//            String key = (String) entry.getKey();
//            SourceSystem val = (SourceSystem) entry.getValue();
//            try {
//                Connection conn = getBasicDataSource(val.getSys(), val.getDbSid(), val.getDbSchema()).getConnection();
//                connectionHashSet.add(conn);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//        return connectionHashSet;
//    }


//    public static Connection getConnect(String sys,String dbSid ,String dbSchema){
//
//    }


    /**
     *
     * @param sys
     * @param dbSid
     * @param dbSchema
     * @return  Connection
     */
    public static Connection getConnection(String sys, String dbSid, String dbSchema) {
        Connection conn = null;
        BasicDataSource bds = getBasicDataSource(sys,dbSid,dbSchema);
        try {
        	// add by wangshunye：Oracle JDBC接口开启注释获得
        	bds.addConnectionProperty("remarksReporting", "true");
        	// add by wangshunye：MySQL JDBC接口开启注释获得
        	bds.addConnectionProperty("useInformationSchema", "true");
            conn = bds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }



    private static BasicDataSource getBasicDataSource(String sys, String dbSid, String dbSchema){
        String _key = sys + "," + dbSid + "," + dbSchema;
        if(systemBasicDataSourceMaps.containsKey(_key)){
            return systemBasicDataSourceMaps.get(_key);
        }
        BasicDataSource bds  = new BasicDataSource();
        SourceSystem sourceSystem = systemHashMaps.get(_key);
        String driverClassName = "";
        String url = "";
        String dbType = sourceSystem.getDbType().toLowerCase();
        String userName = sourceSystem.getUserName();
        String passWord = sourceSystem.getPassWord();

        String dbIp = sourceSystem.getDbIp();
        String dbPort = sourceSystem.getDbPort();
        if ("oracle".equals(dbType)) {
            /*
             *  driverClassName=oracle.jdbc.driver.OracleDriver
             *  url=jdbc:oracle:thin:@//192.10.40.115:1521/otcdb1
             *  username=otcts
             *  password=otcts
             */
            driverClassName = "oracle.jdbc.driver.OracleDriver";
            url = "jdbc:oracle:thin:@//" + dbIp + ":" + dbPort + "/" + dbSid;
            bds.setValidationQuery("select 1 from dual");
        } else if ("sqlserver".equals(dbType) || "mssql".equals(dbType)) {
            /*
             *  driverClassName=com.microsoft.sqlserver.jdbc.SQLServerDriver
             *  url=jdbc:sqlserver://192.10.30.105:2433;DatabaseName=opt_stds
             *  username=sa
             *  password=Wkzq1234567890
             */
            driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            url = "jdbc:sqlserver://" + dbIp + ":" + dbPort + ";DatabaseName=" + dbSid;
            bds.setValidationQuery("select 1 from sysobjects");
        } else if ("mysql".equals(dbType)) {
            /*
             *  driverClassName=com.mysql.jdbc.Driver
             *  url=jdbc:mysql://192.10.30.15:3306/edw_dev?useUnicode=true&characterEncoding=UTF8
             *  username=edw
             *  password=edw123456
             */
            driverClassName = "com.mysql.jdbc.Driver";
            url = "jdbc:mysql://" + dbIp + ":" + dbPort + "/" + dbSid +
                    "?useUnicode=true&characterEncoding=UTF8";
            bds.setValidationQuery("select 1");
        } else {
            System.out.println("ERRO:数据库类型有误！");
        }
        bds.setDriverClassName(driverClassName);
        bds.setUrl(url);
        bds.setUsername(userName);
        if (passWord == null || passWord.equals("")){

        }else {
            bds.setPassword(passWord);
        }


        bds.setInitialSize(5);
        bds.setMaxTotal(50);
        bds.setMaxIdle(20);
        bds.setMinIdle(5);
        bds.setMaxWaitMillis(10000);
        bds.setMaxOpenPreparedStatements(50);
        bds.setPoolPreparedStatements(true);

        systemBasicDataSourceMaps.put(_key, bds);
        return  bds;
    }

}
