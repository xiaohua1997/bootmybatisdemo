package com.moumou.bootmybatisdemo.dataAlignment.db;

import java.sql.*;
public class HiveConnTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.10.40.161:10000/datawarehouse01";
    private static String user = "dw_test01";
    private static String passwd = "dwtest88@wkzq";

    public static void getConn() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;

        Class.forName(driverName);

        conn = DriverManager.getConnection(url, user, passwd);
        System.out.println(conn);
        conn.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        res.close();
        stmt.close();
        conn.close();
    }


    public static void main(String[] args) {
        try {
            HiveConnTest.getConn();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}


