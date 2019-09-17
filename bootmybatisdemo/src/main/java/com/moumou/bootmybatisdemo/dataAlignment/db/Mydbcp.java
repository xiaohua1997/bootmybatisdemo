package com.moumou.bootmybatisdemo.dataAlignment.db;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class Mydbcp {

    // 声明DBCP
    static BasicDataSource bds = new BasicDataSource();

    static {
        // 一步一步设置配置，根据需求自主设置,只需set对应的属性就可以
        bds.setDriverClassName("com.mysql.jdbc.Driver");
        bds.setUrl("jdbc:mysql://192.10.30.15/edwassisdb");
        bds.setUsername("edw");
        bds.setPassword("edw123456");

        bds.setInitialSize(5);
        bds.setMaxTotal(20);
        bds.setMaxIdle(20);
        bds.setMinIdle(5);
        bds.setMaxWaitMillis(10000);
    }

    public static void main(String[] args) {
        try {
            // for循环测试连接是否成功
            for(int i = 0; i < 50; i++) {
                Connection conn = bds.getConnection();
                System.out.println(conn.hashCode()+ "...." + i);
                conn.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
