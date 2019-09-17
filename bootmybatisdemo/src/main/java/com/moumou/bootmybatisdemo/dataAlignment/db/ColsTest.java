package com.moumou.bootmybatisdemo.dataAlignment.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;

public class ColsTest {

    public static void main(String[] args) {
        ColsTest hiveCreate = new ColsTest();
        try {
            hiveCreate.traverseFolder2("C:\\Users\\Administrator\\Desktop\\程序\\20190905\\EtlAssistPro\\EtlAssistPro\\bin\\020301 Oracle_当日_建表");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void traverseFolder2(String path) throws SQLException, ClassNotFoundException {
        String driverClassName = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@//192.10.40.169:1521/edwdb";
        String user = "ods";
        String passwd = "ods";
        File file = new File(path);Class.forName(driverClassName);
        Connection conn = DriverManager.getConnection(url,user,passwd);
        Statement stmt = conn.createStatement();
        if (file.exists()) {

            File[] files = file.listFiles();

            if (null == files || files.length == 0) {

                System.out.println("文件夹是空的!");

                return;

            } else {

                for (File file2 : files) {

                    if (file2.isDirectory()) {

                        System.out.println("文件夹:" + file2.getAbsolutePath());

                        traverseFolder2(file2.getAbsolutePath());

                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());

                        String sql = txt2String(file2);
                        System.out.println(sql.trim());
                        int f = stmt.executeUpdate(sql.trim());

                        if (f != 0) {

                            System.out.println("文件:" + file2.getAbsolutePath() + "error");
                        }
                    }

                }

            }

        } else {

            System.out.println("文件不存在!");

        }
        stmt.close();
        conn.close();
    }


    public static String txt2String(File file) {
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                result.append(s);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}
