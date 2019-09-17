package com.moumou.bootmybatisdemo.dataAlignment.db;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

public class DbcpTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			java.sql.Connection conn = bs.getConnection();
			System.out.println("======================:"+conn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    // 声明一个DataSource源,也就是驱动
    static BasicDataSource bs = null; 
    // properties集合，读取properties文件
    static Properties properties = new Properties(); 
    
    static {
        
        // 用类加载器加载文件获得流
        InputStream rs = DbcpTest.class.getClassLoader().getResourceAsStream("dbcp.properties");
        try {
            // 加载文件配置内容到集合中
            properties.load(rs);
            // 通过basic工厂获得DataSource源，也就是驱动，相当于注册

            bs = BasicDataSourceFactory.createDataSource(properties);
            System.out.println("11");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}
