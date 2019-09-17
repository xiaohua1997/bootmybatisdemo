package com.moumou.bootmybatisdemo.dataAlignment.db;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.collections4.map.HashedMap;

public class Test11 {
	public static void main(String[] args) {
        test();
    }
    @SuppressWarnings("resource")
	public static void test() {
        Connection conn = null;
        Statement ps = null;
        boolean rs1;
        StringBuffer sb=new StringBuffer();
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入表名：");
        String tablename = sc.next();
        Scanner sc1 = new Scanner(System.in);
	    System.out.println("请输入系统名：");
	    String tablesys = sc1.next();
	    
        String sys = null;
        String dbsid = null;
        String dbschema = null;
        String dbtype = null;
        String dbip = null;
        String dbport = null;
        String username = null;
        String password = null;
        String sysdbsid = null;
        String sysdbschema = null;
        
        ArrayList<String> list = new ArrayList<String>();
        String tablist = null;
        int columnid = 0;        
        
       try {
    	   //连接mysql://192.10.30.15库备份需要备份的表字段信息
    	   Class.forName("com.mysql.jdbc.Driver").newInstance();
    	   conn = (Connection) DriverManager.getConnection("jdbc:mysql://192.10.30.15:3306/edwassisdb?user=edw&password=edw123456&useUnicode=true&characterEncoding=UTF8");
    	   //根据控制台输入的系统名查询出该系统的连接信息
    	   String sqlsrcsystem = "select db_type,db_ip,db_port,username,password,db_sid,db_schema from src_system where sys = '"+tablesys+"'";
    	   PreparedStatement prepareStatement2 = conn.prepareStatement(sqlsrcsystem); 
    	   ResultSet executeQuery2 = prepareStatement2.executeQuery();
    	   while(executeQuery2.next()) {
    		   dbtype = executeQuery2.getString("db_type") ;
    		   dbip = executeQuery2.getString("db_ip") ;
    		   dbport = executeQuery2.getString("db_port") ;
    		   username = executeQuery2.getString("username") ;
    		   password = executeQuery2.getString("password") ;
    		   sysdbsid = executeQuery2.getString("db_sid") ;
    		   sysdbschema = executeQuery2.getString("db_schema") ;
    	   }
    	   //System.out.println("dbtype:"+dbtype+" dbip:"+dbip+" dbport:"+dbport+" username:"+username+" password:"+password+" sysdbsid:"+sysdbsid);
    	   //备份数据库
    	   String sql4 = "delete from src_column_log where table_name = '"+tablename+"'";
    	   ps = conn.createStatement(); 
    	   rs1 = ps.execute(sql4);
    	   System.out.println("src_column_log初始数据删除成功");
    	   
    	   String sql5 = "delete from src_table_log where table_name = '"+tablename+"'";
    	   ps = conn.createStatement(); 
    	   rs1 = ps.execute(sql5);
    	   System.out.println("src_table_log初始数据删除成功");
    	   
    	   String sql1 = "insert into src_column_log select * from src_column where table_name = "+"'"+tablename+"'"; 
    	   ps = conn.createStatement(); 
    	   rs1 = ps.execute(sql1);
    	   System.out.println("src_column备份成功");
    	   
    	   String sql3 = "insert into src_table_log select * from src_table where table_name = "+"'"+tablename+"'"; 
    	   ps = conn.createStatement(); 
    	   rs1 = ps.execute(sql3);
    	   System.out.println("src_table备份成功");
    	   
    	   String sql81 = "select table_name from src_table where table_name = '"+tablename+"'";
    	   PreparedStatement prepareStatement1 = conn.prepareStatement(sql81); 
    	   ResultSet executeQuery1 = prepareStatement1.executeQuery();
    	   while(executeQuery1.next()) {
    		   tablist = executeQuery1.getString("table_name") ;
    	   }
    	   //System.out.println(tablist);
    	   if("mssql".equals(dbtype)) {
    		   dbtype ="sqlserver";
    	   }else{
    		   dbtype = dbtype;
    	   }
    	   //System.out.println(dbtype);
    	   String url = "";
    	   PreparedStatement ps1 = null;
    	   ResultSet rs = null;
    	   Connection conn1 = null;
    	   String driver = "";
	       if("oracle".equals(dbtype)){
				driver = "oracle.jdbc.driver.OracleDriver";
				url = "jdbc:oracle:thin:@//"+dbip+":"+dbport+"/"+sysdbsid;
			}else if ("sqlserver".equals(dbtype)||"mssql".equals(dbtype)){
				driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
				url = "jdbc:"+dbtype+"://"+dbip+":"+dbport+";User="+username+";Password="+password+";DatabaseName="+sysdbsid;
			}else if ("mysql".equals(dbtype)){
				driver="com.mysql.jdbc.Driver";
				url = "jdbc:mysql://" + dbip + ":" + dbport + "/" + sysdbsid + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF8";
			}else {
				System.out.println("ERRO:数据库类型有误！");
			}
	       
    	   if(tablist != null) {
    		   String sql8 = "select sys,db_sid,db_schema,column_name,column_id from src_column where table_name = '"+tablename+"'";
    		   PreparedStatement prepareStatement = conn.prepareStatement(sql8); 
    		   ResultSet executeQuery = prepareStatement.executeQuery();
    		   while(executeQuery.next()){
    			   sys = executeQuery.getString("sys");
    			   dbsid = executeQuery.getString("db_sid");
    			   dbschema = executeQuery.getString("db_schema");
    			   columnid = Integer.valueOf(executeQuery.getString("column_id"));
    			   list.add(executeQuery.getString("column_name"));
    		   } 
    	   
    	   //连接sqlserver源库将源库查询出来的字段信息更新到mysql里面
    	   Class.forName(driver);
	       conn1 = (Connection) DriverManager.getConnection(url);
	       String sql = "select * from "+tablename;
	       ps1 = (PreparedStatement) conn1.prepareStatement(sql);
	       rs = ps1.executeQuery();
	       //获取主键
	       List list3 = new ArrayList();
	       String zj = null;
	        DatabaseMetaData dmd = conn1.getMetaData();
		    ResultSet pk = dmd.getPrimaryKeys(null,null,tablename);
		    while(pk.next()) {
		    	 zj = pk.getString(4);
		    	 list3.add(zj);
		    	 
		    	//System.out.println(zj);
		    }
		    
            //获取列名及类型
            int colunmCount = rs.getMetaData().getColumnCount();
            String[] colNameArr = new String[colunmCount];
            String[] colTypeArr = new String[colunmCount];
            //String[] colisnullArr = new String[colunmCount];
            String columncnname = null;
            String notnull = null;
            String defaultvalue = null;
            int typelong = 0;
            String tablecnname = null;
            String tabletype = null;
            
            DbInfoUtil df = new DbInfoUtil();
            List<Map<String,String>> tableInfo = df.getTableInfo(driver, url, username, password, tablename);
            
            //把可以从源数据查出来的表信息封装到map里
            Map map = new HashedMap<String, ArrayList>();
			for (int i = 0; i < colunmCount; i++) {
				ArrayList list1 = new ArrayList();
            	String columnname = rs.getMetaData().getColumnName(i + 1);
            	colNameArr[i] = columnname;
                list1.add(columnname);
                String columntype = rs.getMetaData().getColumnTypeName(i + 1);
                list1.add(columntype);
                typelong = rs.getMetaData().getPrecision(i + 1);
                list1.add(typelong);
                
            	for(Map <String,String>map12: tableInfo) {
            		if(columnname.equals(map12.get("colName"))) {
            			tablecnname = map12.get("zs");
            			tabletype = map12.get("tabletype");
            			columncnname = map12.get("remarks");
            			defaultvalue = map12.get("columndef");
            			notnull = map12.get("nullable");
            			list1.add(columncnname);
            			list1.add(defaultvalue);
            			list1.add(notnull);
            			
            		}
            	}
                    
            	map.put(columnname, list1); 
            }
            
			//已有表新增字段
			for(String cn:colNameArr) {
				if(!(list).contains(cn)) {
				ArrayList list2 = (ArrayList) map.get(cn);
				String columntype = null;
				if(list2!=null && list2.size()>2) {
				 columntype = ((ArrayList<String>) list2).get(1);
				}
				columnid++ ;
				if((list3).contains(cn)) {
				zj="Y";
				}else {
			    zj="n";
				}
				String sql7 = "insert into src_column (sys,db_sid,db_schema,table_name,column_id,column_name,column_type,column_cn_name,is_pk,not_null,default_value) "
						+ "values ('"+sys+"','"+sysdbsid+"','"+sysdbschema+"','"+tablename+"','"+columnid+"','"+cn+"','"+columntype+"("+typelong+")'"+",'"+columncnname+"','"+zj+"','"+notnull+"','"+defaultvalue+"')";
                ps = conn.createStatement();                                                                              
                rs1 = ps.execute(sql7);
                
					/*
					 * String sql71 =
					 * "insert into src_table_log (sys,db_sid,table_schema,table_name,table_cn_name,table_type) "
					 * +
					 * "values ('"+sys+"','"+dbsid+"','"+dbschema+"','"+tablename+"','"+tablecnname+
					 * "','"+tabletype+"')"; ps = conn.createStatement();
					 * 
					 * System.out.println(sql71); rs1 = ps.execute(sql71);
					 */
			   }
		   }
			//已有表减少字段
			for(String colmunname:list) {
				
				if(!Arrays.asList(colNameArr).contains(colmunname)) {
					String sql9 = "delete from src_column where colmun_name = "+colmunname;
					ps = conn.createStatement();                                                                              
					rs1 = ps.execute(sql9);
				}
			}
            sb.append("}");
            
    	   }else if(tablist == null) {
    		   Class.forName(driver);
    		   conn1 = (Connection) DriverManager.getConnection(url,username,password);
    	       String sql = "select * from "+tablename;
    	       ps1 = (PreparedStatement) conn1.prepareStatement(sql);
    	       rs = ps1.executeQuery();
    	       //获取主键
    	       List list3 = new ArrayList();
    	       String zj = null;
    	        DatabaseMetaData dmd = conn1.getMetaData();
    		    ResultSet pk = dmd.getPrimaryKeys(null,null,tablename);
    		    while(pk.next()) {
    		    	 zj = pk.getString(4);
    		    	 list3.add(zj);
    		    }
    	       
                //获取列名及类型
                int colunmCount = rs.getMetaData().getColumnCount();
                String[] colNameArr = new String[colunmCount];
                String[] colTypeArr = new String[colunmCount];
                int[] colnotnull1 = new int[colunmCount];
                int[] coltypelong1 = new int[colunmCount];
                //String[] colisnullArr = new String[colunmCount];
                String columncnname = null;
                String defaultvalue = null;
                String tablecnname = null;
                String tabletype = null;
                String tablecat = null;
                String tableschem = null;
                String ifmark = "F";
                String etldb = "Y";
                String inccdt = null;
                //String isdk = null;
                //String breakflag = null;
                
                DbInfoUtil df = new DbInfoUtil();
                List<Map<String,String>> tableInfo = df.getTableInfo(driver, url, username, password, tablename);
                
                //把可以从源数据查出来的表信息封装到map里
                Map map = new HashedMap<String, ArrayList>();
    			for (int i = 0; i < colunmCount; i++) {
    				ArrayList list1 = new ArrayList();
                	String columnname = rs.getMetaData().getColumnName(i + 1);
                	colNameArr[i] = columnname;
                    list1.add(columnname);
                    
                    String columntype = rs.getMetaData().getColumnTypeName(i + 1);
                    colTypeArr[i] = columntype;
                    list1.add(columntype);
                    
                    int typelong = rs.getMetaData().getPrecision(i + 1);
                    coltypelong1[i] = typelong;
                    list1.add(typelong);
                    
                    int notnull = rs.getMetaData().isNullable(i + 1);
                    colnotnull1[i] = notnull;
                    list1.add(notnull);
                    //System.out.println(list1);
                	for(Map <String,String>map12: tableInfo) {
                			tablecat = map12.get("tablecat");
                			tableschem = map12.get("tableschem");
                			tablecnname = map12.get("zs");
                			tabletype = map12.get("tabletype");
                			columncnname = map12.get("remarks");
                			defaultvalue = map12.get("columndef");
                			//notnull = map12.get("nullable").toString();
                			list1.add(columncnname);
                			list1.add(defaultvalue);
                			//list1.add(notnull);
                }
                	//System.out.println(list1);
                	map.put(columnname, list1); 
                }
    			//新表新增字段
    			for(String cn:colNameArr) {
    				ArrayList list2 = (ArrayList) map.get(cn);
    				String columntype = null;
    				int coltypelong = 0;
    				int colnotnull = 0;
    				String notnull = null;
    				String breakflag = null;
    				if(list2!=null && list2.size()>2) {
    				 columntype = ((ArrayList<String>) list2).get(1);
    				 if(columntype=="LONGTEXT"||columntype=="Ntext"||columntype=="nvarchar(2000)"||columntype=="nvarchar(4000)"||columntype=="varchar(8000)") {
    					 breakflag = "Y";
    				 }else {
    					 breakflag = "N";
    				 }
    				 coltypelong = ((ArrayList<Integer>) list2).get(2);
    				 colnotnull = ((ArrayList<Integer>) list2).get(3);
    				 if(colnotnull==0) {
    					 notnull = "Y";
    				 }else if(colnotnull==1) {
    					 notnull = "N";
    				 }
    				}
    				columnid++ ;
    				
    				if((list3).contains(cn)) {
    				zj="Y";
    				}else {
    			    zj="n";
    				}
    				
    				String sql7 = "insert into src_column (sys,db_sid,db_schema,table_name,column_id,column_name,column_type,column_cn_name,is_pk,not_null,default_value,is_dk,break_flag) "
    						+ "values ('"+tablesys+"','"+sysdbsid+"','"+sysdbschema+"','"+tablename+"','"+columnid+"','"+cn+"','"+columntype+"("+coltypelong+")'"+",'"+columncnname+"','"+zj+"','"+notnull+"',"+null+","+null+",'"+breakflag+"')";
                    ps = conn.createStatement();                                                                              
                    rs1 = ps.execute(sql7);
                    
    		   }
				/*
				 * if(etldb=="Y" && ifmark=="F") { inccdt = null; }
				 */
    			String sql71 = "insert into src_table (sys,db_sid,table_schema,table_name,table_cn_name,if_mark,is_put_to_etldb) "
    					+ "values ('"+tablesys+"','"+sysdbsid+"','"+sysdbschema+"','"+tablename+"','"+tablecnname+"','"+ifmark+"','"+etldb+"')"; 
    			ps = conn.createStatement();
    			rs1 = ps.execute(sql71);
    			
   	    }
   	   
    	   System.out.println("src_clomun和src_table字段更新成功");
            
        } catch (Exception e) {
            e.printStackTrace(System.out);
        } finally {
            try {
				/*
				 * if (null != rs) { rs.close(); }
				 */
                if (null != ps) {
                    ps.close();
                }
                if (null != conn) {
                    conn.close();
                }
            } catch (Exception e2) {
            }
        }
    }
    
}
