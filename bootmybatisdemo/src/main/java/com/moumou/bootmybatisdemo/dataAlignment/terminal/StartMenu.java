package com.moumou.bootmybatisdemo.dataAlignment.terminal;

import java.awt.AWTException;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Scanner;

public class StartMenu {
	private static Scanner sc = new Scanner(System.in);

	public static void main(String[] args) throws AWTException {	
		do {
			showTopMenu();
			String str = sc.next();
			clearConsole();
			System.out.println(":" + str);
			
			switch (str) {
			case "ddl":
				System.out.println("生成DDL");
				enterDdlMenu();
				break;
			case "dml":
				System.out.println("生成DML");
				enterDmlMenu();
				break;
			case "job":
				System.out.println("生成调度作业");
				enterJobMenu();
				break;
			case "meta":
				System.out.println("元数据管理");
				enterMetaMenu();
				break;
			case "exit":
				System.exit(0);
				break;
			default:
				break;
			}
			
			try {
				Enter();
			} catch (IOException e) {
				e.printStackTrace();
			}
			clearConsole();
		} while (true);		
	}



	private static void enterMetaMenu() {
		// TODO Auto-generated method stub
		System.out.println("1、更新元数据");
		//System.out.println("2、删除元数据");
		//System.out.println("3、恢复历史版本");
		String operatorIndexStr = sc.next();
		
		MetadataManagement aMetadataManagement = new MetadataManagement();
		String sys;
		String sid;
		String schema;
		
		System.out.println("选择范围");
		if(operatorIndexStr.equals("1")) {
			System.out.println("1、所有系统");
			System.out.println("2、指定系统缩写");
			System.out.println("3、指定数据库模式");
			System.out.println("4、指定表");
			System.out.println("9、取消生成");
			String rangeIndexStr = sc.next();
			
			switch (rangeIndexStr) {
			case "1":
				try {
					aMetadataManagement.updateFromSourceDB();
				} catch (SQLException | NullPointerException e) {
					e.printStackTrace();
				}
				break;
			case "2":
				System.out.println("请输入系统缩写：");
				sys = sc.next();
				try {
					aMetadataManagement.updateFromSourceDB(sys);
				} catch (SQLException | NullPointerException e) {
					e.printStackTrace();
				}
				break;
			case "3":
				System.out.println("请输入系统缩写：");
				sys = sc.next();
				System.out.println("请输入SID：");
				sid = sc.next();
				System.out.println("请输入Schema：");
				schema = sc.next();
				try {
					aMetadataManagement.updateFromSourceDB(sys, sid, schema);
				} catch (SQLException | NullPointerException e) {
					e.printStackTrace();
				}
				break;
			case "4":
				System.out.println("请输入系统缩写：");
				sys = sc.next();
				System.out.println("请输入SID：");
				sid = sc.next();
				System.out.println("请输入Schema：");
				schema = sc.next();
				
				sc.nextLine();
				System.out.println("请输入表名：");
				while(sc.hasNextLine()) {
					String tableInfo = sc.nextLine();
					if (tableInfo.equals(":END")) {
						break;
					} else if (tableInfo.equals("")) {
						continue;
					}
					
					System.out.println(tableInfo);
					parseTableInfo(aMetadataManagement, sys, sid, schema, tableInfo);
					System.out.println("输入:END回车后退出");
				}
				break;
			default:
				break;
			}
		} else if(operatorIndexStr.equals("2")){
//			System.out.println("2、指定系统缩写");
//			System.out.println("3、指定数据库模式");
//			System.out.println("4、指定表");
//			System.out.println("9、取消生成");
//			String rangeIndexStr = sc.next();
		}

	}

	private static void parseTableInfo(MetadataManagement aMetadataManagement
			,String sys, String sid, String schema, String tableInfo) {
		String[] arrInfo = tableInfo.split("\\t");
		if(arrInfo.length >= 11) {
			String tableName = arrInfo[2].trim();
			String tableCnName = arrInfo[3].trim();
			String etl_flag = arrInfo[10].trim();
			String if_mark = "F";
			if("增量".equals(etl_flag)) {
				if_mark = "I";
			}
			
			//System.out.println(tableName + "," + tableCnName + "," + if_mark);
			
			// 调用接口
			try {
				aMetadataManagement.updateFromSourceDB(sys, sid, schema, tableName, tableCnName, if_mark);
			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
			}
		} else if (arrInfo.length == 1 && !arrInfo[0].trim().isEmpty()) {
			String tableName = arrInfo[0].trim();
			// 如果是新表，要求输入中文名
			try {
				if(aMetadataManagement.exists(sys, sid, schema, tableName)) {
					aMetadataManagement.updateFromSourceDB(sys, sid, schema, tableName);
				} else {
					System.out.println("新增表。请输入中文名:");
					String tableCnName = sc.next();
					System.out.println("新增表。请输入F/I:");
					String if_mark = sc.next().toUpperCase();
					
					aMetadataManagement.updateFromSourceDB(sys, sid, schema, tableName, tableCnName, if_mark);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("输入格式错误");
			return ;
		}
		

	}



	private static void enterJobMenu() {
		String scheduleIndexStr;
		String dbTypeIndexStr;
		String levelIndexStr;
		// job：生成调度作业
		System.out.println("选择目标调度系统类型");
		System.out.println("1、Azkaban");
		scheduleIndexStr = sc.next();
		
		switch (scheduleIndexStr) {
		case "1":
			System.out.println("选择目标数据库类型");
			System.out.println("1、Impala");
			System.out.println("2、Oracle");
			dbTypeIndexStr = sc.next();
			
			System.out.println("选择要生成的层次");
			System.out.println("0、全部");
			switch (dbTypeIndexStr) {
			case "1": 
				System.out.println("1、ODS");
				System.out.println("2、CHK");
				levelIndexStr = sc.next();
				enterJobFinalMenu(scheduleIndexStr, dbTypeIndexStr, levelIndexStr);
				break;
			case "2":
				System.out.println("1、ODS");
				System.out.println("2、ODS_M");
				System.out.println("3、CHK");
				levelIndexStr = sc.next();
				enterJobFinalMenu(scheduleIndexStr, dbTypeIndexStr, levelIndexStr);
				break;
			default:
				break;
			}
			
			break;
		default:
			break;
		}
	}

	private static void enterJobFinalMenu(String scheduleIndexStr, String dbTypeIndexStr, String levelIndexStr) {
		System.out.println("选择要生成的范围");
		System.out.println("1、所有系统");
		System.out.println("2、指定系统缩写");
		System.out.println("3、指定数据库模式");
		System.out.println("4、指定表");
		System.out.println("9、取消生成");
		String rangeIndexStr = sc.next();
		
		// 将序号转换为参数
		String level = "";
		String scheduleSystem = "";
		String dbtype = "";
		
		if(scheduleIndexStr.equals("1")) {
			scheduleSystem = "azkaban";
		}

		if(dbTypeIndexStr.equals("1")) {
			dbtype = "impala";
		} else if (dbTypeIndexStr.equals("2")) {
			dbtype = "oracle";
		}
		
		if(levelIndexStr.equals("0")) {
			level = "all";
		} else if (levelIndexStr.equals("1")) {
			level = "ods";
		} else if (levelIndexStr.equals("2") && dbtype.equals("impala")) {
			level = "chk";
		} else if (levelIndexStr.equals("2") && dbtype.equals("oracle")) {
			level = "ods_m";
		} else if (levelIndexStr.equals("3") && dbtype.equals("oracle")) {
			level = "chk";
		}
		
		
		
		ScheduleJobs aScheduleJobs = new ScheduleJobs();
		String sys;
		String sid;
		String schema;
		
		// 根据生成范围，调用不同的接口
		switch (rangeIndexStr) {
		case "1":
			try {
				aScheduleJobs.generate(level, scheduleSystem, dbtype);
			} catch (IOException | SQLException e) {
				e.printStackTrace();
			}
			break;
		case "2":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			try {
				aScheduleJobs.generate(level, scheduleSystem, dbtype, sys);
			} catch (IOException | SQLException e) {
				e.printStackTrace();
			} 
			break;
		case "3":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			System.out.println("请输入SID：");
			sid = sc.next();
			System.out.println("请输入Schema：");
			schema = sc.next();
			try {
				aScheduleJobs.generate(level, scheduleSystem, dbtype, sys, sid, schema);
			} catch (IOException | SQLException e) {
				e.printStackTrace();
			}
			break;
		case "4":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			System.out.println("请输入SID：");
			sid = sc.next();
			System.out.println("请输入Schema：");
			schema = sc.next();
			System.out.println("请输入表名：");
			String tableName = sc.next();
			try {
				aScheduleJobs.generate(level, scheduleSystem, dbtype, sys, sid, schema, tableName);
			} catch (IOException | SQLException e) {
				e.printStackTrace();
			}
			break;
		default:
			break;
		}
	}

	private static void enterDmlMenu() {
		String dbTypeIndexStr;
		String levelIndexStr;
		
		System.out.println("选择目标数据库类型");
		System.out.println("1、Impala");
		System.out.println("2、Oracle");
		dbTypeIndexStr = sc.next();
		
		System.out.println("选择要生成的层次");
		System.out.println("0、全部");
		switch (dbTypeIndexStr) {
		case "1":
			System.out.println("1、ODS");
			System.out.println("2、CHK");
			levelIndexStr = sc.next();
			enterDmlFinalMenu(dbTypeIndexStr, levelIndexStr);
			break;
		case "2":
			System.out.println("1、ODS");
			System.out.println("2、ODS_M");
			System.out.println("3、CHK");
			levelIndexStr = sc.next();
			enterDmlFinalMenu(dbTypeIndexStr, levelIndexStr);
			break;
		default:
			break;
		}
	}



	private static void enterDmlFinalMenu(String dbTypeIndexStr, String levelIndexStr) {
		// 选择使用的工具（如果上一部选择是0，分开询问）
		// 1、Impala-Executer | Oracle-Executer | DbCheck
		// TODO 待后续数据库表建立后补充
		String dataManipulationTool = "";
		
		System.out.println("选择要生成的范围");
		System.out.println("1、所有系统");
		System.out.println("2、指定系统缩写");
		System.out.println("3、指定数据库模式");
		System.out.println("4、指定表");
		System.out.println("9、取消生成");
		String rangeIndexStr = sc.next();
		
		// 将序号转换为参数
		String level = "";
		String dbtype = "";
		
		if(dbTypeIndexStr.equals("1")) {
			dbtype = "impala";
		} else if (dbTypeIndexStr.equals("2")) {
			dbtype = "oracle";
		}
		
		if(levelIndexStr.equals("0")) {
			level = "all";
		} else if (levelIndexStr.equals("1")) {
			level = "ods";
		} else if (levelIndexStr.equals("2") && dbtype.equals("impala")) {
			level = "chk";
		} else if (levelIndexStr.equals("2") && dbtype.equals("oracle")) {
			level = "ods_m";
		} else if (levelIndexStr.equals("3") && dbtype.equals("oracle")) {
			level = "chk";
		}
		
		DataManipulation aDataManipulation = new DataManipulation();
		String sys;
		String sid;
		String schema;
		
		// 根据生成范围，调用不同的接口
		switch (rangeIndexStr) {
		case "1":
			try {
				aDataManipulation.generate(level, dataManipulationTool, dbtype, "lower");
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			break;
		case "2":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			try {
				aDataManipulation.generate(level, dataManipulationTool, dbtype, sys, "lower");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "3":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			System.out.println("请输入SID：");
			sid = sc.next();
			System.out.println("请输入Schema：");
			schema = sc.next();
			try {
				aDataManipulation.generate(level, dataManipulationTool, dbtype, sys, sid, schema, "lower");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "4":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			System.out.println("请输入SID：");
			sid = sc.next();
			System.out.println("请输入Schema：");
			schema = sc.next();
			System.out.println("请输入表名：");
			String tableName = sc.next();
			try {
				aDataManipulation.generate(level, dataManipulationTool, dbtype, sys, sid, schema, tableName, "lower");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		default:
			break;
		}
		
	}



	private static void enterDdlMenu() {
		String dbTypeIndexStr;
		String levelIndexStr;
		
		System.out.println("选择目标数据库类型");
		System.out.println("1、Impala");
		System.out.println("2、Oracle");
		dbTypeIndexStr = sc.next();

		System.out.println("选择要生成的层次");
		System.out.println("0、全部");
		switch (dbTypeIndexStr) {
		case "1":
			System.out.println("1、ODS");
			levelIndexStr = sc.next();
			enterDdlFinalMenu(dbTypeIndexStr, levelIndexStr);
			break;
		case "2":
			System.out.println("1、ODS");
			System.out.println("2、ODS_M");
			levelIndexStr = sc.next();
			enterDdlFinalMenu(dbTypeIndexStr, levelIndexStr);
			break;
		default:
			break;
		}
	}



	private static void enterDdlFinalMenu(String dbTypeIndexStr, String levelIndexStr) {
		System.out.println("选择要生成的范围");
		System.out.println("1、所有系统");
		System.out.println("2、指定系统缩写");
		System.out.println("3、指定数据库模式");
		System.out.println("4、指定表");
		System.out.println("9、取消生成");
		String rangeIndexStr = sc.next();
		
		// 将序号转换为参数
		String level = "";
		String dbtype = "";
		
		if(dbTypeIndexStr.equals("1")) {
			dbtype = "impala";
		} else if (dbTypeIndexStr.equals("2")) {
			dbtype = "oracle";
		}
		
		if(levelIndexStr.equals("0")) {
			level = "all";
		} else if (levelIndexStr.equals("1")) {
			level = "ods";
		} else if (levelIndexStr.equals("2") && dbtype.equals("oracle")) {
			level = "ods_m";
		}
		
		String sys;
		String sid;
		String schema;
		DataDefinition aDataDefinition = new DataDefinition();
		
		switch (rangeIndexStr) {
		case "1":
			aDataDefinition.generate(level, dbtype, true, "lower");
			break;
		case "2":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			aDataDefinition.generate(level, dbtype, sys, true, "lower");
			break;
		case "3":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			System.out.println("请输入SID：");
			sid = sc.next();
			System.out.println("请输入Schema：");
			schema = sc.next();
			aDataDefinition.generate(level, dbtype, sys, sid, schema, true, "lower");
			break;
		case "4":
			System.out.println("请输入系统缩写：");
			sys = sc.next();
			System.out.println("请输入SID：");
			sid = sc.next();
			System.out.println("请输入Schema：");
			schema = sc.next();
			System.out.println("请输入表名：");
			String tableName = sc.next();
			aDataDefinition.generate(level, dbtype, sys, sid, schema, tableName, true, "lower");
			break;
		default:
			break;
		}
		

	}



	private static void showTopMenu() {
		System.out.println("ddl：生成DDL");
		System.out.println("dml：生成DM");
		System.out.println("job：生成调度作业");
		System.out.println("meta：元数据管理");
		System.out.println("exit：退出程序");
		System.out.println("----------------------------");
		System.out.print("请输入:");
	}
	
	public static void Enter() throws IOException  {//停顿
		System.out.println("按回车继续");
		new BufferedReader(new InputStreamReader(System.in)).readLine();
	}

	private static void clearConsole() throws AWTException
	{
	    try
	    {
	        String os = System.getProperty("os.name");

	        if (os.contains("Windows"))
	        {
	            Runtime.getRuntime().exec("cls");
	        }
	        else
	        {
	            Runtime.getRuntime().exec("clear");
	        }
	    }
	    catch (Exception ex)
	    {
	    	// Eclipse控制台清理
//	    	Robot r = new Robot();
//	    	r.mousePress(InputEvent.BUTTON3_MASK); // 按下鼠标右键
//	    	r.mouseRelease(InputEvent.BUTTON3_MASK); // 释放鼠标右键
//	    	r.keyPress(KeyEvent.VK_CONTROL); // 按下Ctrl键
//	    	r.keyPress(KeyEvent.VK_R); // 按下R键
//	    	r.keyRelease(KeyEvent.VK_R); // 释放R键
//	    	r.keyRelease(KeyEvent.VK_CONTROL); // 释放Ctrl键
//	    	r.delay(100);
	        //ex.printStackTrace();
	    }
	}
}
