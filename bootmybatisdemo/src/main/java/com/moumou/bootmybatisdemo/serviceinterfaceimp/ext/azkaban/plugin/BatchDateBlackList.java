package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban.plugin;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.dao.CustomDateBlackListDao;
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlackList;

public class BatchDateBlackList {
	public static void main(String[] args) {
//		Calendar instance = Calendar.getInstance();
//		instance.set(2019, 5-1, 29);
//		
//		System.out.println(instance.toString());
//        System.out.println(instance.get(Calendar.YEAR)+"年");
//        System.out.println((instance.get(Calendar.MONTH)+1)+"月");
//        System.out.println(instance.get(Calendar.DATE)+"日");
//        System.out.println(instance.get(Calendar.HOUR_OF_DAY)+"时"); //24小时制
//		System.out.println(instance.get(Calendar.DAY_OF_WEEK));
//		
//		instance.add(Calendar.DATE, 1);
//		System.out.println(instance.toString());
//        System.out.println(instance.get(Calendar.YEAR)+"年");
//        System.out.println((instance.get(Calendar.MONTH)+1)+"月");
//        System.out.println(instance.get(Calendar.DATE)+"日");
//        System.out.println(instance.get(Calendar.HOUR_OF_DAY)+"时"); //24小时制
//		System.out.println(instance.get(Calendar.DAY_OF_WEEK));
//		
//		instance.add(Calendar.DATE, 1);
//		System.out.println(instance.toString());
//        System.out.println(instance.get(Calendar.YEAR)+"年");
//        System.out.println((instance.get(Calendar.MONTH)+1)+"月");
//        System.out.println(instance.get(Calendar.DATE)+"日");
//        System.out.println(instance.get(Calendar.HOUR_OF_DAY)+"时"); //24小时制
//		System.out.println(instance.get(Calendar.DAY_OF_WEEK));
//		
//		instance.add(Calendar.DATE, 1);
//		System.out.println(instance.toString());
//        System.out.println(instance.get(Calendar.YEAR)+"年");
//        System.out.println((instance.get(Calendar.MONTH)+1)+"月");
//        System.out.println(instance.get(Calendar.DATE)+"日");
//        System.out.println(instance.get(Calendar.HOUR_OF_DAY)+"时"); //24小时制
//		System.out.println(instance.get(Calendar.DAY_OF_WEEK));
//		
//		instance.add(Calendar.DATE, 1);
//		System.out.println(instance.toString());
//        System.out.println(instance.get(Calendar.YEAR)+"年");
//        System.out.println((instance.get(Calendar.MONTH)+1)+"月");
//        System.out.println(instance.get(Calendar.DATE)+"日");
//        System.out.println(instance.get(Calendar.HOUR_OF_DAY)+"时"); //24小时制
//		System.out.println(instance.get(Calendar.DAY_OF_WEEK));
//		
//		String start_dt = "20190529";
//		int start_year = Integer.parseInt(start_dt.substring(0, 4));
//		int start_month = Integer.parseInt(start_dt.substring(4, 6));
//		int start_date = Integer.parseInt(start_dt.substring(6, 8));
//		System.out.println(start_year);
//		System.out.println(start_month);
//		System.out.println(start_date);
//		
		new BatchDateBlackList().getWeekend("20190101", "20191231");
	}
	
	public static final String EOL = "\n";
	public static final String CHARSET_NAME = "UTF-8";
	
	protected String fileName = "black.calendar";
	
	public void generate(String target_dir, String start_dt, String end_dt, boolean addCustom) throws IOException {
		List<String> lstWek = this.getWeekend(start_dt, end_dt);
		
		if(addCustom) {
			List<String> lstCustom = this.getCustom();
			if (lstCustom.size() > 0) {
				lstWek.addAll(lstCustom);
			}
		}
		
		if (lstWek.size() > 0) {
			File file = new File(target_dir + File.separator + this.fileName);
			FileUtils.writeLines(file, lstWek, EOL, false);
		}
	}
	
	private List<String> getWeekend(String start_dt, String end_dt) {
		List<String> lstResult = new ArrayList<String>();
		
		int start_year = Integer.parseInt(start_dt.substring(0, 4));
		int start_month = Integer.parseInt(start_dt.substring(4, 6));
		int start_date = Integer.parseInt(start_dt.substring(6, 8));
		
		Calendar instance = Calendar.getInstance();
		instance.set(start_year, start_month - 1, start_date);
		
		SimpleDateFormat spf = new SimpleDateFormat("yyyyMMdd");
		
		// 比较逻辑
		int day_of_week = instance.get(Calendar.DAY_OF_WEEK);
		if(Calendar.SUNDAY == day_of_week || Calendar.SATURDAY == day_of_week) {
			//将周末的日期加入列表
			lstResult.add(spf.format(instance.getTime()));
		}
		do {
			instance.add(Calendar.DATE, 1);
			day_of_week = instance.get(Calendar.DAY_OF_WEEK);
			if(Calendar.SUNDAY == day_of_week || Calendar.SATURDAY == day_of_week) {
				//将周末的日期加入列表
				lstResult.add(spf.format(instance.getTime()));
			}
		} while (!end_dt.equals(spf.format(instance.getTime())));
		
		return lstResult;
	}

	private List<String> getCustom() {
		List<String> lstResult = new ArrayList<String>();
		
		for (CustomDateBlackList obj : new CustomDateBlackListDao().getRecords()) {
			lstResult.add(obj.get_batch_date());
		} 
		
		return lstResult;
	}
}
