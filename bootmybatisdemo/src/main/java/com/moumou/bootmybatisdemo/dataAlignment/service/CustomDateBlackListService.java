package com.moumou.bootmybatisdemo.dataAlignment.service;

import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlackList;

public interface CustomDateBlackListService {
	
	List<CustomDateBlackList> queryCustomDateBlackList();
	
	List<CustomDateBlackList> conditionQueryCustomDateBlackList(CustomDateBlackList customDateBlackList);
	
	String addCustomDateBlackList(CustomDateBlackList customDateBlackList);
	
	String updateCustomDateBlackList(CustomDateBlackList customDateBlackList);
	
	Boolean delCustomDateBlackList(CustomDateBlackList customDateBlackList);

}
