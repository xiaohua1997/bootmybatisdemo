package com.moumou.bootmybatisdemo.dataAlignment.service;

import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlack;

public interface CustomDateBlackListService {
	
	List<CustomDateBlack> queryCustomDateBlackList();
	
	List<CustomDateBlack> conditionQueryCustomDateBlackList(CustomDateBlack customDateBlack);
	
	String addCustomDateBlackList(CustomDateBlack customDateBlack);
	
	String updateCustomDateBlackList(CustomDateBlack customDateBlack);
	
	Boolean delCustomDateBlackList(CustomDateBlack customDateBlack);

}
