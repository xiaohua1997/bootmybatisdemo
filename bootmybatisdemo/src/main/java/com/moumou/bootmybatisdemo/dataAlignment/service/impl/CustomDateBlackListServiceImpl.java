package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.CustomDateBlackListMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlackList;
import com.moumou.bootmybatisdemo.dataAlignment.service.CustomDateBlackListService;

@Service
public class CustomDateBlackListServiceImpl implements CustomDateBlackListService{

	@Autowired
	public CustomDateBlackListMapper customDateBlackListMapper;
	
	@Override
	public List<CustomDateBlackList> queryCustomDateBlackList() {
		
		return customDateBlackListMapper.queryCustomDateBlackList();
	}

	@Override
	public List<CustomDateBlackList> conditionQueryCustomDateBlackList(CustomDateBlackList customDateBlackList) {
		
		return customDateBlackListMapper.conditionCustomDateBlackList(customDateBlackList);
	}

	@Override
	public String addCustomDateBlackList(CustomDateBlackList customDateBlackList) {
		
			int i = customDateBlackListMapper.addCustomDateBlackList(customDateBlackList);
            System.out.println("新增:"+i);
            if(i == 0){
                return "添加失败";
            }
        return "添加成功";
	}

	@Override
	public String updateCustomDateBlackList(CustomDateBlackList customDateBlackList) {
		
		int i = customDateBlackListMapper.uptateCustomDateBlackList(customDateBlackList);
		if(i == 0) {
			return "修改失败";
		}
		return "修改成功";
	}

	@Override
	public Boolean delCustomDateBlackList(CustomDateBlackList customDateBlackList) {

		int i = customDateBlackListMapper.delCustomDateBlackList(customDateBlackList);
		if(i==0) {
			return false;
		}else {
			return true;
		}
	}
}
