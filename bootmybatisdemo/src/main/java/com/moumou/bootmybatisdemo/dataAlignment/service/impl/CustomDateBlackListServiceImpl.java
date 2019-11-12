package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.CustomDateBlackListMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlack;
import com.moumou.bootmybatisdemo.dataAlignment.service.CustomDateBlackListService;

@Service
public class CustomDateBlackListServiceImpl implements CustomDateBlackListService{

	@Autowired
	public CustomDateBlackListMapper customDateBlackListMapper;
	
	@Override
	public List<CustomDateBlack> queryCustomDateBlackList() {
		
		return customDateBlackListMapper.queryCustomDateBlackList();
	}

	@Override
	public List<CustomDateBlack> conditionQueryCustomDateBlackList(CustomDateBlack customDateBlack) {
		
		return customDateBlackListMapper.conditionCustomDateBlackList(customDateBlack);
	}

	@Override
	public String addCustomDateBlackList(CustomDateBlack customDateBlack) {
		
			int i = customDateBlackListMapper.addCustomDateBlackList(customDateBlack);
            System.out.println("新增:"+i);
            if(i == 0){
                return "添加失败";
            }
        return "添加成功";
	}

	@Override
	public String updateCustomDateBlackList(CustomDateBlack customDateBlack) {
		
		int i = customDateBlackListMapper.uptateCustomDateBlackList(customDateBlack);
		if(i == 0) {
			return "修改失败";
		}
		return "修改成功";
	}

	@Override
	public Boolean delCustomDateBlackList(CustomDateBlack customDateBlack) {

		int i = customDateBlackListMapper.delCustomDateBlackList(customDateBlack);
		if(i==0) {
			return false;
		}else {
			return true;
		}
	}
}
