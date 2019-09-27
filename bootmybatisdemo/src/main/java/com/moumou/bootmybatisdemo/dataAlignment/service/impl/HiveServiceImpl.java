package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.model.Hive;
import com.moumou.bootmybatisdemo.dataAlignment.service.HiveService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.input.XlsxInput;
@Service
public class HiveServiceImpl implements HiveService{
	
    XlsxInput xlsxInput  = new XlsxInput();
	@Override
	public String hives() {
		
			xlsxInput.hives();
			return "success";
		
	}
	
	@Override
	public String aHive(Hive hive) {
		
		xlsxInput.aHive(hive);
		return "success";
	}

}
