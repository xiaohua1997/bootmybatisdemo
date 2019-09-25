package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.service.HiveService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.input.XlsxInput;
@Service
public class HiveServiceImpl implements HiveService{
	
    private XlsxInput xlsxInput;
	@Override
	public String hive() {
		
			xlsxInput.hive();
			return "success";
		
	}

}
