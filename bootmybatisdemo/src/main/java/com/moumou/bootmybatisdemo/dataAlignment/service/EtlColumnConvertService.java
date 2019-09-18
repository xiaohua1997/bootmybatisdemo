package com.moumou.bootmybatisdemo.dataAlignment.service;

import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;

public interface EtlColumnConvertService {
	
	List<EtlColumnConvert> queryEtlColumnConvert();
	
    String updateEtlColumnConvert(EtlColumnConvert etlColumnConvert);
	
	boolean delEtlColumnConvert(EtlColumnConvert etlColumnConvert);
	
	String addEtlColumnConvert(EtlColumnConvert etlColumnConvert);

}
