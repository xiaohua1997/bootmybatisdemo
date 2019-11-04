package com.moumou.bootmybatisdemo.dataAlignment.service;

import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;

public interface EtlColumnConvertService {
	
	List<EtlColumnConvert> queryEtlColumnConvert();
	List<EtlColumnConvert> conditionQueryEtlColumnConvert(EtlColumnConvert etlColumnConvert);
	
    String updateEtlColumnConvert(EtlColumnConvert etlColumnConvert);
	
	boolean delEtlColumnConvert(EtlColumnConvert etlColumnConvert);
	
	String addEtlColumnConvert(EtlColumnConvert etlColumnConvert);

}
