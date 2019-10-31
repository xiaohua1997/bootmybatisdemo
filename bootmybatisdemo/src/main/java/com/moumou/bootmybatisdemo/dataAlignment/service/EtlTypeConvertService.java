package com.moumou.bootmybatisdemo.dataAlignment.service;

import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;

public interface EtlTypeConvertService {
	
	List<EtlTypeConvert> queryEtlTypeConvert();
	
	String updateEtlTypeConvert(EtlTypeConvert etlTypeConvert, EtlTypeConvert etlTypeConvert1);
	
	boolean delEtlTypeConvert(EtlTypeConvert etlTypeConvert);
	
	String addEtlTypeConvert(EtlTypeConvert etlTypeConvert);

}
