package com.moumou.bootmybatisdemo.dataAlignment.service;

import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTableNameConvert;

public interface SrcTableNameConvertService {
	
	List<SrcTableNameConvert> querySrcTableNameConvert();
	
	List<SrcTableNameConvert> conditionQuerySrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
	
    String addSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
    
    boolean delSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
    
    String updateSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
}
