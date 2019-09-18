package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.SrcTableNameConvertMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTableNameConvert;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcTableNameConvertService;
@Service
public class SrcTableNameConvertServiceImpl implements SrcTableNameConvertService{
	
    @Autowired 
    private SrcTableNameConvertMapper srcTableNameConvertMapper;
    
	@Override
	public List<SrcTableNameConvert> querySrcTableNameConvert() {
		
		return srcTableNameConvertMapper.querySrcTableNC();
	}

	@Override
	public String addSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert) {
		try {
            int i = srcTableNameConvertMapper.addSrcTableNameConvert(srcTableNameConvert);
            System.out.println("新增:"+i);
            if(0 == i){
                return "添加失败";
            }

        }catch (DuplicateKeyException e){
                return "添加失败";
        }
        return "添加成功";
	}

	@Override
	public boolean delSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert) {
		
		return srcTableNameConvertMapper.delSrcTableNameConvert(srcTableNameConvert);
	}

	@Override
	public String updateSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert) {
		int i = srcTableNameConvertMapper.updateSrcTableNameConvert(srcTableNameConvert);
		if(0 == i){
            return "修改失败";
        }else {
            return "修改成功";
        }
	}

}
