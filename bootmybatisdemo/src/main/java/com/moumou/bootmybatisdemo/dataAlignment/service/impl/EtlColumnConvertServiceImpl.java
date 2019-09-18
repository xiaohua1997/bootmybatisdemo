package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.EtlColumnConvertMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;
import com.moumou.bootmybatisdemo.dataAlignment.service.EtlColumnConvertService;

@Service
public class EtlColumnConvertServiceImpl implements EtlColumnConvertService{
	
    @Autowired
    private EtlColumnConvertMapper etlColumnConvertMapper;
    
	@Override
	public List<EtlColumnConvert> queryEtlColumnConvert() {
		
		return etlColumnConvertMapper.queryEtlColumnCon();
	}

	@Override
	public String updateEtlColumnConvert(EtlColumnConvert etlColumnConvert) {
		int i = etlColumnConvertMapper.updateEtlColumnCon(etlColumnConvert);
		if(i==0) {
			return "修改失败";
		}else {
			return "修改成功";
		}
	}

	@Override
	public boolean delEtlColumnConvert(EtlColumnConvert etlColumnConvert) {
		
		return etlColumnConvertMapper.delEtlColumnCon(etlColumnConvert);
	}

	@Override
	public String addEtlColumnConvert(EtlColumnConvert etlColumnConvert) {
		try {
            int i = etlColumnConvertMapper.addEtlColumnCon(etlColumnConvert);
            System.out.println("新增:"+i);
            if(i == 0){
                return "添加失败";
            }

        }catch (DuplicateKeyException e){
                return "添加失败";
        }
        return "添加成功";
	}
}
