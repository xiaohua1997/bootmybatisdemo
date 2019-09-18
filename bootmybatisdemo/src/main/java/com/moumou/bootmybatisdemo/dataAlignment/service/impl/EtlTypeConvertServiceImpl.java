package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.EtlTypeConvertMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;
import com.moumou.bootmybatisdemo.dataAlignment.service.EtlTypeConvertService;

@Service
public class EtlTypeConvertServiceImpl implements EtlTypeConvertService{
	
    @Autowired
    private EtlTypeConvertMapper etlTypeConvertMapper;
    
	@Override
	public List<EtlTypeConvert> queryEtlTypeConvert() {
	
		return etlTypeConvertMapper.queryEtlConvert();
	}

	@Override
	public String updateEtlTypeConvert(EtlTypeConvert etlTypeConvert) {
		int i = etlTypeConvertMapper.updateEtlConvert(etlTypeConvert);
		if(i==0) {
			return "修改失败";
		}else {
			return "修改成功";
		}
	}

	@Override
	public boolean delEtlTypeConvert(EtlTypeConvert etlTypeConvert) {

		return etlTypeConvertMapper.delEtlConvert(etlTypeConvert);
	}

	@Override
	public String addEtlTypeConvert(EtlTypeConvert etlTypeConvert) {
		try {
            int i = etlTypeConvertMapper.addEtlConvert(etlTypeConvert);
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
