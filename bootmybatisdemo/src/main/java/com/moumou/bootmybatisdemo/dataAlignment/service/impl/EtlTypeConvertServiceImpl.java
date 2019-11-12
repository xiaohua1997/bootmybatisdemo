package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.EtlTypeConvertMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvertCopy;
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
	public String updateEtlTypeConvert(EtlTypeConvert etlTypeConvert,EtlTypeConvert etlTypeConvert1) {
		
		String srcDbType1 = etlTypeConvert1.getSrcDbType();
		String srcColumnType1 = etlTypeConvert1.getSrcColumnType();
		String tgtDbType1 = etlTypeConvert1.getTgtDbType();
		String tgtColumnType1 = etlTypeConvert1.getTgtColumnType();
		String tgtColumnBigType1 = etlTypeConvert1.getTgtColumnBigType();
		String tgtColumnLength1 = etlTypeConvert1.getTgtColumnLength();
		String tgtColumnDefault1 = etlTypeConvert1.getTgtColumnDefault();
		String tgtColumnFormat1 = etlTypeConvert1.getTgtColumnFormat();
		String convertMode1 = etlTypeConvert1.getConvertMode();
		
		EtlTypeConvertCopy etlTypeConvertCopy = new EtlTypeConvertCopy();
		etlTypeConvertCopy.setSrcDbType1(srcDbType1);
		etlTypeConvertCopy.setSrcColumnType1(srcColumnType1);
		etlTypeConvertCopy.setTgtDbType1(tgtDbType1);
		etlTypeConvertCopy.setTgtColumnType1(tgtColumnType1);
		etlTypeConvertCopy.setTgtColumnBigType1(tgtColumnBigType1);
		etlTypeConvertCopy.setTgtColumnLength1(tgtColumnLength1);
		etlTypeConvertCopy.setTgtColumnDefault1(tgtColumnDefault1);
		etlTypeConvertCopy.setTgtColumnFormat1(tgtColumnFormat1);
		etlTypeConvertCopy.setConvertMode1(convertMode1);
		
		
		int i = etlTypeConvertMapper.updateEtlConvert(etlTypeConvertCopy,etlTypeConvert,etlTypeConvert1);
		if(i==0) {
			return "修改失败";
		}else {
			return "修改成功";
		}
	}

	@Override
	public boolean delEtlTypeConvert(EtlTypeConvert etlTypeConvert) {
        int i = etlTypeConvertMapper.delEtlConvert(etlTypeConvert);
        if(i==0) {
        	return false;
        }else {
        	return true;
        }
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

	@Override
	public List<EtlTypeConvert> conditionQueryEtlTypeConvert(EtlTypeConvert etlTypeConvert) {
		
		return etlTypeConvertMapper.conditionQueryEtlTypeConvert(etlTypeConvert);
	}

}
