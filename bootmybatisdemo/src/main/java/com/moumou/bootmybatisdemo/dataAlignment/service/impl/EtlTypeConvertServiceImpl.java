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
	public String updateEtlTypeConvert(EtlTypeConvert etlTypeConvert,EtlTypeConvert etlTypeConvert1) {
		String srcdbtype = etlTypeConvert.getSrcDbType();
		String srccolumntype = etlTypeConvert.getSrcColumnType();
		String tgtdbtype = etlTypeConvert.getTgtDbType();
		String tgtcolumntype = etlTypeConvert.getTgtColumnType();
		String tgtcolumnbigtype = etlTypeConvert.getTgtColumnBigType();
		String tgtcolumnlength = etlTypeConvert.getTgtColumnLength();
		String tgtcolumndefault = etlTypeConvert.getTgtColumnDefault();
		String tgtcolumnformat = etlTypeConvert.getTgtColumnFormat();
		String convertmode = etlTypeConvert.getConvertMode();
		
		String srcdbtype1 = etlTypeConvert1.getSrcDbType();
		String srccolumntype1 = etlTypeConvert1.getSrcColumnType();
		String tgtdbtype1 = etlTypeConvert1.getTgtDbType();
		String tgtcolumntype1 = etlTypeConvert1.getTgtColumnType();
		String tgtcolumnbigtype1 = etlTypeConvert1.getTgtColumnBigType();
		String tgtcolumnlength1 = etlTypeConvert1.getTgtColumnLength();
		String tgtcolumndefault1 = etlTypeConvert1.getTgtColumnDefault();
		String tgtcolumnformat1 = etlTypeConvert1.getTgtColumnFormat();
		String convertmode1 = etlTypeConvert1.getConvertMode();
		
		int i = etlTypeConvertMapper.updateEtlConvert(etlTypeConvert,etlTypeConvert1);
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
