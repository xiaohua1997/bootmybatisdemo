package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.SrcTableMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SrcTableServiceImpl implements SrcTableService{

    @Autowired
    private SrcTableMapper srcTableMapper;

    @Override
    public List<SrcTable> querySrcTable() {
        
        return srcTableMapper.queryDBS();
    }

    @Override
    public String uptateSrcTable(SrcTable srcTable) {
        int i = srcTableMapper.uptateSrcTable(srcTable);
        if(0 == i){
            return "修改失败";
        }else {
            return "修改成功";
        }
    }

    @Override
    public String addSrcTable(SrcTable srcTable) {
        try {
            int i = srcTableMapper.addSrcTable(srcTable);
            if(0 == i){
                return "添加失败";
            }

        }catch (DuplicateKeyException e){
            return "添加失败";
        }
        return "添加成功";
    }

	@Override
	public boolean delSrcTable(SrcTable srcTable) {
		int i = srcTableMapper.delSrcTable(srcTable);
		if(i==0) {
			return false;
		}else {
			return true;
		}
	}
}
