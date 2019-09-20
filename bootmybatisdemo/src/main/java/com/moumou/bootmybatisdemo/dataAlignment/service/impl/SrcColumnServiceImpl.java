package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.SrcColumnMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcColumnService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr.ExportExcelTest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SrcColumnServiceImpl implements SrcColumnService {

    @Autowired
    private SrcColumnMapper srcColumnMapper;
    private ExportExcelTest exportExcelTest;

    @Override
    public List<SrcColumn> querySrcColumn() {

        return srcColumnMapper.queryDBS();
    }

    @Override
    public String uptateSrcColumn(SrcColumn srcColumn) {
        int i = srcColumnMapper.uptateSrcColumn(srcColumn);
        if(0 == i){
            return "修改失败";
        }else {
            return "修改成功";
        }
    }

    @Override
    public String addSrcColumn(SrcColumn srcColumn) {
        try {
            int i = srcColumnMapper.addSrcColumn(srcColumn);
            if(0 == i){
                return "添加失败";
            }

        }catch (DuplicateKeyException e){
            return "添加失败";
        }
        return "添加成功";
    }

	@Override
	public boolean delSrcColumn(SrcColumn srcColumn) {
		int i = srcColumnMapper.delSrcColumn(srcColumn);
		if(i==0) {
			return false;
		}else {
			return true;
		}
	}

	@Override
	public String createDictionary() {
		exportExcelTest.createDictionary();
		return "";
	}
}
