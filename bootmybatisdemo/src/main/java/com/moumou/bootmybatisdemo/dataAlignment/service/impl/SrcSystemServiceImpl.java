package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.SrcSystemMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcSystemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SrcSystemServiceImpl implements SrcSystemService {

    @Autowired
    private SrcSystemMapper srcSystemMapper;

    @Override
    public List<SrcSystem> querySrcSystem() {

        return srcSystemMapper.queryDBS();
    }

    @Override
    public String uptateSrcSys(SrcSystem srcSystem) {
        int i = srcSystemMapper.uptateSrcSys(srcSystem);
        if(0 == i){
            return "修改失败";
        }else {
            return "修改成功";
        }
    }

    @Override
    public boolean delSrcSys(SrcSystem srcSystem) {
        int i = srcSystemMapper.delSrcSys(srcSystem);
        if(i==0) {
        	return false;
        }else {
        	return true;
        }
    }

    @Override
    public String addSrcSys(SrcSystem srcSystem) {
        try {
            int i = srcSystemMapper.addSrcSys(srcSystem);
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
	public List<SrcSystem> querySidAndSchema(SrcSystem srcSystem) {
		return srcSystemMapper.querySidAndSchema(srcSystem);
	}

	@Override
	public List<String> querySys() {
		return srcSystemMapper.querySys();
	}

	@Override
	public List<SrcSystem> conditionQuerySrcSystem(SrcSystem srcSystem) {
		
		return srcSystemMapper.conditionQuerySrcSystem(srcSystem);
	}

}
