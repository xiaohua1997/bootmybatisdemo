package com.moumou.bootmybatisdemo.dataAlignment.service;


import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;

import java.util.List;

public interface SrcSystemService {

    List<SrcSystem> querySrcSystem();
    List<SrcSystem> conditionQuerySrcSystem(SrcSystem srcSystem);

    String uptateSrcSys(SrcSystem srcSystem);

    boolean delSrcSys(SrcSystem srcSystem);

    String addSrcSys(SrcSystem srcSystem);
    
    List<String> querySys();
    
    List<SrcSystem> querySidAndSchema(SrcSystem srcSystem);
}
