package com.moumou.bootmybatisdemo.dataAlignment.service;


import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;

import java.util.List;

public interface SrcSystemService {

    List<SrcSystem> querySrcSystem();

    String uptateSrcSys(SrcSystem srcSystem);

    boolean delSrcSys(SrcSystem srcSystem);

    String addSrcSys(SrcSystem srcSystem);
    
    List<String> querySid(SrcSystem srcSystem);
    
    List<String> querySchema(SrcSystem srcSystem);
}
