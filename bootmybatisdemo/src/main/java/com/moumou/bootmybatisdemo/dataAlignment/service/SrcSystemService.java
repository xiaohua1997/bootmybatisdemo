package com.moumou.bootmybatisdemo.dataAlignment.service;


import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;

import java.util.List;
import java.util.Map;

public interface SrcSystemService {

    List<SrcSystem> querySrcSystem();

    String uptateSrcSys(SrcSystem srcSystem);

    boolean delSrcSys(SrcSystem srcSystem);

    String addSrcSys(SrcSystem srcSystem);
    
    List<String> querySys();
    
    List<SrcSystem> querySidAndSchema(SrcSystem srcSystem);
}
