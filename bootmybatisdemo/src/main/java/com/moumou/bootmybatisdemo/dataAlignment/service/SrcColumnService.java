package com.moumou.bootmybatisdemo.dataAlignment.service;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;

import java.util.List;

public interface SrcColumnService {

    List<SrcColumn> querySrcColumn();

    String uptateSrcColumn(SrcColumn srcColumn);
    
    boolean delSrcColumn(SrcColumn srcColumn);

    String addSrcColumn(SrcColumn srcColumn);
    
    String createDictionary();

}
