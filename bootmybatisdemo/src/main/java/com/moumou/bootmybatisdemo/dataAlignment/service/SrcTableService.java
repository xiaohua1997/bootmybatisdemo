package com.moumou.bootmybatisdemo.dataAlignment.service;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;

import java.util.List;

public interface SrcTableService {
    
    List<SrcTable> querySrcTable();

    String uptateSrcTable(SrcTable srcTable);
    
    boolean delSrcTable(SrcTable srcTable);

    String addSrcTable(SrcTable srcTable);
}
