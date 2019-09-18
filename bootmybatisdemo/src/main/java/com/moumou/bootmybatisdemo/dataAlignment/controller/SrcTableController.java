package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcTableService;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/srctable")
public class SrcTableController {

    private static final Logger LOG = LoggerFactory.getLogger(SrcTableController.class);

    @Autowired
    SrcTableService srcTableService;

    @ApiOperation("查询表：src_table")
    @RequestMapping(value = "/querysrctable",method = RequestMethod.GET)
    public @ResponseBody
    List<SrcTable> querySrcTable(){
        LOG.info("查询");
        return srcTableService.querySrcTable();
    }
    @ApiOperation("查询表(分页)：src_table")
    @RequestMapping(value = "/querysrctablepage",method = RequestMethod.GET)
    public @ResponseBody PageInfo querySrcTablePage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
        LOG.info("分页查询");
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<SrcTable> srcTables = srcTableService.querySrcTable();
        PageInfo<SrcTable> pageInfo = new PageInfo<SrcTable>(srcTables);
        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        return pageInfo;
    }
    @RequestMapping(value = "/addsrctable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody String addSrcTable (@RequestBody SrcTable srcTable){
        LOG.info("新增");
        return srcTableService.addSrcTable(srcTable);
    }

    @RequestMapping(value = "/updatesrctable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public String updateSrcTable (@RequestBody SrcTable srcTable){
        LOG.info("修改");
        return srcTableService.uptateSrcTable(srcTable);
    }

    @RequestMapping(value = "/delsrctable" ,method = RequestMethod.POST)
    public String delSrcTable (@RequestBody SrcTable srcTable){
        LOG.info("删除");
        srcTableService.delSrcTable(srcTable);
        return null;
    }
}
