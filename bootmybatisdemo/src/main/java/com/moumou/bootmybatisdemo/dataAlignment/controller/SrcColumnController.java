package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcColumnService;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/srccolumn")
public class SrcColumnController {

    @Autowired
    SrcColumnService srcColumnService;

    private static final Logger LOG = LoggerFactory.getLogger(SrcColumnController.class);

    @ApiOperation("查询表：src_column")
    @RequestMapping(value = "/querysrccolumn",method = RequestMethod.GET)
    public @ResponseBody
    List<SrcColumn> querySrcColumn(){
        LOG.info("查询");

        return srcColumnService.querySrcColumn();
    }
    @ApiOperation("查询表(分页)：src_column")
    @RequestMapping(value = "/querysrccolumnpage",method = RequestMethod.GET)
    public @ResponseBody
    PageInfo querySrcColumnPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
        LOG.info("分页查询");
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<SrcColumn> srcColumns = srcColumnService.querySrcColumn();
        PageInfo<SrcColumn> pageInfo = new PageInfo<SrcColumn>(srcColumns);
//        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        return pageInfo;
    }
    @RequestMapping(value = "/addsrccolumn", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody String addSrcColumn (@RequestBody SrcColumn srcColumn){
        LOG.info("新增");
        return srcColumnService.addSrcColumn(srcColumn);
    }

    @RequestMapping(value = "/updatesrccolumn", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public String updateSrcColumn (@RequestBody SrcColumn srcColumn){
        LOG.info("修改");
        return srcColumnService.uptateSrcColumn(srcColumn);
    }
    
    @RequestMapping(value = "/delsrccolumn" ,method = RequestMethod.POST)
    public boolean delSrcColumn (@RequestBody SrcColumn srcColumn){
        LOG.info("删除");
        return srcColumnService.delSrcColumn(srcColumn);
    }
    @ApiOperation("生成字典：src_column")
    @RequestMapping(value = "/createDictionary",method = RequestMethod.GET)
    public @ResponseBody
    String createDictionary(){
        return srcColumnService.createDictionary();
    }
}
