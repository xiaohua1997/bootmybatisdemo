package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcSystemService;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/srcsystem")
public class SrcSystemController {

    private static final Logger LOG = LoggerFactory.getLogger(SrcSystemController.class);

    @Autowired
    private SrcSystemService srcSystemService;

    @ApiOperation("查询表：src_system")
    @RequestMapping(value = "/querysrcsystem",method = RequestMethod.GET)
    public @ResponseBody List<SrcSystem> querySrcSystem(){
        System.out.println("查询");
        return srcSystemService.querySrcSystem();
    }
    @ApiOperation("查询表(分页)：src_system")
    @RequestMapping(value = "/querysrcsystempage",method = RequestMethod.GET)
    public @ResponseBody
    PageInfo querySrcSystemPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
//        System.out.println("分页查询");
        LOG.info("分页查询");
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<SrcSystem> srcSystems = srcSystemService.querySrcSystem();
        PageInfo<SrcSystem> pageInfo = new PageInfo<SrcSystem>(srcSystems);
        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        return pageInfo;
    }
//    设置返回的请求编码（boot返回对象的编码默认：utf-8，直接返回字符串的编码是windows系统默认编码：ISO-8859-1）
//    produces = {"application/json;charset=UTF-8"}
    @RequestMapping(value = "/addsrcsystem", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody String addSrcSystem (@RequestBody SrcSystem srcSystem){
        LOG.info("新增");
        return srcSystemService.addSrcSys(srcSystem);
    }

    @RequestMapping(value = "/updatesrcsystem", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public String updateSrcSystem (@RequestBody SrcSystem srcSystem){
        LOG.info("修改-sys:"+srcSystem.getSys());
        return srcSystemService.uptateSrcSys(srcSystem);
    }

    @RequestMapping(value = "/delsrcsystem" ,method = RequestMethod.POST)
    public String delSrcSystem (@RequestBody SrcSystem srcSystem){
        LOG.info("删除");
        srcSystemService.delSrcSys(srcSystem);
        return null;
    }

//  不应该放在srcsystem的controller中
//    @ApiOperation("查询表：src_table")
//    @RequestMapping(value = "/querysrctab",method = RequestMethod.GET)
//    public @ResponseBody List<SrcTable> querySrcTab(){
//        System.out.println(new Date());
//        return srcSystemService.querySrcTable();
//    }
    
    @RequestMapping(value = "/querySid", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public List<String> querySid (@RequestBody SrcSystem srcSystem){
        LOG.info("根据sys查询:"+srcSystem.getSys());
        return srcSystemService.querySid(srcSystem);
    }
    
    @RequestMapping(value = "/querySchema", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public List<String> querySchema (@RequestBody SrcSystem srcSystem){
        LOG.info("根据sys和sid查询:"+srcSystem.getSys()+","+srcSystem.getDbSid());
        return srcSystemService.querySchema(srcSystem);
    }
}
