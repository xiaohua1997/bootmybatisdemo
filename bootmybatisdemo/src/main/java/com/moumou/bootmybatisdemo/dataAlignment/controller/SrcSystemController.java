package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcSystemService;
import com.moumou.bootmybatisdemo.util.JsonResult;

import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    
    @RequestMapping(value = "/conditionquerysrcsystempage", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody PageInfo conditionQuerySrcSystemPage (@RequestBody SrcSystem srcSystem,Model model) {
    	
    	int pageNum = srcSystem.getCurrentPage();
        LOG.info("条件查询分页");
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
    	List<SrcSystem> srcSystems = srcSystemService.conditionQuerySrcSystem(srcSystem);
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
    public boolean delSrcSystem (@RequestBody SrcSystem srcSystem){
        LOG.info("删除");
        return srcSystemService.delSrcSys(srcSystem);
    }

//  不应该放在srcsystem的controller中
//    @ApiOperation("查询表：src_table")
//    @RequestMapping(value = "/querysrctab",method = RequestMethod.GET)
//    public @ResponseBody List<SrcTable> querySrcTab(){
//        System.out.println(new Date());
//        return srcSystemService.querySrcTable();
//    }
    
    @ApiOperation("查询表：src_system")
    @RequestMapping(value = "/querysys",method = RequestMethod.GET)
    public @ResponseBody JsonResult querySys(){
        System.out.println("查询sys");
        Map<String, Object> map = new HashMap<String, Object>();
        List<String> sys = srcSystemService.querySys();
        map.put("status", "success");
        map.put("msg", "查询成功");
        map.put("sys", sys);
        return new JsonResult(map);
    }
    
    @RequestMapping(value = "/querysidandschema", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public JsonResult querySidAndSchema (@RequestBody SrcSystem srcSystem){
        LOG.info("根据sys查询:"+srcSystem.getSys());
        Map<String, Object> map = new HashMap<String, Object>();
        List<SrcSystem> srcsystem = srcSystemService.querySidAndSchema(srcSystem);
        map.put("status", "success");
        map.put("msg", "查询成功");
        map.put("srcsystem", srcsystem);
        return new JsonResult(map);
    }
}
