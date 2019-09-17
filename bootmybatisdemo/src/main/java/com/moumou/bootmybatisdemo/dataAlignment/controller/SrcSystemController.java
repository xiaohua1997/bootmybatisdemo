package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.mapper.UserMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.User;
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
@RequestMapping("/srcsys")
public class SrcSystemController {

    private static final Logger LOG = LoggerFactory.getLogger(SrcSystemController.class);

    @Autowired
    private SrcSystemService srcSystemService;

    @ApiOperation("查询表：src_system")
    @RequestMapping(value = "/queryAllsrcsys",method = RequestMethod.GET)
    public @ResponseBody JsonResult querySrcSys(){
    	Map<String, Object> map = new HashMap<String, Object>();
        System.out.println("查询");
        List<SrcSystem> srcSystems = srcSystemService.querySrcSystem();
        if(!srcSystems.isEmpty()) {
        	map.put("status", "success");
        	map.put("msg", "查询成功");
        }else {
        	map.put("status", "error");
        	map.put("msg", "查询失败");
        }
        map.put("srcSystems", srcSystems);
        return new JsonResult(map);
    }
    @ApiOperation("查询表(分页)：src_system")
    @RequestMapping(value = "/querysrcsys",method = RequestMethod.GET)
    public @ResponseBody
    JsonResult querySrcColumnPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
//        System.out.println("分页查询");
    	Map<String, Object> map = new HashMap<String, Object>();
        LOG.info("分页查询");
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<SrcSystem> srcSystems = srcSystemService.querySrcSystem();
        PageInfo<SrcSystem> pageInfo = new PageInfo<SrcSystem>(srcSystems);
        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        map.put("status", "success");
        map.put("msg", "成功");
        map.put("pageInfo", pageInfo);
        return new JsonResult(map);
    }
//    设置返回的请求编码（boot返回对象的编码默认：utf-8，直接返回字符串的编码是windows系统默认编码：ISO-8859-1）
//    produces = {"application/json;charset=UTF-8"}
    @RequestMapping(value = "/addsrcsys", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody JsonResult addSrcSys (@RequestBody SrcSystem srcSystem){
        LOG.info("新增");
        String status = "";
        String string = srcSystemService.addSrcSys(srcSystem);
        Map<String, Object> map = new HashMap<String, Object>();
        if("添加成功".equals(string)) {
        	status = "success";
        }else {
        	status = "error";
        }
        map.put("status", status);
        map.put("msg", string);
        return new JsonResult(map);
    }

    @RequestMapping(value = "/updatesrcsys", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public JsonResult updateSrcSys (@RequestBody SrcSystem srcSystem){
        LOG.info("修改-sys:"+srcSystem.getSys());
        String status = "";
        String string = srcSystemService.uptateSrcSys(srcSystem);
        Map<String, Object> map = new HashMap<String, Object>();
        if("修改成功".equals(string)) {
        	status = "success";
        }else {
        	status = "error";
        }
        map.put("status", status);
        map.put("msg", string);
        return new JsonResult(map);
    }

    @RequestMapping(value = "/delsrcsys" ,method = RequestMethod.POST)
    public JsonResult delSrcSys (@RequestBody SrcSystem srcSystem){
        LOG.info("删除");
        Boolean f = srcSystemService.delSrcSys(srcSystem);
        Map<String, Object> map = new HashMap<String, Object>();
        if(f) {
        	map.put("status", "success");
        	map.put("msg", "删除成功");
        }else {
        	map.put("status", "error");
        	map.put("msg", "删除失败");
        }
        return new JsonResult(map);
    }

//  不应该放在srcsystem的controller中
//    @ApiOperation("查询表：src_table")
//    @RequestMapping(value = "/querysrctab",method = RequestMethod.GET)
//    public @ResponseBody List<SrcTable> querySrcTab(){
//        System.out.println(new Date());
//        return srcSystemService.querySrcTable();
//    }
}
