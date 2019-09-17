package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcTableService;
import com.moumou.bootmybatisdemo.upload.UploadFileController;
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
@RequestMapping("/srctable")
public class SrcTableController {

    private static final Logger LOG = LoggerFactory.getLogger(SrcTableController.class);

    @Autowired
    SrcTableService srcTableService;

    @ApiOperation("查询表：src_table")
    @RequestMapping(value = "/querysrcAllTable",method = RequestMethod.GET)
    public @ResponseBody
    JsonResult querySrcTable(){
        LOG.info("查询");
        List<SrcTable> srcTables = srcTableService.querySrcTable();
        Map<String, Object> map = new HashMap<String, Object>();
        if(!srcTables.isEmpty()) {
        	map.put("status", "success");
        	map.put("msg", "查询成功");
        }else {
        	map.put("status", "error");
        	map.put("msg", "查询失败");
        }
        map.put("srcColumns", srcTables);
        return new JsonResult(map);
    }
    @ApiOperation("查询表(分页)：src_table")
    @RequestMapping(value = "/querysrcTable",method = RequestMethod.GET)
    public @ResponseBody JsonResult querySrcTablePage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
        LOG.info("分页查询");
        Map<String, Object> map = new HashMap<String, Object>();
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<SrcTable> srcTables = srcTableService.querySrcTable();
        PageInfo<SrcTable> pageInfo = new PageInfo<SrcTable>(srcTables);
        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        map.put("status", "success");
        map.put("msg", "成功");
        map.put("pageInfo", pageInfo);
        return new JsonResult();
    }
    @RequestMapping(value = "/addsrcTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody JsonResult addSrcTable (@RequestBody SrcTable srcTable){
        LOG.info("新增");
        String status = "";
        String string = srcTableService.addSrcTable(srcTable);
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

    @RequestMapping(value = "/updatesrcTable", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public JsonResult updateSrcTable (@RequestBody SrcTable srcTable){
        LOG.info("修改");
        String status = "";
        String string = srcTableService.uptateSrcTable(srcTable);
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


}
