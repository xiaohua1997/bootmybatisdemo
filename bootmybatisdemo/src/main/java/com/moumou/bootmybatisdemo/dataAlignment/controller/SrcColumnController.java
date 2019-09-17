package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcColumnService;
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
@RequestMapping("/srccolumn")
public class SrcColumnController {

    @Autowired
    SrcColumnService srcColumnService;

    private static final Logger LOG = LoggerFactory.getLogger(SrcColumnController.class);

    @ApiOperation("查询表：src_column")
    @RequestMapping(value = "/queryAllsrccolumn",method = RequestMethod.GET)
    public @ResponseBody
    JsonResult querySrcColumn(){
        LOG.info("查询");
        List<SrcColumn> srcColumns = srcColumnService.querySrcColumn();
        Map<String, Object> map = new HashMap<String, Object>();
        if(!srcColumns.isEmpty()) {
        	map.put("status", "success");
        	map.put("msg", "查询成功");
        }else {
        	map.put("status", "error");
        	map.put("msg", "查询失败");
        }
        map.put("srcColumns", srcColumns);
        return new JsonResult(map);
    }
    @ApiOperation("查询表(分页)：src_column")
    @RequestMapping(value = "/querysrccolumn",method = RequestMethod.GET)
    public @ResponseBody
    JsonResult querySrcColumnPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
        LOG.info("分页查询");
        Map<String, Object> map = new HashMap<String, Object>();
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<SrcColumn> srcColumns = srcColumnService.querySrcColumn();
        PageInfo<SrcColumn> pageInfo = new PageInfo<SrcColumn>(srcColumns);
//        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        map.put("status", "success");
        map.put("msg", "成功");
        map.put("pageInfo", pageInfo);
        return new JsonResult(map);
    }
    @RequestMapping(value = "/addsrccolumn", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody JsonResult addSrcColumn (@RequestBody SrcColumn srcColumn){
        LOG.info("新增");
        String status = "";
        String string = srcColumnService.addSrcColumn(srcColumn);
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

    @RequestMapping(value = "/updatesrccolumn", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public JsonResult updateSrcColumn (@RequestBody SrcColumn srcColumn){
        LOG.info("修改");
        String status = "";
        String string = srcColumnService.uptateSrcColumn(srcColumn);
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
