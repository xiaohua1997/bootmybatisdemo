package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTableNameConvert;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcTableNameConvertService;

import io.swagger.annotations.ApiOperation;
	@RestController
	@RequestMapping("/srctablenamecon")
	public class SrcTableNameConvertController {
		private static final Logger LOG = LoggerFactory.getLogger(SrcTableNameConvertController.class);
	@Autowired
	private SrcTableNameConvertService srcTableNameConvertService;
	
	@ApiOperation("查询表：src_tablename_conver")
	@RequestMapping(value = "/querysrcconvert",method = RequestMethod.GET)
	public @ResponseBody List<SrcTableNameConvert> querySrcConvert(){
	    System.out.println("查询");
	    return srcTableNameConvertService.querySrcTableNameConvert();
	  }
	
	@ApiOperation("查询表(分页)：src_tablename_conver")
	@RequestMapping(value = "/querysrcconvertpage",method = RequestMethod.GET)
	public @ResponseBody
	PageInfo querySrcConverPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
	//    System.out.println("分页查询");
	LOG.info("分页查询");
	PageHelper.startPage(pageNum,10);
	LOG.info("pageNum:"+pageNum);
	List<SrcTableNameConvert> srcTableNameConvert = srcTableNameConvertService.querySrcTableNameConvert();
	PageInfo<SrcTableNameConvert> pageInfo = new PageInfo<SrcTableNameConvert>(srcTableNameConvert);
	model.addAttribute("pageInfo",pageInfo);
	LOG.info("pageInfo size:"+pageInfo.getSize());
	    return pageInfo;
	}
	
	//设置返回的请求编码（boot返回对象的编码默认：utf-8，直接返回字符串的编码是windows系统默认编码：ISO-8859-1）
	//produces = {"application/json;charset=UTF-8"}
	@RequestMapping(value = "/addsrcconvert", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String addSrcConver (@RequestBody SrcTableNameConvert srcTableNameConvert){
	  LOG.info("新增");
	  return srcTableNameConvertService.addSrcTableNameConvert(srcTableNameConvert);
	}
	
	@RequestMapping(value = "/updatesrcconvert", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public String updateSrcConver (@RequestBody SrcTableNameConvert srcTableNameConvert){
	  LOG.info("修改-sys:"+srcTableNameConvert.getSys());
	  return srcTableNameConvertService.updateSrcTableNameConvert(srcTableNameConvert);
	}
	
	@RequestMapping(value = "/delsrcconvert" ,method = RequestMethod.POST)
	public String delSrcConver (@RequestBody SrcTableNameConvert srcTableNameConvert){
	  LOG.info("删除");
	  srcTableNameConvertService.delSrcTableNameConvert(srcTableNameConvert);
	  return null;
	  }
	}
