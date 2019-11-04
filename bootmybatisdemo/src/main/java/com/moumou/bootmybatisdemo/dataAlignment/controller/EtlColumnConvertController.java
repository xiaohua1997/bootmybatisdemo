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
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.service.EtlColumnConvertService;

import io.swagger.annotations.ApiOperation;

	@RestController
	@RequestMapping("/etlcolumncon")
	public class EtlColumnConvertController {
		private static final Logger LOG = LoggerFactory.getLogger(EtlColumnConvertController.class);
		
		@Autowired
		private EtlColumnConvertService etlColumnConvertService;
		
		@ApiOperation("查询表：etl_column_convert")
		@RequestMapping(value = "/queryetlcolumnconvert",method = RequestMethod.GET)
		public @ResponseBody List<EtlColumnConvert> queryEtlColumnConvert(){
		    System.out.println("查询");
		    return etlColumnConvertService.queryEtlColumnConvert();
		  }
		
		@ApiOperation("查询表(分页)：etl_column_convert")
		@RequestMapping(value = "/queryetlcolumnconvertpage",method = RequestMethod.GET)
		public @ResponseBody
		PageInfo queryEtlTypeConvertPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
		//    System.out.println("分页查询");
		LOG.info("分页查询");
		PageHelper.startPage(pageNum,10);
		LOG.info("pageNum:"+pageNum);
		List<EtlColumnConvert> etlColumnConvert = etlColumnConvertService.queryEtlColumnConvert();
		PageInfo<EtlColumnConvert> pageInfo = new PageInfo<EtlColumnConvert>(etlColumnConvert);
		model.addAttribute("pageInfo",pageInfo);
		LOG.info("pageInfo size:"+pageInfo.getSize());
		    return pageInfo;
		}
		
		@RequestMapping(value = "/conditionqueryetlcolumnconvertpage", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	    public @ResponseBody PageInfo conditionQueryEtlTypeConvertPage(@RequestBody EtlColumnConvert etlColumnConvert,Model model){
	    	
	    	int pageNum = etlColumnConvert.getCurrentPage();
	        LOG.info("条件查询分页");
	        PageHelper.startPage(pageNum,10);
	        LOG.info("pageNum:"+pageNum);
	        List<EtlColumnConvert> etlColumnConverts = etlColumnConvertService.conditionQueryEtlColumnConvert(etlColumnConvert);
	        PageInfo<EtlColumnConvert> pageInfo = new PageInfo<EtlColumnConvert>(etlColumnConverts);
	        model.addAttribute("pageInfo",pageInfo);
	        LOG.info("pageInfo size:"+pageInfo.getSize());
	        return pageInfo;
	    }
		
		//设置返回的请求编码（boot返回对象的编码默认：utf-8，直接返回字符串的编码是windows系统默认编码：ISO-8859-1）
		//produces = {"application/json;charset=UTF-8"}
		@RequestMapping(value = "/addetlcolumnconvert", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
		public @ResponseBody String addEtlColumnConvert (@RequestBody EtlColumnConvert etlColumnConvert){
		  LOG.info("新增");
		  return etlColumnConvertService.addEtlColumnConvert(etlColumnConvert);
		}
		
		@RequestMapping(value = "/updateetlcolumnconvert", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
		public String updateEtlColumnConvert (@RequestBody EtlColumnConvert etlColumnConvert){
		  LOG.info("修改-sys:"+etlColumnConvert.getSys());
		  return etlColumnConvertService.updateEtlColumnConvert(etlColumnConvert);
		}
		
		@RequestMapping(value = "/deletlcolumnconvert" ,method = RequestMethod.POST)
		public boolean delEtlColumnConvert (@RequestBody EtlColumnConvert etlColumnConvert){
		  LOG.info("删除");
		  return etlColumnConvertService.delEtlColumnConvert(etlColumnConvert);
		  }
	}
