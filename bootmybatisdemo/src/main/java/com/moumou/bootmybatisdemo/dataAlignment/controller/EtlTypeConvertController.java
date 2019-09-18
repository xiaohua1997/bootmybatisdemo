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
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;
import com.moumou.bootmybatisdemo.dataAlignment.service.EtlTypeConvertService;

import io.swagger.annotations.ApiOperation;

	@RestController
	@RequestMapping("/etltypecon")
	public class EtlTypeConvertController {
		private static final Logger LOG = LoggerFactory.getLogger(EtlTypeConvertController.class);
		@Autowired
		private EtlTypeConvertService etlTypeConvertService;
		
		@ApiOperation("查询表：etl_type_convert")
		@RequestMapping(value = "/queryetltypeconvert",method = RequestMethod.GET)
		public @ResponseBody List<EtlTypeConvert> queryEtlTypeConvert(){
		    System.out.println("查询");
		    return etlTypeConvertService.queryEtlTypeConvert();
		  }
		
		@ApiOperation("查询表(分页)：etl_type_convert")
		@RequestMapping(value = "/queryetltypeconvertpage",method = RequestMethod.GET)
		public @ResponseBody
		PageInfo queryEtlTypeConvertPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum){
		//    System.out.println("分页查询");
		LOG.info("分页查询");
		PageHelper.startPage(pageNum,10);
		LOG.info("pageNum:"+pageNum);
		List<EtlTypeConvert> etlTypeConvert = etlTypeConvertService.queryEtlTypeConvert();
		PageInfo<EtlTypeConvert> pageInfo = new PageInfo<EtlTypeConvert>(etlTypeConvert);
		model.addAttribute("pageInfo",pageInfo);
		LOG.info("pageInfo size:"+pageInfo.getSize());
		    return pageInfo;
		}
		//设置返回的请求编码（boot返回对象的编码默认：utf-8，直接返回字符串的编码是windows系统默认编码：ISO-8859-1）
		//produces = {"application/json;charset=UTF-8"}
		@RequestMapping(value = "/addetltypeconvert", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
		public @ResponseBody String addEtlTypeConvert (@RequestBody EtlTypeConvert etlTypeConvert){
		  LOG.info("新增");
		  return etlTypeConvertService.addEtlTypeConvert(etlTypeConvert);
		}
		
		@RequestMapping(value = "/updateetltypeconvert", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
		public String updateEtlTypeConvert (@RequestBody EtlTypeConvert etlTypeConvert){
		  LOG.info("修改");
		  return etlTypeConvertService.updateEtlTypeConvert(etlTypeConvert);
		}
		
		@RequestMapping(value = "/deletltypeconvert" ,method = RequestMethod.POST)
		public String delEtlTypeConvert (@RequestBody EtlTypeConvert etlTypeConvert){
		  LOG.info("删除");
		  etlTypeConvertService.delEtlTypeConvert(etlTypeConvert);
		  return null;
		  }
	}
