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
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlack;
import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlackList;
import com.moumou.bootmybatisdemo.dataAlignment.service.CustomDateBlackListService;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/customdate")
public class CustomDateBlackListController {
	private static final Logger LOG = LoggerFactory.getLogger(EtlColumnConvertController.class);
	
	@Autowired
	public CustomDateBlackListService customDateBlackListService;
	
	@ApiOperation("查询表：custom_date_black_list")
	@RequestMapping(value = "/querycustomdateblacklist", method = RequestMethod.GET)
	public @ResponseBody List<CustomDateBlack> queryCustomDateBlackList(){
		
		System.out.println(customDateBlackListService.queryCustomDateBlackList());
		return customDateBlackListService.queryCustomDateBlackList();
	}
	
	@ApiOperation("查询表(分页)：custom_date_black_list")
	@RequestMapping(value = "/querycustomdateblacklistpage",method = RequestMethod.GET)
	public @ResponseBody PageInfo queryCustomDateBlackListPage(Model model, @RequestParam(defaultValue = "1",value = "pageNum") Integer pageNum) {
	LOG.info("分页查询");
	PageHelper.startPage(pageNum,10);
	LOG.info("pageNum:"+pageNum);
	List<CustomDateBlack> customDateBlackLists = customDateBlackListService.queryCustomDateBlackList();
	PageInfo<CustomDateBlack> pageInfo = new PageInfo<CustomDateBlack>(customDateBlackLists);
	model.addAttribute("pageInfo",pageInfo);
	LOG.info("pageInfo size:"+pageInfo.getSize());
	    return pageInfo;	
	}
	
	@RequestMapping(value = "/conditionquerycustomdateblacklistpage", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody PageInfo conditionQueryCustomDateBlackListPage(@RequestBody CustomDateBlack customDateBlack,Model model){
    	
    	int pageNum = customDateBlack.getCurrentPage();
        LOG.info("条件查询分页");
        PageHelper.startPage(pageNum,10);
        LOG.info("pageNum:"+pageNum);
        List<CustomDateBlack> customDateBlacks = customDateBlackListService.conditionQueryCustomDateBlackList(customDateBlack);
        PageInfo<CustomDateBlack> pageInfo = new PageInfo<CustomDateBlack>(customDateBlacks);
        model.addAttribute("pageInfo",pageInfo);
        LOG.info("pageInfo size:"+pageInfo.getSize());
        return pageInfo;
    }
	
	//设置返回的请求编码（boot返回对象的编码默认：utf-8，直接返回字符串的编码是windows系统默认编码：ISO-8859-1）
	//produces = {"application/json;charset=UTF-8"}
	@RequestMapping(value = "/addcustomdateblacklist", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String addCustomDateBlackList(@RequestBody CustomDateBlack customDateBlack) {
		LOG.info("新增");
		return customDateBlackListService.addCustomDateBlackList(customDateBlack);
	}
	
	@RequestMapping(value = "/updatecustomdateblacklist", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody String updateCustomDateBlackList(@RequestBody CustomDateBlack customDateBlack) {
		LOG.info("修改");
		return customDateBlackListService.updateCustomDateBlackList(customDateBlack);
	}
	
	@RequestMapping(value = "/delcustomdateblacklist", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
	public @ResponseBody Boolean delCustomDateBlackList(@RequestBody CustomDateBlack customDateBlack) {
		LOG.info("删除");
		return customDateBlackListService.delCustomDateBlackList(customDateBlack);
	}
}
