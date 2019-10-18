package com.moumou.bootmybatisdemo.upload;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import com.moumou.bootmybatisdemo.dataAlignment.service.HiveService;
import com.moumou.bootmybatisdemo.util.JsonResult;
@RestController
@RequestMapping("/upload")
public class UploadMapping {
	
	@Autowired
	 public HiveService hiveService;
	
	@RequestMapping(value = "/mapping", method = RequestMethod.POST,produces = {"application/json;charset=UTF-8"})
    public @ResponseBody JsonResult  uploadMapping(@RequestParam("file") CommonsMultipartFile file) throws IOException {
         long  startTime=System.currentTimeMillis();
        System.out.println("fileName："+file.getOriginalFilename());
        String path="E:/"+file.getOriginalFilename();
        String fileName=file.getOriginalFilename();
        File newFile=new File(path+fileName);
        //判断文件是否存在，如果存在直接删除
        if(newFile.exists()){
        	newFile.delete();
        }
        //通过CommonsMultipartFile的方法直接写文件（注意这个时候）
        file.transferTo(newFile);
        long  endTime=System.currentTimeMillis();
        System.out.println("采用file.Transto的运行时间："+String.valueOf(endTime-startTime)+"ms");
        
     // String fileSuffix = file.getOriginalFilename().substring(file.getOriginalFilename().lastIndexOf(".")); //获取文件的后缀名.excl
     // XSSFWorkbook  xssfWorkBook=XSSFWorkbookKit.create(file.getInputStream());
        XSSFSheet xssfSheet = (XSSFSheet) ((Workbook) newFile).getSheetAt(0);

        String sheetName = xssfSheet.getSheetName();//获取sheet名称
                    
        Map<String, Object> map = new HashMap<String, Object>();
        String s = hiveService.aMappingHive(path,sheetName);
        if("success".equals(s)) {
        	map.put("status", "success");
	        map.put("msg", "更新所有系统完成");
        }else {
        	map.put("status", "error");
			map.put("msg", "更新失败");
        }
        return new JsonResult(map);
    }

}
