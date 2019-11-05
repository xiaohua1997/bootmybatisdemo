package com.moumou.bootmybatisdemo.upload;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;
import com.moumou.bootmybatisdemo.dataAlignment.service.EtlColumnConvertService;
import com.moumou.bootmybatisdemo.util.ExcelUtil;
import com.moumou.bootmybatisdemo.util.JsonResult;

@RestController
@RequestMapping("/etlcolumnimp")
public class EtlColumnConvertImport {

private static final Logger LOG = LoggerFactory.getLogger(EtlColumnConvertImport.class);
	
	@Autowired
	private EtlColumnConvertService etlColumnConvertService;
	
	//上传 表新增数据（excel）
    @RequestMapping(value = "/etlcolumnconimp", method = RequestMethod.POST,
//            consumes = "application/json",produces = "application/json")
            consumes = "multipart/form-data", produces = {"application/json;charset=UTF-8"})
    public @ResponseBody
    JsonResult etlColumnConImp(HttpServletRequest request, @RequestParam("file") MultipartFile file) {
        LOG.info("进入fileUpload方法");
        Map<String, Object> map = new HashMap<String, Object>();
        // 判断文件是否为空
        if (!StringUtils.isEmpty(file)) {
            try {
                String originalFilename = file.getOriginalFilename();
                LOG.info("上传文件名：" + originalFilename);
                String filetype = originalFilename.substring(originalFilename.lastIndexOf(".") + 1).toLowerCase();
                LOG.info("filetype:"+filetype);
                    if(filetype.equals("xls") || filetype.equals("xlsx")){
                    }else{
                        map.put("status", "error");
                        map.put("msg", "文件类型错误");
                        return new JsonResult(map);
                    }
                    List<EtlColumnConvert> excelBeans = ExcelUtil.readExcel(file, EtlColumnConvert.class);
//                  System.out.println(excelBeans.size());

                    for (EtlColumnConvert ep : excelBeans) {
                        String stu = etlColumnConvertService.addEtlColumnConvert(ep);
                        LOG.info(ep.toString());
                        if(null == stu || stu.equals("添加失败")){
                            map.put("status", "error");
                            map.put("msg", "导入错误");
                            return new JsonResult(map);
                        }
                    }
                    map.put("status", "success");
                    map.put("msg", "导入成功");
                    return new JsonResult(map);
                } catch (Exception e) {
                    e.printStackTrace();
                    map.put("status", "error");
                    map.put("msg", "导入错误");
                    return new JsonResult(map);
                }
            } else {
                map.put("status", "error");
                map.put("msg", "文件为空");
                return new JsonResult(map);
            }

        }
}
