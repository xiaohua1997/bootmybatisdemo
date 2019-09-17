package com.moumou.bootmybatisdemo.upload;

import com.moumou.bootmybatisdemo.dataAlignment.controller.SrcSystemController;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.service.SrcSystemService;
import com.moumou.bootmybatisdemo.util.ExcelUtil;
import com.moumou.bootmybatisdemo.util.JsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/srcsystem")
public class UploadFileController {

    private static final Logger LOG = LoggerFactory.getLogger(UploadFileController.class);

    @Autowired
    SrcSystemService srcSystemService;

    //上传 src_system表新增数据（excel）
    @RequestMapping(value = "/fileUpload", method = RequestMethod.POST,
//            consumes = "application/json",produces = "application/json")
            consumes = "multipart/form-data", produces = {"application/json;charset=UTF-8"})
    public @ResponseBody
    JsonResult fileUpload(HttpServletRequest request, @RequestParam("file") MultipartFile file) {
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
                List<SrcSystem> excelBeans = ExcelUtil.readExcel(file, SrcSystem.class);
//                System.out.println(excelBeans.size());

                for (SrcSystem ep : excelBeans) {
                    String stu = srcSystemService.addSrcSys(ep);
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
