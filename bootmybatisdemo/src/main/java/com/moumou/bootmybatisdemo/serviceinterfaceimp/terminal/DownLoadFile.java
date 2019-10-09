package com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletResponse;

public class DownLoadFile {
	
	public boolean downloadFile(HttpServletResponse response,String downloadFilePath,String fileName) {
//		 String downloadFilePath = "C:\\Users\\Administrator\\Desktop\\sqoop_project\\sqoop_project.zip";//被下载的文件在服务器中的路径,
//        String fileName = "sqoop_project.zip";//被下载文件的名称
        
		boolean flag = false;
        File file = new File(downloadFilePath);
        if(file.exists()) {
       	 response.setContentType("application/force-download");// 设置强制下载不打开
       	 
       	 response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);
       	 byte[] buffer = new byte[1024];
       	 FileInputStream fis = null;
       	 BufferedInputStream bis = null;
       	 try {
       		 fis = new FileInputStream(file);
       		 bis = new BufferedInputStream(fis);
       		 OutputStream outputStream = response.getOutputStream();
       		 int i = bis.read(buffer);
       		 while (i != -1) {
                    outputStream.write(buffer, 0, i);
                    i = bis.read(buffer);
                }
       		flag = true;
       	 } catch (Exception e) {
                e.printStackTrace();
            }finally {
                if (bis != null) {
                    try {
                        bis.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
       return flag; 
	}
}
