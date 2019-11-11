package com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import javax.servlet.http.HttpServletRequest;
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
	
	public boolean downFile(HttpServletRequest request, HttpServletResponse response, String fileUrl,
			String fileName) {
		boolean flag = false;
		BufferedInputStream bis = null;
		BufferedOutputStream bos = null;
		OutputStream fos = null;
		InputStream fis = null;
		try {
			fis = new FileInputStream(fileUrl);
			bis = new BufferedInputStream(fis);
			fos = response.getOutputStream();
			bos = new BufferedOutputStream(fos);
			setFileDownloadHeader(request, response, fileName);
			int byteRead = 0;
			byte[] buffer = new byte[8192];
			while ((byteRead = bis.read(buffer, 0, 8192)) != -1) {
				bos.write(buffer, 0, byteRead);
			}
			bos.flush();
			fis.close();
			bis.close();
			fos.close();
			bos.close();
			flag = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	public void setFileDownloadHeader(HttpServletRequest request, HttpServletResponse response,
			String fileName) {
		try {
			// 中文文件名支持
			String encodedfileName = null;
			String agent = request.getHeader("USER-AGENT");

			if (null != agent && -1 != agent.indexOf("MSIE")) {// IE
				encodedfileName = java.net.URLEncoder.encode(fileName, "UTF-8");
			} else if (null != agent && -1 != agent.indexOf("Mozilla")) {
				encodedfileName = new String(fileName.getBytes("UTF-8"), "iso-8859-1");
			} else {
				encodedfileName = java.net.URLEncoder.encode(fileName, "UTF-8");
			}
			response.setHeader("Content-Disposition", "attachment; filename=\"" + encodedfileName + "\"");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}
