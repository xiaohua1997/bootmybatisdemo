package com.moumou.bootmybatisdemo.dataAlignment.controller;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.moumou.bootmybatisdemo.dataAlignment.service.FileService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.FileToZip;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.ScheduleJobs;

@RestController
@RequestMapping("/OracleAndImpala")
public class DownloadFile {

	@RequestMapping("/downloadFile")
	private String downloadFile(HttpServletResponse response) {
		String downloadFilePath = "C:\\Users\\Administrator\\Desktop\\sqoop_project\\sqoop_project.zip";// 被下载的文件在服务器中的路径,
		String fileName = "sqoop_project.zip";// 被下载文件的名称

		File file = new File(downloadFilePath);
		if (file.exists()) {
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
				return "下载成功";
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
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
		return "下载失败";
	}

	@Autowired
	private FileService fileService;
	@Autowired
	private FileToZip fileToZip;

	@RequestMapping("/downfile")
	public void down(HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {
		String dir = URLDecoder.decode(ScheduleJobs.class.getClassLoader().getResource("").getPath(), "utf-8")
				+ "/hive_q";
		String downloadFilePath = dir + "/hive_q.zip";
		String fileName = "hive_q";
		File file = new File(downloadFilePath);
		if (file.exists()) {
			System.out.println(dir + "目录下存在名字为:" + fileName + ".zip" + "打包文件.");
			boolean flag1 = file.delete();
			if (flag1)
				System.out.println("删除成功");
		}
		boolean flag2 = fileToZip.fileToZip(dir, dir, fileName);
		if (flag2)
			System.out.println("打包成功");
		boolean flag3 = fileService.downloadFile(request, response, fileName);
//		boolean flag3 = fileService.downloadFile(request, response, downloadFilePath, fileName + ".zip" );
		if (flag3)
			System.out.println("下载成功");
	}

	

}
