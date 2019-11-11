package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.service.FileService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.DownLoadFile;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.FileToZip;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.ScheduleJobs;

@Service
public class FileServiceImpl implements FileService {
	
	/*
	 * sourceFilePath 要压缩的文件的路径      例如："C:\\Users\\Administrator\\Desktop\\sqoop_project"
	 * zipFilePath 压缩以后文件的存放路径      例如："C:\\Users\\Administrator\\Desktop\\sqoop_project"
	 * fileName 压缩以后的文件名      例如："sqoop_project"
	 */
	@Override
	public boolean fileToZip(String sourceFilePath, String zipFilePath, String fileName) {
		FileToZip fileToZip = new FileToZip();
		boolean flag = fileToZip.fileToZip(sourceFilePath, zipFilePath, fileName);
		if (flag) {
			System.out.println("文件打包成功!");
		} else {
			System.out.println("文件打包失败!");
		}
		return flag;
	}

	@Override
	public String delFile(String downloadFilePath, String fileName) {
		return null;
	}
	
	/*
	 * downloadFilePath 被下载的文件在服务器中的路径,      例如："C:\\Users\\Administrator\\Desktop\\sqoop_project\\sqoop_project.zip"
	 * fileName 被下载文件的名称      例如："sqoop_project.zip"
	 */
	@Override
	public boolean downloadFile(HttpServletRequest request, HttpServletResponse response,String fileName) {
		String dir;
		boolean flag = false;
		try {
			dir = URLDecoder.decode(ScheduleJobs.class.getClassLoader().getResource("").getPath(), "utf-8")
					+ "/hive_q";
			String downloadFilePath = dir + "/hive_q.zip";
			File file = new File(downloadFilePath);
			if (file.exists()) {
				System.out.println(dir + "目录下存在名字为:" + fileName + ".zip" + "打包文件.");
				boolean flag1 = file.delete();
				if (flag1)
					System.out.println("删除成功");
			}
			boolean flag2 = fileToZip(dir, dir, fileName);
			if (flag2)
				System.out.println("打包成功");
			DownLoadFile downLoadFile = new DownLoadFile();
			boolean flag3 = downLoadFile.downFile(request, response, downloadFilePath, fileName + ".zip" );
			if (flag3) {
				System.out.println("文件下载成功!");
			} else {
				System.out.println("文件下载失败!");
			}
			flag = true;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return flag;
	}

}
