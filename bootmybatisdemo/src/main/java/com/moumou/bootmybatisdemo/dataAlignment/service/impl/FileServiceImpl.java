package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Service;

import com.moumou.bootmybatisdemo.dataAlignment.service.FileService;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.DownLoadFile;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal.FileToZip;

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
	public boolean downloadFile(HttpServletResponse response, String downloadFilePath, String fileName) {
		DownLoadFile downLoadFile = new DownLoadFile();
		boolean flag = downLoadFile.downloadFile(response, downloadFilePath, fileName);
		if (flag) {
			System.out.println("文件下载成功!");
		} else {
			System.out.println("文件下载失败!");
		}
		return flag;
	}

}
