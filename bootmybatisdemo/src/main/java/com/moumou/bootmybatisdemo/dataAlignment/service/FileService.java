package com.moumou.bootmybatisdemo.dataAlignment.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface FileService {
	boolean fileToZip(String sourceFilePath, String zipFilePath, String fileName);
	
	String delFile(String downloadFilePath,String fileName);
	
	boolean downloadFile(HttpServletRequest request, HttpServletResponse response,String fileName);
}
