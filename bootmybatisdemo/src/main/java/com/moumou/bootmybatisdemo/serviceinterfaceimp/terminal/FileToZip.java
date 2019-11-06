package com.moumou.bootmybatisdemo.serviceinterfaceimp.terminal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.springframework.stereotype.Service;

@Service
public class FileToZip {

	private static final int BUFFER_SIZE = 2 * 1024;

	public static void main(String[] args) {
		String zipName = "sqoop_project";// 压缩以后的文件名
		String zipPath = "C:\\Users\\Administrator\\Desktop\\sqoop_project"; // 压缩以后文件的存放路径
		String sourcePath = "C:\\Users\\Administrator\\Desktop\\sqoop_project";// 要压缩的文件的路径
		FileToZip fileToZip = new FileToZip();
		boolean flag = fileToZip.fileToZip(sourcePath, zipPath, zipName);
		if (flag) {
			System.out.println("文件打包成功!");
		} else {
			System.out.println("文件打包失败!");
		}
	}

	public boolean fileToZip(String sourceFilePath, String zipFilePath, String fileName) {
		boolean flag = false;
		File sourceFile = new File(sourceFilePath);
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		FileOutputStream fos = null;
		ZipOutputStream zos = null;

		if (sourceFile.exists() == false) {
			System.out.println("待压缩的文件目录：" + sourceFilePath + "不存在.");
		} else {
			try {
				File zipFile = new File(zipFilePath + "/" + fileName + ".zip");
				if (zipFile.exists()) {
					System.out.println(zipFilePath + "目录下存在名字为:" + fileName + ".zip" + "打包文件.");
				} else {
					File[] sourceFiles = sourceFile.listFiles();
					if (null == sourceFiles || sourceFiles.length < 1) {
						System.out.println("待压缩的文件目录：" + sourceFilePath + "里面不存在文件，无需压缩.");
					} else {
						fos = new FileOutputStream(zipFile);
						zos = new ZipOutputStream(new BufferedOutputStream(fos));
						for (int i = 0; i < sourceFiles.length; i++) {
							tozip(sourceFiles[i],zos,sourceFiles[i].getName());
						}
						flag = true;
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				// 关闭流
				try {
					if (null != bis)
						bis.close();
					if (null != zos)
						zos.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}
		return flag;
	}

	public String tozip(File sourceFile,ZipOutputStream zos,String name) {
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		byte[] bufs = new byte[1024 * 10];
		// 创建ZIP实体，并添加进压缩包
		try {
			if (sourceFile.isFile()) {
				zos.putNextEntry(new ZipEntry(name));
				// 读取待压缩的文件并写进压缩包里
				fis = new FileInputStream(sourceFile);
				bis = new BufferedInputStream(fis, 1024 * 10);
				int read = 0;
				while ((read = bis.read(bufs, 0, 1024 * 10)) != -1) {
					zos.write(bufs, 0, read);
				}
			} else {
				File[] files = sourceFile.listFiles();
				if (files == null || files.length == 0) {
					// 需要保留原来的文件结构时,需要对空文件夹进行处理
					// 空文件夹的处理
					zos.putNextEntry(new ZipEntry(name + "/"));
					// 没有文件，不需要文件的copy
					zos.closeEntry();
				} else {
					for (File file : files) {
						// 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
						// 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
						tozip(file, zos, name + "/" + file.getName());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}
	
	
}
