package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelOutput {
	/**
	 * 产出excel
	 * 需要的话 可以增加两个入参，将首行tittle也传入
	 * 
	 * @param dataListSheet1
	 *            第一个sheet的信息
	 * @param dataListSheet2
	 *            第二个sheet的信息
	 * @param basePath
	 *            导出文件路径 例："D:/test/"
	 * @param exportFileName
	 *            文件名 例：数据仓库字典(ODS)
	 */
	public void exportExcel(List<String[]> dataListSheet1, List<String[]> dataListSheet2, String exportFileName,
			String basePath) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("YYYYMMdd_HHmmss");
		String now = dateFormat.format(new Date());
		// 文件名
		// String exportFileName = "数据仓库字典(ODS)_" + now + ".xlsx";
		exportFileName = exportFileName + "_" + now + ".xlsx";
		// 第一个sheet第一行
		String[] cellTitle = { "系统名称", "英文缩写", "系统模块", "表英文名", "中文名", "描述",
				"表类型", "是否存在主键" };
		// 第二个sheet第一行
		// 系统名称 英文缩写 系统模块 表英文名 中文名 字段序号 字段英文名 字段中文名 字段类型 主键 是否允许空值 问题/备注
		String[] cellTitle2 = { "系统名称", "英文缩写", "系统模块", "表英文名", "中文名", "字段序号",
				"字段英文名", "字段中文名", "字段类型", "主键", "是否允许空值", "问题/备注" };
		// 需要导出的数据(主体)
		// List<String[]> dataList = new ArrayList<>();
		// dataList.add(new String[] { "数据仓库", "EDW", "ODS", "EnTable_name",
		// "表中文名", "描述", "快照", "是" });

		// 声明一个工作薄
		XSSFWorkbook workBook = null;
		workBook = new XSSFWorkbook();
		// 生成一个表格
		XSSFSheet sheet = workBook.createSheet();
		// 生成第二个表格
		XSSFSheet sheet2 = workBook.createSheet();
		workBook.setSheetName(0, "表级");
		workBook.setSheetName(1, "字段级");
		// 创建表格标题行 第一行
		XSSFRow titleRow = sheet.createRow(0);
		XSSFRow titleRow2 = sheet2.createRow(0);
		CellStyle cellStyle2 = workBook.createCellStyle();
		cellStyle2.setFillForegroundColor(IndexedColors.YELLOW.getIndex()); // 前景色
		cellStyle2.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		// 第一个表格的首行插入
		for (int i = 0; i < cellTitle.length; i++) {
			titleRow.createCell(i).setCellValue(cellTitle[i]);
			titleRow.getCell(i).setCellStyle(cellStyle2);
			sheet.setColumnWidth(i, cellTitle[i].toString().length() * 1024);
		}
		CellRangeAddress ce = CellRangeAddress.valueOf("A1:H1");
		sheet.setAutoFilter(ce);
		// 第二个表格的首行插入
		for (int i = 0; i < cellTitle2.length; i++) {
			titleRow2.createCell(i).setCellValue(cellTitle2[i]);
			titleRow2.getCell(i).setCellStyle(cellStyle2);
			sheet2.setColumnWidth(i, cellTitle2[i].toString().length() * 1024);
		}
		CellRangeAddress cssecd = CellRangeAddress.valueOf("A1:L1");
		sheet2.setAutoFilter(cssecd);
		// 插入需导出的数据(表主体)
		for (int i = 0; i < dataListSheet1.size(); i++) {
			XSSFRow row = sheet.createRow(i + 1);
			// row.createCell(0).setCellValue(i + 1);
			for (int j = 0; j < dataListSheet1.get(i).length; j++) {
				row.createCell(j).setCellValue(dataListSheet1.get(i)[j]);
			}

		}
		// 插入需导出的数据(第二个sheet表主体)
		for (int i = 0; i < dataListSheet2.size(); i++) {
			XSSFRow row = sheet2.createRow(i + 1);
			// row.createCell(0).setCellValue(i + 1);
			for (int j = 0; j < dataListSheet2.get(i).length; j++) {
				row.createCell(j).setCellValue(dataListSheet2.get(i)[j]);
			}

		}
		File file = new File(basePath + exportFileName);
		//判断路径是否存在
		File folder = new File(basePath);
		folderExists(folder);
		
		// 文件输出流
		FileOutputStream outStream;
		try {
			outStream = new FileOutputStream(file);
			workBook.write(outStream);
			outStream.flush();
			outStream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("导出文件成功！文件导出路径：--" + basePath + exportFileName);
	}

	/**
	 * 判断文件是否存在 如果不存在就新建文件
	 * @param file 
	 */
	public static void fileExists(File file) {
		if (file.exists()) {
			System.out.println("file exists");
		} else {
			System.out.println("file not exists, create it ...");
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	/**
	 * 判断文件夹是否存在 如果不存在就新建文件夹
	 * @param file
	 */
	public static void folderExists(File file) {
		if (file.exists()) {
			if (file.isDirectory()) {
				System.out.println("dir exists");
			} else {
				System.out
						.println("the same name file exists, can not create dir");
			}
		} else {
			System.out.println("dir not exists, create it ...");
			file.mkdir();
		}

	}

}
