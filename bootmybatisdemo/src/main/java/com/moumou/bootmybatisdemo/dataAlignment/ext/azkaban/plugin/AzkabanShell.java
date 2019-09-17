package com.moumou.bootmybatisdemo.dataAlignment.ext.azkaban.plugin;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.dataAlignment.dao.SourceSystemDao;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceSystem;

public class AzkabanShell {
	public static void main(String[] args) throws Exception {
		SourceSystemDao aSourceSystemDao = new SourceSystemDao();
		List<SourceSystem> lst = aSourceSystemDao.getSourceSystem();
		AzkabanShell aAzkabanShell = new AzkabanShell();
		aAzkabanShell.generateSqoopImportShell(lst);
		aAzkabanShell.generateKettleImportShell();
		aAzkabanShell.generateOracleExecuter();
		System.out.println("ok");
	}
	
	public void generateOracleExecuter() throws IOException {
		String shellPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_oracle.sh").getPath()
				, "UTF-8");
		File fShell = new File(shellPath);
		
		String confPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_oracle.conf").getPath()
				, "UTF-8");
		File fConf = new File(confPath);
		
		String TARGET_DIR = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("").getPath(), "UTF-8")
				+ "/kettle_project";
		File fDir = new File(TARGET_DIR);
		
		FileUtils.copyFileToDirectory(fShell, fDir, true);
		FileUtils.copyFileToDirectory(fConf, fDir, true);
	}
	
	public void generateKettleImportShell() throws IOException {
		String shellPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_kettle.sh").getPath()
				, "UTF-8");
		File fShell = new File(shellPath);
		
		String confPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_kettle.conf").getPath()
				, "UTF-8");
		File fConf = new File(confPath);
		
		String TARGET_DIR = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("").getPath(), "UTF-8")
				+ "/kettle_project";
		File fDir = new File(TARGET_DIR);
		
		FileUtils.copyFileToDirectory(fShell, fDir, true);
		FileUtils.copyFileToDirectory(fConf, fDir, true);
	}
	
	public void generateSqoopImportShell(List<SourceSystem> lstSrcSys) throws IOException {
		for (SourceSystem sourceSystem : lstSrcSys) {
			generateSqoopImportShell(sourceSystem);
		}
	}
	
	public void generateSqoopImportShell(SourceSystem sourceSystem) throws IOException {
		String jdbcUrl = "";
		String sys = sourceSystem.getSys();
		String dbType = sourceSystem.getDbType().trim();
		String dbIp = sourceSystem.getDbIp();
		String dbPort = sourceSystem.getDbPort();
		String dbSid = sourceSystem.getDbSid();
		String dbSchema = sourceSystem.getDbSchema();
		
		if (dbType.equalsIgnoreCase("oracle")) {
			jdbcUrl = "jdbc:oracle:thin:@//" + dbIp + ":" + dbPort + "/" + dbSid;
		} else if (dbType.equalsIgnoreCase("mysql")) {
			jdbcUrl = "jdbc:mysql://" + dbIp +":"+ dbPort + "/" + dbSid;
		} else if (dbType.equalsIgnoreCase("mssql")) {
			jdbcUrl = "jdbc:sqlserver://"+ dbIp + ":"+ dbPort + ";database=" + dbSid;
		}
		
		String shellPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_xxx_to_impala.sh").getPath()
						, "UTF-8");
		
		File fShell = new File(shellPath);
		String shellContent = FileUtils.readFileToString(fShell, "UTF-8");
		
		shellContent = shellContent.replace("${EtlAssistPro:jdbcurl}", jdbcUrl)
		.replace("${EtlAssistPro:usr}", sourceSystem.getUserName())
		.replace("${EtlAssistPro:pwd}", sourceSystem.getPassWord());
		
		String TARGET_DIR = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("").getPath(), "UTF-8")
				+ "/sqoop_project";
		
		File file = new File(TARGET_DIR + "/" + "call_" + sys + "_" + dbSid + "_" + dbSchema + "_to_impala.sh");
		FileUtils.writeStringToFile(file, shellContent, "UTF-8");
	}

	public void generateDbCheck(String target_dir) throws IOException {
		String shellPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_dbcheck.sh").getPath()
				, "UTF-8");
		File fShell = new File(shellPath);
		
		String confPath = URLDecoder.decode(AzkabanShell.class.getClassLoader().getResource("call_dbcheck.conf").getPath()
				, "UTF-8");
		File fConf = new File(confPath);
		
		File fDir = new File(target_dir);
		
		FileUtils.copyFileToDirectory(fShell, fDir, true);
		FileUtils.copyFileToDirectory(fConf, fDir, true);
	}
}
