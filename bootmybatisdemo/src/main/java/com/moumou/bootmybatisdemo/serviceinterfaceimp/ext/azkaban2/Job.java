package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;

public abstract class Job {
	public static final String EOL = "\n";
	public static final String CHARSET_NAME = "UTF-8";
	public static final String RETRIES = "3";             //作业重试次数
	public static final String RETRY_BACKOFF = "300000"; //重试间隔（毫秒）
	private static int CHECK_INTERVAL_MINUTES = 5;        //依赖作业检查间隔（分钟）
	private static long CHECK_MAX_LOOP = 12 * 24;         //依赖作业检查次数

	protected String target_dir;
	protected String defaultFlowName;
	
	protected Job() {}
	
	protected Job(String target_dir, String defaultFlowName) {
		this.target_dir = target_dir;
		this.defaultFlowName = defaultFlowName;
	}
	
	
	public boolean createProjectFile(boolean overwrite) throws IOException {
		boolean createNew = true;
		
		if(overwrite) {
			this.writeProjectFileContent();
			createNew = true;
		} else {
			File file = new File(this.target_dir + File.separator + "azkaban.project");
			if(file.exists()) {
				createNew = false;
			} else {
				this.writeProjectFileContent();
				createNew = true;
			}
		}
		
		return createNew;
	}

	public boolean createFlowFile(boolean overwrite, boolean checkDependencies) throws IOException {
		return this.createFlowFile(overwrite, this.defaultFlowName, checkDependencies);
	}

	public boolean createFlowFile(boolean overwrite, String flowName, boolean checkDependencies) throws IOException {
		boolean createNew = true;
		
		if(overwrite) {
			this.initFlowFileHeadContent(flowName, checkDependencies);
			createNew = true;
		} else {
			File file = new File(this.target_dir + File.separator + flowName + ".flow");
			if(file.exists()) {
				createNew = false;
			} else {
				this.initFlowFileHeadContent(flowName, checkDependencies);
				createNew = true;
			}
		}
		
		return createNew;
	} 
	
	private void writeProjectFileContent() throws IOException {
		StringBuffer sb = new StringBuffer();
		sb.append("azkaban-flow-version: 2.0").append(EOL);
		
		File file = new File(this.target_dir + File.separator + "azkaban.project");
		FileUtils.writeStringToFile(file, sb.toString(), CHARSET_NAME);
	}
	
	private void initFlowFileHeadContent(String flowName, boolean checkDependencies) throws IOException {
		StringBuffer sb = new StringBuffer();
		sb.append("---").append(EOL);
		sb.append("config:").append(EOL);
		sb.append("  retries: ").append(RETRIES).append(EOL);
		sb.append("  retry.backoff: ").append(RETRY_BACKOFF).append(EOL);

		//如果该流的作业需要用到跨流依赖
		if(checkDependencies) {
			sb.append("  check: C").append(EOL);
			sb.append("  check_interval_minutes: ").append(CHECK_INTERVAL_MINUTES).append(EOL);
			sb.append("  check_max_loop: ").append(CHECK_MAX_LOOP).append(EOL);
		}

		sb.append(EOL);
		sb.append("nodes:").append(EOL);
		
		File file = new File(this.target_dir + File.separator + flowName + ".flow");
		FileUtils.writeStringToFile(file, sb.toString(), CHARSET_NAME);
	}

	protected void appendJob(String name, String type, String command, List<String> lstDependsOn) throws IOException{
		this.appendJob(name, type, command, lstDependsOn, this.defaultFlowName, 1);
	}

	private String getFisrtItemBlankBlock(int itemYamlLevel) {
		StringBuffer blankBlock = new StringBuffer();
		for(int i = 1 ; i<= (itemYamlLevel * 4 - 2) ; i++) {
			blankBlock.append(" ");
		}
		return blankBlock.toString();
	}

	private String getNoFirstItemBlankBlock(int itemYamlLevel) {
		StringBuffer blankBlock = new StringBuffer();
		for(int i = 1 ; i<= (itemYamlLevel * 4) ; i++) {
			blankBlock.append(" ");
		}
		return blankBlock.toString();
	}

	private String getSubItemBlankBlock(int itemYamlLevel) {
		StringBuffer blankBlock = new StringBuffer();
		for(int i = 1 ; i<= (itemYamlLevel * 4 + 2) ; i++) {
			blankBlock.append(" ");
		}
		return blankBlock.toString();
	}

	protected void appendJob(String name, String type, String command, List<String> lstDependsOn
			, String flowName, int itemYamlLevel) throws IOException{
		StringBuffer sb = new StringBuffer();

		// 获得YAML层级格式排版用空格字符串
		String fisrtItemBlankBlock = this.getFisrtItemBlankBlock(itemYamlLevel);
		String noFirstItemBlankBlock = this.getNoFirstItemBlankBlock(itemYamlLevel);
		String subItemBlankBlock = this.getSubItemBlankBlock(itemYamlLevel);

		sb.append(fisrtItemBlankBlock).append("- name: ").append(name).append(EOL);
		sb.append(noFirstItemBlankBlock).append("type: ").append(type).append(EOL);

		// 只有非SubFlow类型的项才有Command
		if (!type.equalsIgnoreCase("flow")) {
			sb.append(noFirstItemBlankBlock).append("config:").append(EOL);
			sb.append(subItemBlankBlock).append("command: ").append(command).append(EOL);
		}

		if (null != lstDependsOn && lstDependsOn.size() > 0) {
			sb.append(noFirstItemBlankBlock).append("dependsOn:").append(EOL);
			for (String dependName : lstDependsOn) {
				sb.append(subItemBlankBlock).append("- ").append(dependName).append(EOL);
			}
		}

		// 如果是SubFlow类型，则需要添加node起始字符串
		if (type.equalsIgnoreCase("flow")) {
			sb.append(noFirstItemBlankBlock).append("nodes:");
		}
		
		sb.append(EOL);
		
		File file = new File(this.target_dir + File.separator + flowName + ".flow");
		FileUtils.writeStringToFile(file, sb.toString(), CHARSET_NAME, true);
	}
	
	public static String getShortTableName(HashMap<String, String> tableNameConvertMap, String tableName, String sys) {
		//getConvertTable
		String key = tableName.toLowerCase() + ","  + sys;
		if(tableNameConvertMap.containsKey(key)) {
			return tableNameConvertMap.get(key);
		} else {
			return tableName;
		}
	}
	
	public abstract List<String> appendJobsToFlowFile(List<SourceTable> lstTable
			, HashMap<String, String> schemaNumMap
			, HashMap<String, String> tableNameConvertMap) throws IOException;
}
