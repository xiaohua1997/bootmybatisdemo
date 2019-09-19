package com.moumou.bootmybatisdemo.serviceinterfaceimp.metamgr;

import java.util.List;
import java.util.Map;

public class ExportExcelTest {
	public static void main(String[] args) {
		//表级 (%表示全部系统的表级,生成具体系统可传入相应的系统缩写 如：CPZX)
		Map<String, List<String[]>>  qryTableInfoTableAll = new TableInfoExcle().qryTableInfoGather("%");
		//取到表级信息 (impala)
		List<String[]> impalaTableInfoList = qryTableInfoTableAll.get("impala");
		//取到表级信息 (oracle)
		List<String[]> oracleTableInfoList = qryTableInfoTableAll.get("oracle");
		//字段级 (impala) (%表示全部系统的字段级,生成具体系统可传入相应的系统缩写 如：CPZX)
		List<String[]> dataListImpalaColumnAll = new ImlapaColumnInfoExcel().qryColumnInfoGather("%");
		System.out.println(dataListImpalaColumnAll.size());
		//字段级 (oracle) (%表示全部系统的字段级,生成具体系统可传入相应的系统缩写 如：CPZX)
		List<String[]> dataListOracleColumnAll = new OracleColumnInfoExcel().qryColumnInfoGather("%");
		System.out.println(dataListOracleColumnAll.size());
		
		//两个 库生成到同一个excel时的合并操作
		//dataListImpalaColumnAll.addAll(dataListOracleColumnAll);
		//System.out.println(dataListImpalaColumnAll.size());

		//导出excel(impala)
		new ExcelOutput().exportExcel(impalaTableInfoList,dataListImpalaColumnAll, "数据仓库字典(ODS)_impala", "E:/DataDictionary/");
		//导出excel(oracle)
		new ExcelOutput().exportExcel(oracleTableInfoList,dataListOracleColumnAll, "数据仓库字典(ODS)_oracle", "E:/DataDictionary/");
		
	}
}
