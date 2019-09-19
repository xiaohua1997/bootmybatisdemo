package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.output;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.input.PoiHelper;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.Block;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.BlockSet;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.target.TargetField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.target.TargetTable;

public class HiveCreateImlTableOutput {

    public static final String EOL = "\n";
    public static final String CHARSET_NAME = "UTF-8";
    


    /**
     * 初始化block，去重、排序字段 
     * @param allBlockSet
     */
    private static java.util.Map<String,TargetTable> initAllBlockSet(Map<String, BlockSet> allBlockSet){
    	java.util.Map<String,TargetTable > targetTableMap = new java.util.HashMap<String, TargetTable>();
        for (String tgtTableName : allBlockSet.keySet()) {
            BlockSet aBlockSet = allBlockSet.get(tgtTableName);
            List<Block> blocks = aBlockSet.get_blocks();
            for (Block block : blocks) {
                   TargetTable aTargetTable = block.get_targetTable();
                   if(!targetTableMap.containsKey(aTargetTable.get_tableName())){
                	   targetTableMap.put(aTargetTable.get_tableName(), aTargetTable);
                   }else{
                	   TargetTable preTargetTable = targetTableMap.get(aTargetTable.get_tableName());
                	   List<TargetField> preTargetField = preTargetTable.get_fields();
                	   preTargetField.addAll(aTargetTable.get_fields());                	   
                   }                
            }
        }
        
        for (String lastTargetTabName : targetTableMap.keySet()) {
        	TargetTable oneTab = targetTableMap.get(lastTargetTabName);
        	 List<TargetField> list = oneTab.get_fields();
        	 List<TargetField> remevodList = removeDuplicateField(list);
        	 List<TargetField> sortList = SortField(remevodList);
        	 oneTab.set_fields(sortList);
        }
    	
        return targetTableMap;
    }
    
    /**
     * 去重
     * @param list
     * @return
     */
    public static List<TargetField> removeDuplicateField(List<TargetField> list){
    	java.util.Set<TargetField> set = new java.util.TreeSet<TargetField>(new Comparator<TargetField>() {
            @Override
            public int compare(TargetField a, TargetField b) {
                // 字符串则按照asicc码升序排列
                return a.get_fieldName().compareTo(b.get_fieldName());
            }
        });
    	set.addAll(list);
    	return new java.util.ArrayList<TargetField>(set);
    }
    
    /**
     * 排序
     * @param list
     * @return
     */
    public static List<TargetField> SortField(List<TargetField> list){
    	Collections.sort(list, new Comparator<TargetField>() {
    		@Override
    		public int compare(TargetField u1,TargetField u2){
    			int diff = u1.get_index() - u2.get_index();
    			if(diff >0){
    				return 1;
    			}else if (diff < 0){
    				return -1;
    			}else return 0;
    			
    		}
    	});
    	return list;
    	
    }
    
    /**
     * 生成建表語句
     * @param target_dir
     * @param allBlockSet
     * @throws IOException
     */
    public static void output(String target_dir, Map<String, BlockSet> allBlockSet) throws IOException {
    	java.util.Map<String,TargetTable> targetTableMap = initAllBlockSet(allBlockSet);
        for (String tgtTableName : targetTableMap.keySet()) {
            StringBuffer sb = new StringBuffer();
            sb.append("drop table if exists ").append(PoiHelper.TARGET_SCHEMA).append(".").append(tgtTableName).append(";").append(EOL);
            sb.append("create table ").append(PoiHelper.TARGET_SCHEMA).append(".").append(tgtTableName).append(" (").append(EOL);
            sb.append("data_date string").append(EOL);
            TargetTable aTargetTable = targetTableMap.get(tgtTableName);
           // System.out.println("-----------------------"+aTargetTable.get_tableName()+"-----------------------");
	        List<TargetField> lstTargetField = aTargetTable.get_fields();
	        if(null != lstTargetField && lstTargetField.size() > 0) {
	           for (int i = 0; i < lstTargetField.size(); i++) {
	             TargetField aTargetField = lstTargetField.get(i);
	             sb.append(",").append(aTargetField.get_fieldName()).append(" ").append(aTargetField.get_fileType()).append(" comment \"").append(aTargetField.get_fieldCnName()).append("\"").append(EOL);
	        //     System.out.println(aTargetField.get_index()+" "+aTargetField.get_fieldName());
	           }
	           sb.append(",etl_time string").append(EOL);
	           sb.append(") partitioned by (part_ymd string comment 'partition_by_year_month_day') ").append(EOL);
	           sb.append("stored as parquet; ").append(EOL);
	           sb.append("invalidate metadata ").append(PoiHelper.TARGET_SCHEMA).append(".").append(aTargetTable.get_tableName()).append(" ;").append(EOL);
	           sb.append("refresh ").append(PoiHelper.TARGET_SCHEMA).append(".").append(aTargetTable.get_tableName()).append(" ;").append(EOL);                    
	         }
	     //输出文件
	     String fileName = tgtTableName;
	     File file = new File(target_dir + File.separator + fileName + ".sql");
	     FileUtils.writeStringToFile(file, sb.toString(), CHARSET_NAME);

      }
    }
}
