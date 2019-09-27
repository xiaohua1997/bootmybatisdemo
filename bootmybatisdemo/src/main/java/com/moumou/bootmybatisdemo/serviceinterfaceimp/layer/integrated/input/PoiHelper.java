package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.input;

import org.apache.poi.ss.usermodel.Cell;

public class PoiHelper {
    /***
     * 源表名
     */
    public static final int SOURCE_TABLE_NAME_INDEX = 1;
    /***
     * 源表中文名
     */
    public static final int SOURCE_TABLE_CN_NAME_INDEX = 2;
    /***
     * 源字段名称
     */
    public static final int SOURCE_FIELD_NAME_INDEX = 3;
    /***
     * 源字段中文名称
     */
    public static final int SOURCE_FIELD_CN_NAME_INDEX = 4;
    /***
     * 映射规则
     */
    public static final int MAPPING_EXPRESSION_INDEX = 8;
    /***
     * 映射规则说明(中文描述)
     */
    public static final int MAPPING_COMMENT_INDEX = 9;
    /***
     * JOIN方式
     */
    public static final int JOIN_TYPE_INDEX = 10;
    /***
     * 次源表名
     */
    public static final int JOIN_TABLE_NAME_INDEX = 11;
    /***
     *次源表别名
     */
    public static final int JOIN_TABLE_AS_NAME_INDEX = 12;
    /***
     * ON条件
     */
    public static final int JOIN_CONDITION_INDEX = 13;
    /***
     *过滤条件
     */
    public static final int WHERE_CONDITION_INDEX = 15;
    /***
     * 目标表名
     */
    public static final int TARGET_TABLE_NAME_INDEX = 18;
    /***
     * 目标表中文名
     */
    public static final int TARGET_TABLE_CN_NAME_INDEX = 19;
    /**
     * 目标字段顺序
     */
    public static final int TARGET_FIELD_NUM_INDEX = 20;
    /**
     *目标字段名称
     */
    public static final int TARGET_FIELD_NAME_INDEX = 21;
    /**
     *目标字段中文名称
     */
    public static final int TARGET_FIELD_CN_NAME_INDEX = 22;
    
    /**
     *目标字段类型 
     */
    public static final int TARGET_FIELD_TYPE_INDEX = 23;
    

    public static final String SOURCE_SCHEMA = "datawarehouse01";
    public static final String TARGET_SCHEMA = "datawarehouse01";

    public static String cellValueToString(Cell cell) {
        String result = "";

        switch (cell.getCellTypeEnum().name()) {
            case "NUMERIC":
                result = cell.getNumericCellValue() + "";
                break;
            case "STRING":
                result = cell.getStringCellValue();
                break;
            default:
                break;
        }

        return  result;
    }
}
