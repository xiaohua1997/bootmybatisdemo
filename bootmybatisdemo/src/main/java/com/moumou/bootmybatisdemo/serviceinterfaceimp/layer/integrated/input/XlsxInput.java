package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.input;

import com.moumou.bootmybatisdemo.dataAlignment.model.Hive;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.common.StringExtension;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.output.HiveCreateImlTableOutput;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.output.HiveScriptOutput;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.Block;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.BlockSet;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.SelectFields;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.JoinCondition;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.WhereCondition;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.DirectField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.ExpressionField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.SourceField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.target.TargetField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source.*;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.target.TargetTable;

import org.apache.commons.io.FileUtils;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class XlsxInput {
    /* 需要的关键列：
            目标表名★
            目标表中文名
            目标字段顺序★
            目标字段名称★
            目标字段中文名称

            源表名★
            源表中文名
            源字段名称★
            源字段中文名称
            映射规则(sql)★
            映射规则说明(中文描述)

            过滤条件 ★

            JOIN方式 ★
            次源表名 ★
            次源表别名 ★
            ON条件 ★
     */
	
	public void aHive(Hive hive) {
		//指定本地SVN副本根目录
		String rootPath = "C:\\Users\\Administrator\\Desktop\\workbench\\projdoc_edw2";
		int startRow = 1;
		String yewu = hive.getYewu();
		String mapping = hive.getMapping();
		
		XlsxInput.createHiveScript(rootPath
                + "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\"+yewu
                , mapping, startRow);
	}
	
    public void hives() {
        //指定本地SVN副本根目录
        String rootPath = "C:\\Users\\Administrator\\Desktop\\workbench\\projdoc_edw2";
        int startRow = 1;
        //品种
        XlsxInput.createHiveScript(rootPath
                + "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\品种mapping.xlsx"
                , "股票", startRow);
        XlsxInput.createHiveScript(rootPath
                + "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\品种mapping.xlsx"
                , "基金", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\品种mapping.xlsx"
                , "债券", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\期货mapping.xlsx"
                , "期货", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\期权mapping.xlsx"
                , "Sheet3", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\其他类mapping.xlsx"
                , "其他类mapping", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\02 品种\\品种mapping\\融资类 产品mapping.xlsx"
                , "融资类", startRow);
        //主体
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\03 主体\\主体(客户)mapping.xlsx"
                , "mapping", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\03 主体\\主体（内部机构、经纪人）mapping.xlsx"
                , "Sheet1", startRow);
        //04 经纪业务
        XlsxInput.createHiveScript(rootPath +
                  "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\场外柜台mapping.xlsx"
                  , "场外柜台", startRow);
        //目标字段对应有误
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\法人清算mapping.xlsx"
                , "Sheet1", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\集中交易mapping.xlsx"
                , "集中交易", startRow);
        //格式有问题
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\金证CRM系统mapping.xlsx"
                , "金证CRM", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\金证产品中心mapping.xlsx"
                , "Mapping", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\期权系统mapping.xlsx"
                , "Sheet1", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\04 经纪业务\\mapping\\资金管理mapping.xlsx"
                , "Mapping", startRow);

        //信用
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\05 信用业务\\融资融券mapping.xlsx"
                , "融资融券", startRow);
        //自营
        //目标表字段有为空
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\06 自营业务\\O32自营业务mapping.xlsx"
                , "自营业务", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\06 自营业务\\海益（固收）mapping.xlsx"
                , "海益", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\06 自营业务\\恒生自营估值mapping.xlsx"
                , "Sheet1", startRow);
        //07 资管业务
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\07 资管业务\\TAmapping.xlsx"
                , "TA", startRow);
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\07 资管业务\\恒生资管估值mapping.xlsx"
                , "Sheet1", startRow);
        //统一账户
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\08 统一账户\\统一账户(账户).xlsx"
                , "用户", startRow);
        //做市
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\09 做市业务\\做市业务mapping.xlsx"
                , "MAPPING", startRow);

        //10 投行业务
        //格式有问题非标准模板
        XlsxInput.createHiveScript(rootPath +
                "\\02 项目实施\\0203 模型设计\\020303 逻辑模型设计\\10 投行业务\\投行MAPPING.xlsx"
                , "投行mapping", startRow);

    }
    
    
    
	public static void createHiveScript(String filePath,String sheetName,int startRow){
        Map<String, BlockSet> allBlockSet = null;
        try (InputStream inp = new FileInputStream(filePath)) {
            Workbook wb = WorkbookFactory.create(inp);
            Sheet sheet = wb.getSheet(sheetName);

            allBlockSet = getAll(startRow, sheet);
            
            // 生成Hive脚本
            String outputDir = URLDecoder.decode(XlsxInput.class.getClassLoader().getResource("").getPath(), "utf-8")
                    +"/hive_q";
            FileUtils.forceMkdir(new File(outputDir));
            HiveScriptOutput.output(outputDir, allBlockSet);

            //生成impala建表語句
            
            String createTabOutputDir = URLDecoder.decode(XlsxInput.class.getClassLoader().getResource("").getPath(), "utf-8")
                    +"/hive_iml_dll";
            FileUtils.forceMkdir(new File(createTabOutputDir));
            HiveCreateImlTableOutput.output(createTabOutputDir, allBlockSet);
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (EncryptedDocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        System.out.println("OK - " + allBlockSet.size());
	}
	


    public static Map<String, BlockSet> getAll(int startRow, Sheet sheet) {
        Map<String, BlockSet> allBlockSet = new HashMap<>();

        int prevStartRow = -1;
        int prevEndRow = -1;

        for (int row = startRow; row <= sheet.getLastRowNum() ; row++) {

            String tgtTableName = "";
            int tgtFieldPos;

            Cell tableCell = sheet.getRow(row).getCell(PoiHelper.TARGET_TABLE_NAME_INDEX);
            Cell posCell = sheet.getRow(row).getCell(PoiHelper.TARGET_FIELD_NUM_INDEX);

            if (null != tableCell && null != posCell) {
               tgtTableName =  tableCell.getStringCellValue();
               tgtFieldPos = (int)posCell.getNumericCellValue();
               //System.out.println(tgtTableName + "," + tgtFieldPos);
            } else {
                // 如果是空单元格则跳过
                continue;
            }

            // 如果是空数据则跳过
            if(null == tgtTableName || tgtTableName.trim().isEmpty()) {
                continue;
            }

            //如果当前行是新起点
            if(1 == tgtFieldPos) {
                //处理上一Block数据
                if(-1 != prevStartRow && -1 != prevEndRow) {
                    extractScript(allBlockSet, prevStartRow, prevEndRow, sheet);
                }

                prevStartRow = row;
            }

            prevEndRow = row;

        }

        //处理最后一个Block
        extractScript(allBlockSet, prevStartRow, prevEndRow, sheet);

        return allBlockSet;
    }

    private static void extractScript(Map<String, BlockSet> allBlockSet, int prevStartRow, int prevEndRow, Sheet sheet) {
        //获得TargetTable数据。输出：TargetTable
        TargetTable targetTable = extractTargetTable(sheet, prevStartRow, prevEndRow);
        //获得SourceTable数据。输出：SourceTable
        SourceTable sourceTable = extractSourceTable(sheet, prevStartRow, prevEndRow);
        //获得Join数据。List<Join>
        List<Join> joins = extractJoins(sheet, prevStartRow, prevEndRow);
        //获得Where数据。WhereCondition
        WhereCondition whereCondition = extractWhereCondition(sheet, prevStartRow, prevEndRow);
        //获得SelectFields数据。SelectFields
        SelectFields selectFields = extractSelectFields(sheet, prevStartRow, prevEndRow, sourceTable, joins);
        //没有groupby和having

        //组装为Block
        Block block = new Block();
        block.set_targetTable(targetTable);
        block.set_sourceTable(sourceTable);
        block.set_joins(joins);
        block.set_whereCondition(whereCondition);
        block.set_select(selectFields);

        //放入BlockSet
        String key = StringExtension.toStyleString(targetTable.get_tableName());
        if(allBlockSet.containsKey(key)) {
            allBlockSet.get(key).get_blocks().add(block);
        } else {
            List<Block> blocks = new ArrayList<Block>();
            blocks.add(block);

            BlockSet blockSet = new BlockSet();
            blockSet.set_name(key);
            blockSet.set_blocks(blocks);

            allBlockSet.put(key, blockSet);
        }
    }

    private static SelectFields extractSelectFields(Sheet sheet, int startRow, int endRow
            , SourceTable sourceTable, List<Join> joins) {
        SelectFields aSelectFields = null;
        List<SourceField> lstSourceFields = null;

        // 合成出两个字典
        // 字典1：key 表别名 value 表名
        HashMap<String, String> mapAsName = new HashMap<>();
        // 字典2：key 表名   value 表别名
        HashMap<String, String> mapTableName = new HashMap<>();

        if(sourceTable instanceof DirectTable) {
            DirectTable aDirectTable = (DirectTable)sourceTable;
            mapAsName.put(aDirectTable.get_asName().toLowerCase(), aDirectTable.get_tableName().toLowerCase());
            mapTableName.put(aDirectTable.get_tableName().toLowerCase(), aDirectTable.get_asName().toLowerCase());
        } else if (sourceTable instanceof ExpressionTable) {
            ExpressionTable aExpressionTable = (ExpressionTable)sourceTable;
            mapAsName.put(aExpressionTable.get_asName().toLowerCase(), aExpressionTable.get_expression().toLowerCase());
            mapTableName.put(aExpressionTable.get_expression().toLowerCase(), aExpressionTable.get_asName().toLowerCase());
        }

        for (Join aJoin : joins) {
            if(aJoin instanceof JoinExpressionTable) {
                JoinExpressionTable aJoinExpressionTable = (JoinExpressionTable)aJoin;
                mapAsName.put(aJoinExpressionTable.get_asName().toLowerCase(), aJoinExpressionTable.get_expression().toLowerCase());
                mapTableName.put(aJoinExpressionTable.get_expression().toLowerCase(), aJoinExpressionTable.get_asName().toLowerCase());
            } else if (aJoin instanceof JoinDirectTable){
                JoinDirectTable aJoinDirectTable = (JoinDirectTable)aJoin;
                mapAsName.put(aJoinDirectTable.get_asName().toLowerCase(), aJoinDirectTable.get_tableName().toLowerCase());
                mapTableName.put(aJoinDirectTable.get_tableName().toLowerCase(), aJoinDirectTable.get_asName().toLowerCase());
            }
        }


        for (int i = startRow; i <= endRow; i++) {
            if(i == startRow) {
                aSelectFields = new SelectFields();
                lstSourceFields = new ArrayList<>();
            }

            Row aRow = sheet.getRow(i);

            String mapping_expression = aRow.getCell(PoiHelper.MAPPING_EXPRESSION_INDEX
                                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
            if(null != mapping_expression && !mapping_expression.trim().isEmpty()) {
                // 填写了映射规则的，根据填写内容判断
                // 如果是xx.xxxx格式的
                String[] arr = mapping_expression.split("[.]");
                if(mapping_expression.contains(".") && arr.length == 2
                        && !mapping_expression.contains("(")
                        && !mapping_expression.contains("=")
                        && !mapping_expression.contains(">")
                        && !mapping_expression.contains("<")) {
                    // 放入直抽字段
                    String tableAsName = arr[0];
                    String fieldName = arr[1];

                    DirectField aDirectField = new DirectField();
                    aDirectField.set_fieldName(fieldName);
                    aDirectField.set_tableAsName(tableAsName);

                    String fieldCnName = aRow.getCell(PoiHelper.SOURCE_FIELD_CN_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    aDirectField.set_fieldCnName(fieldCnName);

                    int fieldNum = (int)aRow.getCell(PoiHelper.TARGET_FIELD_NUM_INDEX).getNumericCellValue();
                    aDirectField.set_index(fieldNum);

                    String fieldAsName = aRow.getCell(PoiHelper.TARGET_FIELD_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    aDirectField.set_fieldAsName(fieldAsName);

                    lstSourceFields.add(aDirectField);
                } else {
                    // 如果不是，放入表达式字段
                    ExpressionField aExpressionField = new ExpressionField();
                    aExpressionField.set_expression(mapping_expression);
                    //aExpressionField.set_directFields(null);

                    String fieldCnName = aRow.getCell(PoiHelper.SOURCE_FIELD_CN_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    String mapping_comment = aRow.getCell(PoiHelper.MAPPING_COMMENT_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();

                    if(null != mapping_comment && !mapping_comment.isEmpty()) {
                        aExpressionField.set_fieldCnName(mapping_comment);
                    } else {
                        aExpressionField.set_fieldCnName(fieldCnName);
                    }

                    int fieldNum = (int)aRow.getCell(PoiHelper.TARGET_FIELD_NUM_INDEX).getNumericCellValue();
                    aExpressionField.set_index(fieldNum);

                    String fieldAsName = aRow.getCell(PoiHelper.TARGET_FIELD_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    aExpressionField.set_fieldAsName(fieldAsName);

                    lstSourceFields.add(aExpressionField);
                }

            } else {
                // 没有填写映射规则的，都是直抽字段
                String fieldName = aRow.getCell(PoiHelper.SOURCE_FIELD_NAME_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                if(null != fieldName && !fieldName.trim().isEmpty()) {
                    DirectField aDirectField = new DirectField();
                    aDirectField.set_fieldName(fieldName);

                    String tableName = aRow.getCell(PoiHelper.SOURCE_TABLE_CN_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    String tableAsName = mapTableName.get(tableName);
                    if (null != tableAsName) {
                        aDirectField.set_tableAsName(tableAsName);
                    }

                    String fieldCnName = aRow.getCell(PoiHelper.SOURCE_FIELD_CN_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    aDirectField.set_fieldCnName(fieldCnName);

                    int fieldNum = (int)aRow.getCell(PoiHelper.TARGET_FIELD_NUM_INDEX).getNumericCellValue();
                    aDirectField.set_index(fieldNum);

                    String fieldAsName = aRow.getCell(PoiHelper.TARGET_FIELD_NAME_INDEX
                            , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                    aDirectField.set_fieldAsName(fieldAsName);

                    lstSourceFields.add(aDirectField);
                }
            }

        }





        if(null != aSelectFields) {
            aSelectFields.set_fields(lstSourceFields);
        }

        return aSelectFields;
    }

    private static WhereCondition extractWhereCondition(Sheet sheet, int startRow, int endRow) {
        WhereCondition aWhereCondition = null;

        for (int i = startRow; i <= endRow ; i++) {
            Row aRow = sheet.getRow(i);
            String where = aRow.getCell(PoiHelper.WHERE_CONDITION_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
            if (null != where && !where.trim().isEmpty()) {
                aWhereCondition = new WhereCondition();
                aWhereCondition.set_expression(where);
                //aWhereCondition.set_conditionDetails("");Excel模式暂时不解析
                break;
            }
        }

        return aWhereCondition;
    }

    private static List<Join> extractJoins(Sheet sheet, int startRow, int endRow) {
        List<Join> lstJoins = null;

        for (int i = startRow; i <= endRow; i++) {
            if(i == startRow) {
                lstJoins = new ArrayList<>();
            }

            Row aRow = sheet.getRow(i);

            String joinType = aRow.getCell(PoiHelper.JOIN_TYPE_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
            JoinType aJoinType = JoinType.parseJoinType(joinType);
            if (null != aJoinType) {
                String tableName = aRow.getCell(PoiHelper.JOIN_TABLE_NAME_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                String asName = aRow.getCell(PoiHelper.JOIN_TABLE_AS_NAME_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                String onCondition = aRow.getCell(PoiHelper.JOIN_CONDITION_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();

                if(tableName.toLowerCase().contains("select") && tableName.toLowerCase().contains("from")) {
                    // 如果Join表是复杂表
                    JoinExpressionTable aJoinExpressionTable = new JoinExpressionTable();
                    aJoinExpressionTable.set_joinType(aJoinType);
                    aJoinExpressionTable.set_expression(tableName);
                    aJoinExpressionTable.set_asName(asName);
                    //aJoinExpressionTable.set_blockNode(null); Excel模式暂时不解析
                    JoinCondition aJoinCondition = new JoinCondition();
                    aJoinCondition.set_expression(onCondition);
                    //aJoinCondition.set_conditionDetails(); Excel模式暂时不解析
                    aJoinExpressionTable.set_Condition(aJoinCondition);
                    //aJoinExpressionTable.set_tableCnName(""); Excel模式暂时不解析
                    lstJoins.add(aJoinExpressionTable); //逆变
                } else {
                    // 如果Join表是简单表
                    JoinDirectTable aJoinDirectTable = new JoinDirectTable();
                    aJoinDirectTable.set_joinType(aJoinType);
                    aJoinDirectTable.set_schema(PoiHelper.SOURCE_SCHEMA);
                    aJoinDirectTable.set_tableName(tableName);
                    aJoinDirectTable.set_asName(asName);
                    JoinCondition aJoinCondition = new JoinCondition();
                    aJoinCondition.set_expression(onCondition);
                    //aJoinCondition.set_conditionDetails(); Excel模式暂时不解析
                    aJoinDirectTable.set_Condition(aJoinCondition);
                    //aJoinDirectTable.set_tableCnName(""); Excel模式暂时不解析
                    lstJoins.add(aJoinDirectTable); //逆变
                }
            }
        }

        return lstJoins;
    }

    private static SourceTable extractSourceTable(Sheet sheet, int startRow, int endRow) {
        SourceTable aSourceTable = null;

        for (int i = startRow; i <= endRow ; i++) {
            Row aRow = sheet.getRow(i);
            String joinType = aRow.getCell(PoiHelper.JOIN_TYPE_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
            // 没写join类型，但是写了表名的，就是主表
            if(null == joinType || joinType.trim().isEmpty()) {
                String srcTable = aRow.getCell(PoiHelper.JOIN_TABLE_NAME_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue();
                if(null != srcTable && !srcTable.trim().isEmpty()) {
                    // 如果包含select和from则为复杂表 TODO： （后续改正则）
                    if(srcTable.toLowerCase().contains("select") && srcTable.toLowerCase().contains("from")) {
                        // 如果主表是复杂表
                        ExpressionTable aExpressionTable = new ExpressionTable();
                        aExpressionTable.set_expression(aRow.getCell(PoiHelper.JOIN_TABLE_NAME_INDEX
                                , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
                        aExpressionTable.set_asName(aRow.getCell(PoiHelper.JOIN_TABLE_AS_NAME_INDEX
                                , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
                        //aExpressionTable.set_tableCnName(""); Excel模式暂时不解析
                        //aExpressionTable.set_blockNode(null); Excel模式暂时不解析
                        aSourceTable = aExpressionTable; //装箱
                        break;
                    } else {
                        // 如果主表是直接表
                        DirectTable aDirectTable = new DirectTable();
                        aDirectTable.set_schema(PoiHelper.SOURCE_SCHEMA);
                        aDirectTable.set_tableName(aRow.getCell(PoiHelper.JOIN_TABLE_NAME_INDEX
                                , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
                        aDirectTable.set_asName(aRow.getCell(PoiHelper.JOIN_TABLE_AS_NAME_INDEX
                                , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
                        //aDirectTable.set_tableCnName(""); Excel模式暂时不解析
                        aSourceTable = aDirectTable; //装箱
                        break;
                    }
                }
            }

        }

        return aSourceTable;
    }

    private static TargetTable extractTargetTable(Sheet sheet, int startRow, int endRow) {
        TargetTable aTargetTable = null;
        List<TargetField> lstTargetField = null;

        Row aRow = null;
        for (int i = startRow ; i <= endRow ; i++) {
            aRow = sheet.getRow(i);

            if( i == startRow) {
                aTargetTable = new TargetTable();
                lstTargetField = new ArrayList<>();

                aTargetTable.set_schema(PoiHelper.TARGET_SCHEMA);
                aTargetTable.set_tableName(aRow.getCell(PoiHelper.TARGET_TABLE_NAME_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
                aTargetTable.set_tableCnName(aRow.getCell(PoiHelper.TARGET_TABLE_CN_NAME_INDEX
                        , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
            }

            TargetField aTargetField = new TargetField();
            aTargetField.set_fieldName(aRow.getCell(PoiHelper.TARGET_FIELD_NAME_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
            aTargetField.set_fieldCnName(aRow.getCell(PoiHelper.TARGET_FIELD_CN_NAME_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
            aTargetField.set_index((int)aRow.getCell(PoiHelper.TARGET_FIELD_NUM_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getNumericCellValue());
            aTargetField.set_fileType(aRow.getCell(PoiHelper.TARGET_FIELD_TYPE_INDEX
                    , Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).getStringCellValue());
            lstTargetField.add(aTargetField);
        }

        if(null != aTargetTable) {
            aTargetTable.set_fields(lstTargetField);
        }

        return aTargetTable;
    }
}
