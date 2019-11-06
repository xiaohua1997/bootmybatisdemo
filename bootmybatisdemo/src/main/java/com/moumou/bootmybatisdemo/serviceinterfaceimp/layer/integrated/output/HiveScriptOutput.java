package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.output;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.integrated.input.PoiHelper;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.Block;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.BlockSet;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.WhereCondition;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.DirectField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.ExpressionField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.SourceField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.target.TargetField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source.*;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.target.TargetTable;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

public class HiveScriptOutput {
    public static final String EOL = "\n";
    public static final String CHARSET_NAME = "UTF-8";

    public static void output(String target_dir, Map<String, BlockSet> allBlockSet) throws IOException {
        // 获得所有BlockSet
        for (String tgtTableName : allBlockSet.keySet()) {
            StringBuffer sb = new StringBuffer();

            BlockSet aBlockSet = allBlockSet.get(tgtTableName);
            // 一个BlockSet一个脚本
            // 写入hive配置语句
            sb.append("set hivevar:targetdbname=").append(PoiHelper.SOURCE_SCHEMA).append(";").append(EOL);
            sb.append("set hivevar:sourcedbname=").append(PoiHelper.TARGET_SCHEMA).append(";").append(EOL);
            sb.append("use ${hivevar:targetdbname};").append(EOL);
            sb.append(EOL);
            sb.append("--不启用锁特性").append(EOL);
            sb.append("set hive.support.concurrency=false;").append(EOL);
            sb.append("--动态分区严格模式").append(EOL);
            sb.append("set hive.exec.dynamic.partition.mode=strict;").append(EOL);
            sb.append("--日常跑批脚本关闭动态分区特性").append(EOL);
            sb.append("set hive.exec.dynamic.partition=false;").append(EOL);
            sb.append("--不进行parquet压缩").append(EOL);
            sb.append("set parquet.compression=uncompressed;").append(EOL);
            sb.append(EOL);
            sb.append("alter table ");
            sb.append(PoiHelper.TARGET_SCHEMA).append(".").append(tgtTableName);
            sb.append(" drop if exists partition(").append("part_ymd='${hivevar:batch_date}'").append(");").append(EOL);
            List<Block> blocks = aBlockSet.get_blocks();
            for (Block block : blocks) {
                sb.append(EOL);
                sb.append(EOL);
                // 写insert into部分
                TargetTable aTargetTable = block.get_targetTable();

                sb.append("insert into table ");
                if(null != aTargetTable.get_schema()) {
                    sb.append(aTargetTable.get_schema()).append(".");
                }
                sb.append(aTargetTable.get_tableName());
                sb.append(" partition (").append("part_ymd='${hivevar:batch_date}'").append(")").append(EOL);
                List<TargetField> lstTargetField = aTargetTable.get_fields();
                if(null != lstTargetField && lstTargetField.size() > 0) {
                	lstTargetField = HiveCreateImlTableOutput.removeDuplicateField(lstTargetField);
                	lstTargetField = HiveCreateImlTableOutput.SortField(lstTargetField);
                    sb.append("(").append(EOL);
                    sb.append("data_date").append(EOL);
                    for (int i = 0; i < lstTargetField.size(); i++) {
                        TargetField aTargetField = lstTargetField.get(i);
//                        if(0 == i) {
//                            sb.append(" ");
//                        } else {
//                            sb.append(",");
//                        }
                        sb.append(",");
                        sb.append(aTargetField.get_fieldName());

                        if(null != aTargetField.get_fieldCnName()) {
                            sb.append(" --").append(aTargetField.get_fieldCnName().replaceAll("[\\t\\n\\r]", ""));
                        }

                        sb.append(EOL);
                    }
                    sb.append(",etl_time").append(EOL);
                    sb.append(")").append(EOL);
                }
                // 写select部分
                sb.append("select").append(EOL);
                List<SourceField> lstSourceField = block.get_select().get_fields();
                sb.append("'${batch_date}'").append(EOL);
                for (int i = 0; i < lstSourceField.size(); i++) {
//                    if(0 == i) {
//                        sb.append("'${batch_date}'");
//                    } else {
//                        sb.append(",");
//                    }
                	sb.append(",");
                    SourceField aSourceField = lstSourceField.get(i);
                    if(aSourceField instanceof DirectField) {
                        DirectField aDirectField = (DirectField)aSourceField;
                        if(null != aDirectField.get_tableAsName()) {
                            sb.append(aDirectField.get_tableAsName()).append(".");
                       }
                        sb.append(aDirectField.get_fieldName());
   
                        if(null != aDirectField.get_fieldAsName()) {
                            sb.append(" as ").append(aDirectField.get_fieldAsName());
                        }
                        if(null != aDirectField.get_fieldCnName()) {
                            sb.append(" --").append(aDirectField.get_fieldCnName().replaceAll("[\\t\\n\\r]", ""));
                        }
                    } else if (aSourceField instanceof ExpressionField) {
                        ExpressionField aExpressionField = (ExpressionField)aSourceField;
                        sb.append(aExpressionField.get_expression());
                        if(null != aExpressionField.get_fieldAsName()) {
                            sb.append(" as ").append(aExpressionField.get_fieldAsName().replaceAll("[\\t\\n\\r]", ""));
                        }
                        if (null != aExpressionField.get_fieldCnName()) {
                            sb.append(" --").append(aExpressionField.get_fieldCnName().replaceAll("[\\t\\n\\r]", ""));
                        }
                    }
                    
                    sb.append(EOL);
                }
                sb.append(",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_time").append(EOL);
                
                // 写from部分
                sb.append("from ");
                SourceTable aSourceTable = block.get_sourceTable();
                if(aSourceTable instanceof DirectTable) {
                    DirectTable aDirectTable = (DirectTable)aSourceTable;
                    if(null != aDirectTable.get_schema()) {
                        sb.append(aDirectTable.get_schema()).append(".");
                    }
                    sb.append(aDirectTable.get_tableName());
                    if(null != aDirectTable.get_asName()) {
                        sb.append(" ").append(aDirectTable.get_asName());
                    }
                    if (null != aDirectTable.get_tableCnName()) {
                        sb.append(" --").append(aDirectTable.get_tableCnName());
                    }
                } else if (aSourceTable instanceof ExpressionTable) {
                    ExpressionTable aExpressionTable = (ExpressionTable)aSourceTable;
                    sb.append("(").append(aExpressionTable.get_expression()).append(")");
                    if(null != aExpressionTable.get_asName()) {
                        sb.append(" ").append(aExpressionTable.get_asName());
                    }
                    if(null != aExpressionTable.get_tableCnName()) {
                        sb.append(" --").append(aExpressionTable.get_tableCnName().replaceAll("[\\t\\n\\r]", ""));
                    }
                }
                sb.append(EOL);
                // 写join部分
                List<Join> lstJoin = block.get_joins();
                if(null != lstJoin && lstJoin.size() > 0) {
                    for(Join aJoin : lstJoin) {
                        switch (aJoin.get_joinType()) {
                            case INNER_JOIN:
                                sb.append("inner join ");
                                break;
                            case LEFT_OUTER_JOIN:
                                sb.append("left join ");
                                break;
                            case RIGHT_OUTER_JOIN:
                                sb.append("right join ");
                                break;
                            case FULL_OUTER_JOIN:
                                sb.append("full join ");
                                break;
                            case CROSS_JOIN:
                                sb.append("cross join ");
                                break;
                            default:
                                break;
                        }

                        if(aJoin instanceof JoinDirectTable) {
                            JoinDirectTable aJoinDirectTable = (JoinDirectTable)aJoin;
                            if(null != aJoinDirectTable.get_schema()) {
                                sb.append(aJoinDirectTable.get_schema()).append(".");
                            }
                            sb.append(aJoinDirectTable.get_tableName());
                            if(null != aJoinDirectTable.get_asName()) {
                                sb.append(" ").append(aJoinDirectTable.get_asName());
                            }
                            if(null != aJoinDirectTable.get_tableCnName()) {
                                sb.append(" --").append(aJoinDirectTable.get_tableCnName().replaceAll("[\\t\\n\\r]", ""));
                            }
                            sb.append(EOL);
                        } else if (aJoin instanceof JoinExpressionTable) {
                            JoinExpressionTable aJoinExpressionTable = (JoinExpressionTable)aJoin;
                            sb.append("(").append(aJoinExpressionTable.get_expression()).append(")");
                            if(null != aJoinExpressionTable.get_asName()) {
                                sb.append(" ").append(aJoinExpressionTable.get_asName());
                            }
                            if(null != aJoinExpressionTable.get_tableCnName()) {
                                sb.append(" --").append(aJoinExpressionTable.get_tableCnName().replaceAll("[\\t\\n\\r]", ""));
                            }
                        }

                        if(null != aJoin.get_Condition()) {
                            sb.append(" on ").append(aJoin.get_Condition().get_expression());
                            sb.append(EOL);
                        }
                    }
                }
                // 写where部分
                WhereCondition aWhereCondition =  block.get_whereCondition();
                if(null != aWhereCondition) {
                    sb.append("where ").append(aWhereCondition.get_expression());
                    sb.append(EOL);
                }
                // 写groupby部分
                // 写having部分
                sb.append(";");
            }
            //输出文件
            String fileName = tgtTableName;
            File file = new File(target_dir + File.separator + fileName + ".q");
            String newFileContent = sb.toString()
                    .replaceAll("\\$\\{batch_date\\}", Matcher.quoteReplacement("${hivevar:batch_date}"));
            FileUtils.writeStringToFile(file, newFileContent, CHARSET_NAME);
        }

    }
}
