package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;

@Mapper
public interface SrcTableMapper {
    //查询（全量）
    @Select("select * from src_table ")
    List<SrcTable> queryDBS();
    //条件查询
    @Select(
    	    "<script> SELECT * from src_table "
    	        + "WHERE 1=1"
    	        + "<if test=\"null!=sys\"> and sys like \"% #{sys} %\" </if> "
    	        + "<if test=\"null!=sid\"> and db_sid \"% #{sid} %\" </if> "
    	        + "<if test=\"null!=tablename\"> and table_name \"% #{tablename} %\" </if> "
    	        /*+ "LIMIT #{pageindex},#{pagenum} " */
    	        + "</script>")
    //@ResultMap("com.assessmentTargetAssPsndocResult")
    List<SrcTable> conditionQuerySrcTable(String sys, String sid, String tablename);
    
    //修改
    @Update("UPDATE\n" +
            "src_table\n" +
            "SET\n" +
            "table_cn_name=#{tableCnName},\n" +
            "inc_cdt=#{incCdt},\n" +
            "if_mark=#{ifMark},\n" +
            "table_type=#{tableType},\n" +
            "template_code=#{templateCode},\n" +
            "is_put_to_etldb=#{isPutToEtldb}\n" +
            "WHERE \n" +
            "sys=#{sys} and \n" +
            "db_sid=#{dbSid} and \n" +
            "table_schema=#{tableSchema} and \n" +
            "table_name=#{tableName}\n")
    int uptateSrcTable(SrcTable srcTable);

    /*增加*/
    @Insert("INSERT INTO\n" +
            "src_table(\n" +
            "sys,\n" +
            "db_sid,\n" +
            "table_schema,\n" +
            "table_name,\n" +
            "table_cn_name,\n" +
            "inc_cdt,\n" +
            "if_mark,\n" +
            "table_type,\n" +
            "template_code,\n" +
            "is_put_to_etldb\n" +
            ")VALUES(\n" +
            "#{sys},\n" +
            "#{dbSid},\n" +
            "#{tableSchema},\n" +
            "#{tableName},\n" +
            "#{tableCnName},\n" +
            "#{incCdt},\n" +
            "#{ifMark},\n" +
            "#{tableType},\n" +
            "#{templateCode},\n" +
            "#{isPutToEtldb}\n" +
            ")")
    int addSrcTable(SrcTable srcTable);

  //不调用这个删除
  @Delete("delete from src_table where sys =#{sys} and db_sid =#{dbSid} and table_schema = #{tableSchema} and table_name =#{tableName} and table_cn_name =#{tableCnName} and inc_cdt =#{incCdt} and if_mark =#{ifMark} and table_type =#{tableType} and template_code =#{templateCode} and is_put_to_etldb =#{isPutToEtldb}")
  int delSrcTable(SrcTable srcTable);
  //删除（置无效）
//  @Update("update src_table set invalid = now() where sys =#{sys} and db_sid =#{dbSid} and table_schema = #{tableSchema} and table_name =#{tableName} and table_cn_name =#{tableCnName} and inc_cdt =#{incCdt} and if_mark =#{ifMark} and table_type =#{tableType} and template_code =#{templateCode} and is_put_to_etldb =#{isPutToEtldb}")
//  boolean delSrcTable(SrcTable srcTable);
}
