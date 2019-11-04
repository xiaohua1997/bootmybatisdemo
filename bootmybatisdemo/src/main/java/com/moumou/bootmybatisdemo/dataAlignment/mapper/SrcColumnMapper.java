package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface SrcColumnMapper {

    //查询（全量）
    @Select("select * from src_column ")
    List<SrcColumn> queryDBS();

  //条件查询
    @Select(
    	    "<script> SELECT * from src_column "
    	        + "WHERE 1=1"
    	        + "<if test='null!=#{sys}'> and sys like concat('%',#{sys},'%') </if> "
    	        + "<if test='null!=#{dbSid}'> and db_sid like concat('%',#{dbSid},'%') </if> "
    	        + "<if test='null!=#{tableName}'> and table_name like concat('%',#{tableName},'%') </if> "
    	        + "<if test='null!=#{columnName}'> and column_name like concat('%',#{columnName},'%') </if> "
    	        /*+ "LIMIT #{pageindex},#{pagenum} " */
    	        + "</script>")
    //@ResultMap("com.assessmentTargetAssPsndocResult")
    List<SrcColumn> conditionQuerySrcColumn(SrcColumn srcColumn);
    
    //修改
    @Update("UPDATE \n" +
            "src_column \n" +
            "SET \n" +
            "column_id=#{columnId}, \n" +
            "column_type=#{columnType}, \n" +
            "column_cn_name=#{columnCnName}, \n" +
            "is_pk=#{isPk}, \n" +
            "not_null=#{notNull}, \n" +
            "default_value=#{defaultValue}, \n" +
            "is_dk=#{isDk}, \n" +
            "break_flag=#{breakFlag}\n" +
            "WHERE \n" +
            "sys=#{sys} and \n" +
            "db_sid=#{dbSid} and \n" +
            "db_schema=#{dbSchema} and \n" +
            "table_name=#{tableName} and \n" +
            "column_name=#{columnName}")
    int uptateSrcColumn(SrcColumn srcColumn);

    /*增加*/
    @Insert("insert into src_column(\n" +
            "sys,\n" +
            "db_sid,\n" +
            "db_schema,\n" +
            "table_name,\n" +
            "column_id,\n" +
            "column_name,\n" +
            "column_type,\n" +
            "column_cn_name,\n" +
            "is_pk,\n" +
            "not_null,\n" +
            "default_value,\n" +
            "is_dk,\n" +
            "break_flag\n" +
            ")\n" +
            "values(\n" +
            "#{sys},\n" +
            "#{dbSid},\n" +
            "#{dbSchema},\n" +
            "#{tableName},\n" +
            "#{columnId},\n" +
            "#{columnName},\n" +
            "#{columnType},\n" +
            "#{columnCnName},\n" +
            "#{isPk},\n" +
            "#{notNull},\n" +
            "#{defaultValue},\n" +
            "#{isDk},\n" +
            "#{breakFlag}\n" +
            ")")
    int addSrcColumn(SrcColumn srcColumn);

    //不调用这个删除
    @Delete("delete from src_column where sys =#{sys} and db_sid = #{dbSid} and db_schema =#{dbSchema} and table_name =#{tableName} and column_id =#{columnId} and column_name =#{columnName} and column_type =#{columnType} and column_cn_name =#{columnCnName} and is_pk =#{isPk} and not_null =#{notNull} and default_value =#{defaultValue} and is_dk =#{isDk} and break_flag =#{breakFlag}")
    int delSrcColumn(SrcColumn srcColumn);
    //删除（置无效）
//    @Update("update src_column set invalid = now() where sys = #{sys} and sys_num = #{sysNum} and db_sid = #{dbSid} and db_schema = #{dbSchema}")
//    boolean delSrcColumn(SrcColumn srcColumn);
}
