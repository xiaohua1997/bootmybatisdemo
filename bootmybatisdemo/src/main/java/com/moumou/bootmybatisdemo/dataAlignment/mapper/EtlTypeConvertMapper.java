package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvertCopy;

@Mapper
public interface EtlTypeConvertMapper {
	
    //查询
	@Select("select * from etl_type_convert order by src_column_type")
	List<EtlTypeConvert> queryEtlConvert();
	
	//条件查询
    @Select(
    	    "<script> SELECT * from etl_type_convert "
    	        + "WHERE 1=1"
    	        + "<if test='null!=#{srcDbType}'> and src_db_type like concat('%',#{srcDbType},'%') </if> "
    	        + "<if test='null!=#{srcColumnType}'> and src_column_type like concat('%',#{srcColumnType},'%') </if> "
    	        + "<if test='null!=#{tgtDbType}'> and tgt_db_type like concat('%',#{tgtDbType},'%') </if> "
    	        + "<if test='null!=#{tgtColumnType}'> and tgt_column_type like concat('%',#{tgtColumnType},'%') </if> "
    	        + "order by src_column_type " 
    	        + "</script>")
    //@ResultMap("com.assessmentTargetAssPsndocResult")
    List<EtlTypeConvert> conditionQueryEtlTypeConvert(EtlTypeConvert etlTypeConvert);
	
	//修改
	@Update("UPDATE \n" +
            "etl_type_convert \n" +
            "SET \n" +
            "src_db_type=#{srcDbType1}, \n" +
            "src_column_type=#{srcColumnType1}, \n" +
            "tgt_db_type=#{tgtDbType1}, \n" +
            "tgt_column_type=#{tgtColumnType1}, \n" +
            "tgt_column_big_type=#{tgtColumnBigType1}, \n" +
            "tgt_column_length=#{tgtColumnLength1}, \n" +
            "tgt_column_default=#{tgtColumnDefault1}, \n" +
            "tgt_column_format=#{tgtColumnFormat1}, \n" +
            "convert_mode=#{convertMode1} \n" +
            "WHERE \n" +
            "src_db_type=#{srcDbType} and \n" +
            "src_column_type=#{srcColumnType} and \n" +
            "tgt_db_type=#{tgtDbType} and \n" +
            "tgt_column_type=#{tgtColumnType} and \n" +
            "tgt_column_big_type=#{tgtColumnBigType} and \n" +
            "tgt_column_length=#{tgtColumnLength} and \n" +
            "tgt_column_default=#{tgtColumnDefault} and \n" +
            "tgt_column_format=#{tgtColumnFormat} and \n" +
            "convert_mode=#{convertMode}")
	int updateEtlConvert(EtlTypeConvert etlTypeConvert,EtlTypeConvert etlTypeConvert1,EtlTypeConvertCopy etlTypeConvertCopy);
	
	//添加
	@Insert("insert into etl_type_convert(\n" +
            "src_db_type,\n" +
            "src_column_type,\n" +
            "tgt_db_type,\n" +
            "tgt_column_type,\n" +
            "tgt_column_big_type,\n" +
            "tgt_column_length,\n" +
            "tgt_column_default,\n" +
            "tgt_column_format,\n" +
            "convert_mode \n" +
            ")\n" +
            "values(\n" +
            "#{srcDbType},\n" +
            "#{srcColumnType},\n" +
            "#{tgtDbType},\n" +
            "#{tgtColumnType},\n" +
            "#{tgtColumnBigType},\n" +
            "#{tgtColumnLength},\n" +
            "#{tgtColumnDefault},\n" +
            "#{tgtColumnFormat},\n" +
            "#{convertMode} \n" +
            ")")
    int addEtlConvert(EtlTypeConvert etlTypeConvert);
	
   //测试不调用这个删除（实际要用的删除方法）
  @Delete("delete from etl_type_convert where src_db_type =#{srcDbType} and src_column_type =#{srcColumnType} and tgt_db_type = #{tgtDbType} and tgt_column_type =#{tgtColumnType} and tgt_column_big_type =#{tgtColumnBigType} and tgt_column_length =#{tgtColumnLength} and tgt_column_default =#{tgtColumnDefault} and tgt_column_format =#{tgtColumnFormat} and convert_mode =#{convertMode}")
  int delEtlConvert(EtlTypeConvert etlTypeConvert);
  //测试删除方法（置无效）
//  @Update("update etl_type_convert set invalid = now() where src_db_type =#{srcDbType} and src_column_type =#{srcColumnType} and tgt_db_type = #{tgtDbType} and tgt_column_type =#{tgtColumnType} and tgt_column_big_type =#{tgtColumnBigType} and tgt_column_length =#{tgtColumnLength} and tgt_column_default =#{tgtColumnDefault} and tgt_column_format =#{tgtColumnFormat} and convert_mode =#{convertMode}")
//  boolean delEtlConvert(EtlTypeConvert etlTypeConvert);
}
