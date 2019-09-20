package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.moumou.bootmybatisdemo.dataAlignment.model.EtlColumnConvert;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;

@Mapper
public interface EtlColumnConvertMapper {
	
	//查询
	@Select("select * from etl_column_convert")
	List<EtlColumnConvert> queryEtlColumnCon();
	
	//修改
	@Update("UPDATE \n" +
            "etl_column_convert \n" +
            "SET \n" +
            "src_column=#{srcColumn}, \n" +
            "tgt_column=#{tgtColumn}, \n" +
            "table_name=#{tableName}, \n" +
            "sys=#{sys}, \n" +
            "WHERE \n" +
            "src_column=#{srcColumn} and \n" +
            "tgt_column=#{tgtColumn} and \n" +
            "table_name=#{tableName} and \n" +
            "sys=#{sys}")
	int updateEtlColumnCon(EtlColumnConvert etlColumnConvert);
	
	//添加
	@Insert("insert into etl_column_convert(\n" +
            "src_column,\n" +
            "tgt_column,\n" +
            "table_name,\n" +
            "sys,\n" +
            ")\n" +
            "values(\n" +
            "#{srcColumn},\n" +
            "#{tgtColumn},\n" +
            "#{tableName},\n" +
            "#{sys},\n" +
            ")")
    int addEtlColumnCon(EtlColumnConvert etlColumnConvert);

	//测试不调用这个删除（实际要用的删除方法）
  @Delete("delete from etl_column_convert where src_column =#{srcColumn} and tgt_column =#{tgtColumn} and table_name = #{tableName} and sys =#{sys}")
  int delEtlColumnCon(EtlColumnConvert etlColumnConvert);
  //测试删除方法（置无效）
//  @Update("update etl_column_convert set invalid = now() where src_column =#{srcColumn} and tgt_column =#{tgtColumn} and table_name = #{tableName} and sys =#{sys}")
//  boolean delEtlColumnCon(EtlColumnConvert etlColumnConvert);
}
