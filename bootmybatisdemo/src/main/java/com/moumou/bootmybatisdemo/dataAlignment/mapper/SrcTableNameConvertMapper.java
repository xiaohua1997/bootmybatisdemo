package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTableNameConvert;

@Mapper
public interface SrcTableNameConvertMapper {
	
    //查询
	@Select("select * from src_tablename_convert ")
	List<SrcTableNameConvert> querySrcTableNC();
	
	//修改
	@Update("UPDATE \n" +
            "src_tablename_convert \n" +
            "SET \n" +
            "src_table_name=#{srcTableName}, \n" +
            "tgt_table_name=#{tgtTableName}, \n" +
            "sys=#{sys}, \n" +
            "remark=#{remark} \n" +
            "WHERE \n" +
            "src_table_name=#{srcTableName} and \n" +
            "tgt_table_name=#{tgtTableName} and \n" +
            "sys=#{sys}" )
	int updateSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
	
	//增加
	@Insert("insert into src_tablename_convert(\n" +
            "src_table_name,\n" +
            "tgt_table_name,\n" +
            "sys,\n" +
            "remark \n" +
            ")\n" +
            "values(\n" +
            "#{srcTableName},\n" +
            "#{tgtTableName},\n" +
            "#{sys},\n" +
            "#{remark} \n" +
            ")")
    int addSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
	
	//测试不调用这个删除（实际要用的删除方法）
    @Delete("delete from src_tablename_convert where src_table_name =#{srcTableName} and tgt_table_name =#{tgtTableName} and sys = #{sys} and remark =#{remark}")
    int delSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
	//测试删除方法(置无效)
//	@Update("update src_tablename_convert set invalid = now() where src_table_name = #{srcTableName} and tgt_table_name = #{tgtTableName} and sys = #{sys} and remark = #{remark}")
//    boolean delSrcTableNameConvert(SrcTableNameConvert srcTableNameConvert);
}
