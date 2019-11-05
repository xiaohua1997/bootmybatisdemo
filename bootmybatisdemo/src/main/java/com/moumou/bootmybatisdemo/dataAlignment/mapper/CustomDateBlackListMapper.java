package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.moumou.bootmybatisdemo.dataAlignment.model.CustomDateBlackList;
import com.moumou.bootmybatisdemo.dataAlignment.model.EtlTypeConvert;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcColumn;
import com.moumou.bootmybatisdemo.dataAlignment.model.SrcTable;

@Mapper
public interface CustomDateBlackListMapper {
	
	//查询
	@Select("select * from custom_date_black_list")
	List<CustomDateBlackList> queryCustomDateBlackList();

	//条件查询
    @Select(
    	    "<script> SELECT * from custom_date_black_list "
    	        + "WHERE 1=1"
    	        + "<if test='null!=#{_batch_date}'> and batch_date like concat('%',#{_batch_date},'%') </if> "
    	        + "<if test='null!=#{_comment}'> and comment like concat('%',#{_comment},'%') </if> "
    	        /*+ "LIMIT #{pageindex},#{pagenum} " */
    	        + "</script>")
    //@ResultMap("com.assessmentTargetAssPsndocResult")
    List<CustomDateBlackList> conditionCustomDateBlackList(CustomDateBlackList customDateBlackList);
    
    //添加
  	@Insert("insert into custom_date_black_list(\n" +
              "batch_date,\n" +
              "comment \n" +
              ")\n" +
              "values(\n" +
              "#{_batch_date},\n" +
              "#{_comment} \n" +
              ")")
      int addCustomDateBlackList(CustomDateBlackList customDateBlackList);
  	
    //修改
    @Update("UPDATE \n" +
            "custom_date_black_list \n" +
            "SET \n" +
            "batch_date=#{_batch_date},\n" +
            "comment=#{_comment}\n" +
            "WHERE \n" +
            "batch_date=#{_batch_date}")
    int uptateCustomDateBlackList(CustomDateBlackList customDateBlackList);
  	
    //删除
    @Delete("delete from custom_date_black_list where batch_date=#{_batch_date} and comment=#{_comment}")
    int delCustomDateBlackList(CustomDateBlackList customDateBlackList);
}
