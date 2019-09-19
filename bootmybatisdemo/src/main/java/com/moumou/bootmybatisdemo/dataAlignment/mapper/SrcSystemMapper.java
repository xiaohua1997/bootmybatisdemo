package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import org.apache.ibatis.annotations.*;
import java.util.List;

import com.moumou.bootmybatisdemo.dataAlignment.model.SrcSystem;

@Mapper
//@Repository
public interface SrcSystemMapper {
    //查询（全量）
    @Select("select * from src_system ")
    List<SrcSystem> queryDBS();
    //修改
    @Update("update src_system set db_type = #{dbType}, db_version =#{dbVersion}, db_charset =#{dbCharset}, db_ip =#{dbIp}, " +
            "db_port =#{dbPort}, username =#{username}, password =#{password}, encrpassword =#{encrpassword}, remark =#{remark} \n" +
            "where sys =#{sys} and sys_num =#{sysNum} and db_sid = #{dbSid} and db_schema =#{dbSchema} ")
    int uptateSrcSys(SrcSystem srcSystem);
//    //不调用这个删除
//    @Delete("delete table src_system where sys =#{sys} and sys_num =#{sysNum} and db_sid = #{dbSid} and db_schema =#{dbSchema}")
//    void delSrcSys(SrcSystem srcSystem);
    //删除（置无效）
    @Update("update src_system set invalid = now() where sys = #{sys} and sys_num = #{sysNum} and db_sid = #{dbSid} and db_schema = #{dbSchema}")
    boolean delSrcSys(SrcSystem srcSystem);
    /*增加*/
    @Insert("insert into src_system(sys, sys_num, db_type, db_version, db_sid, db_schema, db_charset, db_ip,  db_port,  username,  password, encrpassword, remark) \n" +
            "values(#{sys},#{sysNum},#{dbType},#{dbVersion},#{dbSid},#{dbSchema},#{dbCharset},#{dbIp},#{dbPort},#{username},#{password},#{encrpassword},#{remark})")
    int addSrcSys(SrcSystem srcSystem);
    //根据sys查询sid
    @Select("select distinct(db_sid) from src_system where sys = #{sys}")
    List<String> querySid(SrcSystem srcSystem);
    //根据sys和sid查询schema
    @Select("select distinct(db_schema) from src_system where sys = #{sys} and db_sid = #{dbSid}")
    List<String> querySchema(SrcSystem srcSystem);
}
