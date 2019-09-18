package com.moumou.bootmybatisdemo.dataAlignment.mapper;

import com.moumou.bootmybatisdemo.dataAlignment.model.UserLogin;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface RegisterMapper {

    @Insert("insert into user (username,password) values (#{username},#{password})")
    Boolean registerUser(UserLogin userLogin);
}
