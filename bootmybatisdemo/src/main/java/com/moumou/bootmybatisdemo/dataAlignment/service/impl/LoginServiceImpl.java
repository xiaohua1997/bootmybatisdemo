package com.moumou.bootmybatisdemo.dataAlignment.service.impl;

import com.moumou.bootmybatisdemo.dataAlignment.mapper.LoginMapper;
import com.moumou.bootmybatisdemo.dataAlignment.mapper.RegisterMapper;
import com.moumou.bootmybatisdemo.dataAlignment.model.User;
import com.moumou.bootmybatisdemo.dataAlignment.model.UserLogin;
import com.moumou.bootmybatisdemo.dataAlignment.service.LoginService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LoginServiceImpl implements LoginService {

    @Autowired
    private LoginMapper loginMapper;

    @Autowired
    private RegisterMapper registerMapper;

    @Override
    public String login(UserLogin userLogin) {
        User userFruit = loginMapper.userQry((String) userLogin.getUsername());
        //用户不存在
        if (null == userFruit) {
            return "-1";
        } else {
            if (userLogin.getPassword().equals(userFruit.getPassword())) {
                return "1";
            } else {
                return "0";
            }
        }
    }

    @Override
    public String register(UserLogin userLogin) {
        boolean flag = registerMapper.registerUser(userLogin);
        if(flag){
            return "ok";
        }else{
            return "fail";
        }
    }
}