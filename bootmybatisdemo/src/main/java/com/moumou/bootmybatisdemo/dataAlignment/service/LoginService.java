package com.moumou.bootmybatisdemo.dataAlignment.service;

import com.moumou.bootmybatisdemo.dataAlignment.model.UserLogin;

public interface LoginService {

    String login(UserLogin userLogin);

    String register(UserLogin userLogin);
}
