package com.moumou.bootmybatisdemo.dataAlignment.controller;

import com.alibaba.fastjson.JSONObject;
import com.moumou.bootmybatisdemo.dataAlignment.model.UserLogin;
import com.moumou.bootmybatisdemo.dataAlignment.service.LoginService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hello")
public class LoginController {

    @Autowired
    private LoginService loginService;
//
    @RequestMapping(value = "/login",method = RequestMethod.POST )
    public @ResponseBody JSONObject login(@RequestBody UserLogin user){
        String flag = loginService.login(user);
        JSONObject res = new JSONObject();
        JSONObject data = new JSONObject();
        JSONObject body = new JSONObject();

        res.put("res", flag);
        if("1".equals(flag)) {
            res.put("token", flag);
            body.put("body",res);
            data.put("data",body);
        }
        return data;

    }

    @RequestMapping(value = "/rst" ,method = RequestMethod.POST)
    public @ResponseBody JSONObject register(@RequestBody UserLogin user){
        String flag = loginService.register(user);
        JSONObject res = new JSONObject();
        res.put("data", flag);
        return res;
    }

}
