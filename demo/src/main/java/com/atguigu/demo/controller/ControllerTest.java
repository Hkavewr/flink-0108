package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//通过Controller这个注解来表示我们这个类为Controller层
//可以返回一个普通对象
//@ResponseBody
//@Controller+@ResponseBody=@RestController
@RestController
public class ControllerTest {

    //通过方法映射名来决定处理的请求调取哪个方法来处理
    @RequestMapping("test")
    public String test(){
        System.out.println("123");
        return "success";
    }

    //携带参数的请求
    @RequestMapping("test1")
    public String test2(@RequestParam("name") String na,
                        @RequestParam("age") Integer ag){
        System.out.println("123");
        return "姓名："+na+"年龄："+ag;
    }

}
