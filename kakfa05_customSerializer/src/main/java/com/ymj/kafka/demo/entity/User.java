package com.ymj.kafka.demo.entity;

/**
 * @Classname User
 * @Description TODO
 * @Date 2021/8/25 17:27
 * @Created by yemingjie
 */
public class User {
    private Integer userId;

    private String username;

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
