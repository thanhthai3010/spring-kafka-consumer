package com.consumer.model;

public class UserModel {
    private String name;
    private String dept;

    public UserModel() {
    }

    public UserModel(String name, String dept) {
        this.name = name;
        this.dept = dept;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDept() {
        return dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }

    @Override
    public String toString() {
        return "UserModel{" +
                "name='" + name + '\'' +
                ", dept='" + dept + '\'' +
                '}';
    }
}
