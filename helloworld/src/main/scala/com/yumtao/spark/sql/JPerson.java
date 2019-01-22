package com.yumtao.spark.sql;

import java.io.Serializable;

/**
 * Created by yumtao on 2019/1/22.
 */
public class JPerson implements Serializable {
    private int id;
    private String name;
    private int age;

    public JPerson() {
    }

    public JPerson(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "JPerson{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
