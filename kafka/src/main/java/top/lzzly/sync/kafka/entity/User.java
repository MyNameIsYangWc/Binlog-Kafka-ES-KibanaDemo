package top.lzzly.sync.kafka.entity;

import io.searchbox.annotations.JestId;

import java.io.Serializable;

/**
 * @Author : yangwc
 * @Description: User实体类
 * @Date : 2019/3/20  11:56
 * @Modified By :
 */
public class User implements Serializable {

    private String id;
    private String name;

    public User() {
    }

    public User(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
