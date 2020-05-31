package top.lzzly.sync.kafka.entity;

import io.searchbox.annotations.JestId;

import java.io.Serializable;

/**
 * @Description: Role实体类
 * @Date : 2020/5/30
 * @Author : 杨文超
 */
public class Role implements Serializable {
    @JestId
    private String id;
    private String name;

    public Role() {
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
        return "Role{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
