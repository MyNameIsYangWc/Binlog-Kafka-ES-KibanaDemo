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
    private int id;
    private String name;

    public Role() {
    }

    public Role(int id, String name) {
        this.id = id;
        this.name = name;
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
}
