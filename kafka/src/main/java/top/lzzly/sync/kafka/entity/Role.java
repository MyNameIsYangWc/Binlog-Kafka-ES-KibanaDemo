package top.lzzly.sync.kafka.entity;

import io.searchbox.annotations.JestId;

import java.io.Serializable;

/**
 * @Author : yangwc
 * @Description: Role实体类
 * @Date : 2019/3/20  11:56
 * @Modified By :
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
