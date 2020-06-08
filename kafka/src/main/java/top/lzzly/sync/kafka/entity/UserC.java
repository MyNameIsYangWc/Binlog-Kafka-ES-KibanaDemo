package top.lzzly.sync.kafka.entity;

import java.io.Serializable;
import java.util.List;

/**
 * @Description: User实体类
 * @Date : 2020/05/30
 * @Date : 2020/5/30
 * @Author : 杨文超
 */
public class UserC implements Serializable {

    private String id;
    private String name;
    private List<Role> roles;

    public List<Role> getRoles() {
        return roles;
    }

    public void setRoles(List<Role> roles) {
        this.roles = roles;
    }

    public UserC() {
    }

    public UserC(String id, String name) {
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
