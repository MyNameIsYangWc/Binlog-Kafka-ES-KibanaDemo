package top.lzzly.sync.binlog.kafka;

import java.util.Date;

/**
 * @Description: Kafka数据传输对象
 * @Date : 2020/3/30
 * @Author : 杨文超
 */
public class Message {
    private Long id;
    private String msg;
    private Date sendTime;

    public Message(Long id, String msg, Date sendTime) {
        this.id = id;
        this.msg = msg;
        this.sendTime = sendTime;
    }

    public Message() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Date getSendTime() {
        return sendTime;
    }

    public void setSendTime(Date sendTime) {
        this.sendTime = sendTime;
    }
}
