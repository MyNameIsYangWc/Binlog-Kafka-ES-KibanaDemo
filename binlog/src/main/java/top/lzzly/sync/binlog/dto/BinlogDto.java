package top.lzzly.sync.binlog.dto;

/**
 * @Author :
 * @Description: Binlog数据传输对象
 * @Date : 2019/3/18  11:01
 * @Modified By :
 */
public class BinlogDto {
    private String event;
    private Object value;

    public BinlogDto(String event, Object value) {
        this.event = event;
        this.value = value;
    }

    public BinlogDto() {
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
