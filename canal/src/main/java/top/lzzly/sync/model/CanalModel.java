package top.lzzly.sync.model;

public class CanalModel {
    private String event;
    private Object value;

    public CanalModel(String event,Object value){
        this.event=event;
        this.value=value;
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

    @Override
    public String toString() {
        return "CanalModel{" +
                "event='" + event + '\'' +
                ", value=" + value +
                '}';
    }
}
