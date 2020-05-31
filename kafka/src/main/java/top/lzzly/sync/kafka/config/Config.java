package top.lzzly.sync.kafka.config;

/**
 * @Description: config 配置文件
 * @Date : 2020/5/30
 * @Author : 杨文超
 */
public interface Config {

    String ES_HOST = "http://localhost:9200";

    String KAFKA_JSON_TOPICS = "binlog";
    String KAFKA_JSON_ID = "consumer2";

}
