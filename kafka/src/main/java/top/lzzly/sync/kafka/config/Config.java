package top.lzzly.sync.kafka.config;

/**
 * @Author : Liuzz
 * @Description: config 配置文件
 * @Date : 2019/3/20  12:04
 * @Modified By :
 */
public interface Config {

    String ES_HOST = "http://localhost:9200";

    String ES_INDICES = "temmoliu";
    String ES_USER_TYPE = "user";
    String ES_ROLE_TYPE = "role";

    String KAFKA_JSON_TOPICS = "binlog";
    String KAFKA_JSON_ID = "consumer2";

}
