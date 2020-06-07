package top.lzzly.sync.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Description: kafka生产者
 * @Date : 2020/3/30
 * @Author :杨文超
 */
@Component
public class KafkaSender {

    @Autowired
    KafkaTemplate kafkaTemplate;

    public void createTopic(String host,String topic,int partNum,short repeatNum) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,host);
        properties.put("enable.auto.commit", false);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest"); // 如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", "40000"); // 必须大于session.timeout.ms的设置
        properties.put("session.timeout.ms", "30000"); // 默认为30秒
        properties.put("isolation.level", "read_committed");
        properties.put("max.poll.records", 1024);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        NewTopic newTopic = new NewTopic(topic, partNum, repeatNum);

        AdminClient adminClient = AdminClient.create(properties);
        List<NewTopic> topicList = Arrays.asList(newTopic);
        adminClient.createTopics(topicList);

        adminClient.close(10, TimeUnit.SECONDS);
    }

    public <T>void send(String topic,T msg){
        kafkaTemplate.send(topic,msg);
    }
}
