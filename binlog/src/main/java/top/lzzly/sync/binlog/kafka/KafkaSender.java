package top.lzzly.sync.binlog.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author : Liuzz
 * @Description: kafka生产者
 * @Date : 2019/3/20  10:27
 * @Modified By :
 */
@Component
public class KafkaSender {

    @Autowired
    KafkaTemplate kafkaTemplate;

    public void createTopic(String host,String topic,int partNum,short repeatNum) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,host);

        NewTopic newTopic = new NewTopic(topic, partNum, repeatNum);

        AdminClient adminClient = AdminClient.create(props);
        List<NewTopic> topicList = Arrays.asList(newTopic);
        adminClient.createTopics(topicList);

        adminClient.close(10, TimeUnit.SECONDS);
    }

    public void send(String topic,String msg){
        Message message=new Message();
        message.setId(System.currentTimeMillis());
        message.setMsg(msg);
        message.setSendTime(new Date());
        kafkaTemplate.send(topic,JSON.toJSONString(msg));
        kafkaTemplate.send(topic,msg);
    }
}
