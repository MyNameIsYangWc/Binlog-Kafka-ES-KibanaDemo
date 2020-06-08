package top.lzzly.sync.kafka.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import top.lzzly.sync.kafka.entity.Role;
import top.lzzly.sync.kafka.entity.UserC;
import top.lzzly.sync.kafka.server.CommonService;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @Description: kafka消费者
 * @Date : 2020/06/07
 * @Author : 杨文超
 */
@Component
public class CanalKafkaConsumer {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    @Autowired
    CommonService commonService;

    /**
     * @Description: kafka 监听器
     * @Date : 2020/06/07
     * @Author : 杨文超
     */
    @KafkaListener(topics = "canal", id = "canalConsumer", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> list) {

        logger.warn("canalConsumer消费者监听中。。。");
        List<String> messages = new ArrayList<>();
        for (ConsumerRecord<?, ?> record : list) {

            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            logger.warn("###canalConsumer消费者监听数据:"+kafkaMessage);
            // 获取消息
            kafkaMessage.ifPresent(o -> messages.add(o.toString()));
        }

        // 更新索引文档
        if (messages.size() > 0) {
            updateES(messages);
        }
    }

    /**
     * 更新ES索引文档
     * @param messages kafka接收的消息体
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    private void updateES(List<String> messages) {

        List list = new ArrayList<>();

        for (String message : messages) {
            JSONArray result = JSON.parseArray(message);
            // 获取事件类型 event:"wtv3.videos.insert"
            String event = (String)((Map)result.get(0)).get("event");
            String[] eventArray = event.split("\\.");
            String index = eventArray[0]; //数据库名
            String tableName = eventArray[1];//表名
            String eventType = eventArray[2];//数据操作类型 insert/update/delete
            String esType = tableName.toLowerCase(); // 获取ES的type
            // todo 数据处理存入es
            Object value = ((Map) result.get(0)).get("value");
            Role role = JSONObject.parseObject(JSON.toJSONString(value), Role.class);
            ArrayList<Role> objects = new ArrayList<>();
            objects.add(role);
            UserC userC =new UserC();
            userC.setRoles(objects);
            userC.setId("330330");
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http")));
//            ArrayList<Role> objects = new ArrayList<>();
//            Role role = new Role();
//            role.setId("220220");
//            role.setName("UI和");
//            objects.add(role);
//            userC.setRoles(objects);
//
//            IndexRequest indexRequest = new IndexRequest(index).id(userC.getId());
//            indexRequest.source(JSON.toJSONBytes(userC),XContentType.JSON);
//
//            try {
//                client.index(indexRequest,RequestOptions.DEFAULT);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index);
            updateRequest.id(userC.getId());
            updateRequest.doc(JSON.toJSONBytes(userC), XContentType.JSON);

            try {
                client.update(updateRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
