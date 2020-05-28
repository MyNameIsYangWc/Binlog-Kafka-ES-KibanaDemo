package top.lzzly.sync.kafka.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import top.lzzly.sync.kafka.config.Config;
import top.lzzly.sync.kafka.config.EsJestClient;
import top.lzzly.sync.kafka.server.ESService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @Author : yangwc
 * @Description: 消费者
 * @Date : 2018/9/19  09:48
 * @Modified By :
 */
@Component
public class JsonConsumer {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    @Value("${es.data.format.user}")
    String userFormat;
    @Value("${es.data.format.role}")
    String roleFormat;

    JestClient client = EsJestClient.getClient();
    ESService documentDao = new ESService(client);

    @KafkaListener(topics = Config.KAFKA_JSON_TOPICS, id = Config.KAFKA_JSON_ID, containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> list) {
        logger.warn("消费者监听中。。。");
        List<String> messages = new ArrayList<>();
        for (ConsumerRecord<?, ?> record : list) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            // 获取消息
            kafkaMessage.ifPresent(o -> messages.add(o.toString()));
        }
        if (messages.size() > 0) {
            // 更新索引
            updateES(messages);
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取ES的TYPE
     *
     * @param tableName
     * @return
     */
    private String getESType(String tableName) {
        String esType = "";
        switch (tableName) {
            case "role": {
                esType = Config.ES_ROLE_TYPE;
                break;
            }
            case "user": {
                esType = Config.ES_USER_TYPE;
                break;
            }
        }
        return esType;
    }

    /**
     * 获取消息JSON解析格式
     *
     * @param tableName
     * @return
     */
    private String getJsonFormat(String tableName) {
        String format = "";
        switch (tableName) {
            case "role": {
                format = roleFormat;
                break;
            }
            case "user": {
                format = userFormat;
                break;
            }
        }
        return format;
    }


    /**
     * 获取解析后的ES对象
     *
     * @param message
     * @param tableName
     * @return
     */
    private JSONObject getESObject(JSONArray message, String tableName) {
        JSONObject resultObject = new JSONObject();
        String format = getJsonFormat(tableName);
        if (!format.isEmpty()) {
            JSONObject jsonFormatObject = JSON.parseObject(format);
            for (String key : jsonFormatObject.keySet()) {
                String[] formatValues = jsonFormatObject.getString(key).split(",");
                if (formatValues.length < 2) {
                    resultObject.put(key, message.get(jsonFormatObject.getInteger(key)));
                } else {
                    Object object = message.get(Integer.parseInt(formatValues[0]));
                    if (object == null) {
                        String[] array = {};
                        resultObject.put(key, array);
                    } else {
                        String objectStr = message.get(Integer.parseInt(formatValues[0])).toString();
                        String[] result = objectStr.split(formatValues[1]);
                        resultObject.put(key, result);
                    }
                }
            }
        }
        return resultObject;
    }


    /**
     * 更新ES索引
     *
     * @param messages
     */
    private void updateES(List<String> messages) {
        List<Index> updateUserList = new ArrayList<>();
        List<Index> updateRoleList = new ArrayList<>();
        List<Delete> deleteUserList = new ArrayList<>();
        List<Delete> deleteRoleList = new ArrayList<>();
        for (String message : messages) {
            JSONObject result = null;
            try {
                result = JSON.parseObject(message);
            } catch (Exception e) {
                continue;
            }
            // 获取事件类型 event:"wtv3.videos.insert"
            String event = (String) result.get("event");
            String[] eventArray = event.split("\\.");
            String tableName = eventArray[1];
            String eventType = eventArray[2];
            // 获取具体数据
            JSONArray valueStr = (JSONArray) result.get("value");
            // 转化为对应格式的json字符串
            JSONObject object = getESObject(valueStr, tableName);
            // 获取ES的type
            String esType = getESType(tableName);
            switch (eventType) {
                case "insert": {
                    appendUpdateList(updateUserList, updateRoleList, object, esType);
                    break;
                }
                case "update": {
                    // 更新videos
                    appendUpdateList(updateUserList, updateRoleList, object, esType);
                    break;
                }
                case "delete": {
                    // 删除videos
                    appendDeleteList(deleteUserList, deleteRoleList, object, esType);
                    break;
                }
            }
        }
        if (updateUserList.size() > 0) {
            documentDao.executeESClientRequest(updateUserList, Config.ES_USER_TYPE);
        }
        if (updateRoleList.size() > 0) {
            documentDao.executeESClientRequest(updateRoleList, Config.ES_ROLE_TYPE);
        }
        if (deleteUserList.size() > 0) {
            documentDao.executeESClientRequest(deleteUserList, Config.ES_USER_TYPE);
        }
        if (deleteRoleList.size() > 0) {
            documentDao.executeESClientRequest(deleteRoleList, Config.ES_ROLE_TYPE);
        }
    }

    private void appendDeleteList(List<Delete> userList, List<Delete> roleList, JSONObject object, String esType) {
        switch (esType) {
            case Config.ES_USER_TYPE: {
                userList.add(documentDao.getDeleteIndex(object.get("id").toString(), esType));
                break;
            }
            case Config.ES_ROLE_TYPE: {
                roleList.add(documentDao.getDeleteIndex(object.get("id").toString(), esType));
                break;
            }
        }
    }

    private void appendUpdateList(List<Index> userList, List<Index> roleList, JSONObject object, String esType) {
        switch (esType) {
            case Config.ES_USER_TYPE: {
                userList.add(documentDao.getUpdateIndex(object.get("id").toString(), esType, object));
                break;
            }
            case Config.ES_ROLE_TYPE: {
                roleList.add(documentDao.getUpdateIndex(object.get("id").toString(), esType, object));
                break;
            }
        }
    }

}
