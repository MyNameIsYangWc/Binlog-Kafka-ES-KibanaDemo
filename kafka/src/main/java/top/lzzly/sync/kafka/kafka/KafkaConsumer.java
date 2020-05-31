package top.lzzly.sync.kafka.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import top.lzzly.sync.kafka.common.util.OrmEntityUtil;
import top.lzzly.sync.kafka.config.Config;
import top.lzzly.sync.kafka.server.CommonService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @Description: kafka消费者
 * @Date : 2020/05/30
 * @Author : 杨文超
 */
@Component
public class KafkaConsumer {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    @Autowired
    CommonService commonService;

    /**
     * @Description: kafka 监听器
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @KafkaListener(topics = Config.KAFKA_JSON_TOPICS, id = Config.KAFKA_JSON_ID, containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> list) {

        logger.warn("消费者监听中。。。");
        List<String> messages = new ArrayList<>();
        for (ConsumerRecord<?, ?> record : list) {

            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            logger.warn("###消费者监听数据:"+kafkaMessage);
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

        //每个表对应一个list TODO 增加实体类此处需增加对应的list
        List userList = new ArrayList<>();//user
        List roleList = new ArrayList<>();//role

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
            String index = eventArray[0]; //数据库名
            String tableName = eventArray[1];//表名
            String eventType = eventArray[2];//数据操作类型 insert/update/delete
            JSONArray valueStr = (JSONArray) result.get("value"); // 获取具体数据
            String esType = tableName.toLowerCase(); // 获取ES的type

            // 转化为对应格式的json字符串
            JSONObject entity = OrmEntityUtil.ormEntity(tableName,valueStr);
            //生成index/Delete 对象存入list
            commonService.ObjectSaveList(index,esType,entity,eventType,userList,roleList);
        }

        //执行es客户端请求 TODO 增加实体类此处需增加对应的list到tableList中
        List<List> tableList = new ArrayList<>();
        tableList.add(userList);
        tableList.add(roleList);
        commonService.executeESClientRequest(tableList);
    }
}
