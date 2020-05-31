package top.lzzly.sync.kafka.server;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @Description: 表数据映射到es
 * @Date : 2020/05/31
 * @Author : 杨文超
 */
@Component
public class CommonService {

    private static Logger logger= LoggerFactory.getLogger(CommonService.class);

    @Autowired
    private ESService documentDao;

    /**
     *  生成index/Delete 对象存入list
     * @param eventType
     * @param esType
     * todo 增加实体类,此处需增加对应的逻辑
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    public void ObjectSaveList(String index,String esType,JSONObject entity,String eventType,List userList,List roleList) {

        switch (eventType) {

            case "insert":
            case "update":
                logger.warn("###消费者监听事件:" + eventType);
                if ("user".equalsIgnoreCase(esType)) {
                    logger.warn("user表");
                    userList.add(
                            documentDao.getUpdateIndex(index, entity.get("id").toString(), esType, entity));
                }
                if ("role".equalsIgnoreCase(esType)) {
                    logger.warn("role表");
                    roleList.add(
                            documentDao.getUpdateIndex(index, entity.get("id").toString(), esType, entity));
                }
                break;

            case "delete":
                logger.warn("###消费者监听事件:" + eventType);
                if ("user".equalsIgnoreCase(esType)) {
                    logger.warn("user表");
                    userList.add(
                            documentDao.getDeleteIndex(index, entity.get("id").toString(), esType));
                }
                if ("role".equalsIgnoreCase(esType)) {
                    logger.warn("role表");
                    roleList.add(
                            documentDao.getDeleteIndex(index, entity.get("id").toString(), esType));
                }
                break;
        }
    }

    /**
     * 执行es客户端请求
     * @param tableList 每个表对应的list
     *  TODO 增加实体类,此处需增加对应的逻辑
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    public void executeESClientRequest(Map<String,List> tableList){
        if(tableList!=null){
            if (tableList.get("user") != null && tableList.get("user").size()>0) {
                documentDao.executeESClientRequest(tableList.get("user"));
            }
            if (tableList.get("role") != null && tableList.get("user").size()>0) {
                documentDao.executeESClientRequest(tableList.get("role"));
            }
        }
    }
}
