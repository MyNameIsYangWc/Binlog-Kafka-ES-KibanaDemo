package top.lzzly.sync.kafka.server;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

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
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    public void ObjectSaveList(String index,String esType,JSONObject entity,String eventType,List list) {

        switch (eventType) {

            case "insert":
            case "update":
                logger.warn("###消费者监听事件:" + eventType+",###操作表:" + esType);
                list.add(documentDao.getUpdateIndex(index,esType+"."+entity.get("id"), entity));
                break;

            case "delete":
                logger.warn("###消费者监听事件:" + eventType+",###操作表:" + esType);
                list.add(documentDao.getDeleteIndex(index,esType+"."+entity.get("id")));
                break;
        }
    }

    /**
     * 执行es客户端请求
     * @param tableList
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    public void executeESClientRequest(List tableList){
       if(tableList != null && tableList.size()>0){
           documentDao.executeESClientRequest(tableList);
       }
    }
}
