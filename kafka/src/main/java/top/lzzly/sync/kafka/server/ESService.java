package top.lzzly.sync.kafka.server;

import io.searchbox.core.Delete;
import io.searchbox.core.Index;

import java.util.List;
import java.util.Map;

/**
 * @Description: ES 接口
 * @Date : 2020/05/30
 * @Author : 杨文超
 */
public interface ESService{

    void createIndex(String index);

    void delteIndex(String index);

    void indicatesExists(String index);

    public void operationDocument(String index,List<Map> entity);

    void readDocument(String search);

    boolean executeESClientRequest(List updateUserList);

    Delete getDeleteIndex(String index,String id);

    Index getUpdateIndex(String index,String id,Object object);

    void deleteDocument(String index,List<String> id);
}
