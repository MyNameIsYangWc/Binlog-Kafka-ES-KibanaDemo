package top.lzzly.sync.kafka.server;

import io.searchbox.core.Delete;
import io.searchbox.core.Index;

import java.util.List;

/**
 * @Description: ES 接口
 * @Date : 2020/05/30
 * @Author : 杨文超
 */
public interface ESService{

    void createIndex(String index);

    void delteIndex(String index);

    void indicatesExists(String index);

    public void operationDocument(String index,String type, List entity);

    void readDocument(String search);

    boolean executeESClientRequest(List updateUserList);

    Delete getDeleteIndex(String index,String id, String esType);

    Index getUpdateIndex(String index,String id, String esType, Object object);

    void deleteDocument(String index,String type,List<String> id);
}
