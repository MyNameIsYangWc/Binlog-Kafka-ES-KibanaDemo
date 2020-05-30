package top.lzzly.sync.kafka.server;

import io.searchbox.core.Delete;
import io.searchbox.core.Index;

import java.util.List;

/**
 * @Author : yangwc
 * @Description: ES 接口
 * @Date : 2018/9/14  09:24
 * @Modified By :
 */
public interface ESService{

    void createIndex(String index);

    void delteIndex(String index);

    void indicatesExists(String index);

    void createDocument(String index,String type, Object object);

    void readDocument(String search);



    boolean executeESClientRequest(List updateUserList, String esUserType);

    Delete getDeleteIndex(String id, String esType);

    Index getUpdateIndex(String id, String esType, Object object);


    void createIndexMapping(String index, String type, String mappingString);


    void updateDocument(String index, String id, Object object);

    void deleteDocument(String index, String id, Object object);
}
