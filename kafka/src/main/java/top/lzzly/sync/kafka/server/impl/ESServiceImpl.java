package top.lzzly.sync.kafka.server.impl;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import top.lzzly.sync.kafka.config.EsJestClient;
import top.lzzly.sync.kafka.entity.User;
import top.lzzly.sync.kafka.server.ESService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @Description: ES 通用逻辑
 * @Date : 2020/05/30
 * @Author : 杨文超
 */
@Service
public class ESServiceImpl implements ESService {

    private Logger logger= LoggerFactory.getLogger(ESServiceImpl.class);

    JestClient client = EsJestClient.getClient();

    /**
     * 创建索引
     * @param index
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public void createIndex(String index){

        JestResult execute=null;
        try {
            execute = client.execute(new CreateIndex.Builder(index).build());
        } catch (IOException e) {
            logger.error("创建索引ERROR:"+e);
            e.printStackTrace();
        }
        logger.warn("创建索引:"+execute.isSucceeded()+"信息:"+execute.getJsonString());
    }

    /**
     * 删除索引
     * @param index
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public void delteIndex(String index) {

        JestResult execute=null;
        try {
            execute = client.execute(new DeleteIndex.Builder(index).build());
        } catch (IOException e) {
            logger.error("删除索引ERROR:"+e);
            e.printStackTrace();
        }
        logger.warn("删除索引:"+execute.isSucceeded()+"信息:"+execute.getJsonString());
    }

    /**
     * 检查索引是否存在
     * @param index
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public void indicatesExists(String index) {

        JestResult execute=null;
        try {
            execute = client.execute(new IndicesExists.Builder(index).build());
        } catch (IOException e) {
            logger.error("检查索引是否存在异常:"+e);
            e.printStackTrace();
        }
        logger.warn("检查索引是否存在:"+execute.isSucceeded()+",信息:"+execute.getJsonString());
    }

    /**
     * 创建文档/更新文档
     * @param index 索引 (库名)
     * @param type (表名)
     * @param entity 文档(数据)
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public void operationDocument(String index,String type, List entity) {

        //Index集合
        List<Index> indices = new ArrayList<>();
        for (int i = 0; i < entity.size(); i++) {
            indices.add(getUpdateIndex(
                    index,
                    UUID.randomUUID().toString().replaceAll("-",""),//文档id
                    type,//文档类型
                    entity.get(i)));//文档数据
        }

        //批量更新文档
        boolean flag = executeESClientRequest(indices);

        System.out.println(flag);
    }

    /**
     * 读取文档
     * @param search 条件
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public void readDocument(String search) {

        List<SearchResult.Hit<User, Void>> execute=null;
        try {
            execute = client.execute(new Search.Builder(search).build()).getHits(User.class);
        } catch (IOException e) {
            logger.error("读取文档异常:"+e);
            e.printStackTrace();
        }
        logger.warn("读取文档:"+execute.get(0).id+",信息:"+execute.get(0).source);
    }

    /**
     * 删除文档
     * @param index
     * @param type
     * @param id
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public void deleteDocument(String index,String type,List<String> id) {

        //Delete集合
        List<Delete> deleteIndex = new ArrayList<>();
        for (int i = 0; i < id.size(); i++) {
            deleteIndex.add(getDeleteIndex(
                    index,
                    id.get(i),//文档id
                    type));//文档类型
        }

        //批量更新文档
        boolean flag = executeESClientRequest(deleteIndex);

    }

    public static void main(String[] args) {
//        String search = "{" +
//                "  \"query\": {" +
//                "    \"bool\": {" +
//                "      \"must\": [" +
//                "        { \"match\": { \"name\": \"gg33\" }}" +
//                "      ]" +
//                "    }" +
//                "  }" +
//                "}";
//        new ESServiceImpl().readDocument(search);
        ArrayList<Object> objects1 = new ArrayList<>();
        //Delete集合
        ESServiceImpl esService = new ESServiceImpl();
        Delete deleteIndex = esService.getDeleteIndex(
                "temmoliu",
                "5424",//文档id
                "user");//文档类型

        objects1.add(deleteIndex);

        User user = new User();
        user.setId("66");
//        user.setName("杨文超");
        Index updateIndex = esService.getUpdateIndex("temmoliu", "5424", "role", user);
        objects1.add(updateIndex);

        esService.executeESClientRequest(objects1);
    }

    /**
     * 更新/创建 index
     * @param id
     * @param esType
     * @param object
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public Index getUpdateIndex(String index,String id, String esType, Object object) {
        return new Index.Builder(object).index(index).type(esType).id(id).refresh(true).build();
    }

    /**
     * 删除Index
     * @param index
     * @param id
     * @param esType
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public Delete getDeleteIndex(String index,String id, String esType) {
        return new Delete.Builder(id).index(index).type(esType).build();
    }

    /**
     * 数据同步ES
     * @param list
     * @Date : 2020/05/30
     * @Author : 杨文超
     */
    @Override
    public boolean executeESClientRequest(List list) {
        Bulk bulk = new Bulk.Builder()
                .addAction(list)
                .build();
        list.clear();
        try {
            JestResult result = client.execute(bulk);
            logger.warn("数据同步ES:"+result.isSucceeded()+",信息:"+result.getJsonString());
            return result != null && result.isSucceeded();
        } catch (Exception ignore) {
            logger.error("数据同步ES异常:"+ignore);
        }
        return false;
    }
}
