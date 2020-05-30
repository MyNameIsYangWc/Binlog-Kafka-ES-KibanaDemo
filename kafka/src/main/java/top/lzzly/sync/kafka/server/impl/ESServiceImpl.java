package top.lzzly.sync.kafka.server.impl;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import top.lzzly.sync.kafka.config.Config;
import top.lzzly.sync.kafka.config.EsJestClient;
import top.lzzly.sync.kafka.server.ESService;

import java.io.IOException;
import java.util.List;

/**
 * @Author : yangwc
 * @Description: ES 通用逻辑
 * @Date : 2018/9/14  09:24
 * @Modified By :
 */
@Service
public class ESServiceImpl implements ESService {

    private Logger logger= LoggerFactory.getLogger(ESServiceImpl.class);

    JestClient client = EsJestClient.getClient();

    public static void main(String[] args) {
        new ESServiceImpl().delteIndex("temmoliu");
    }

    /**
     * 创建索引
     * @param index
     * @Author yangwc
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





    public boolean update(String id, String esType, Object object) {
        Index index = new Index.Builder(object).index(Config.ES_INDICES).type(esType).id(id).refresh(true).build();
        try {
            JestResult result = client.execute(index);
            return result != null && result.isSucceeded();
        } catch (Exception ignore) {
        }
        return false;
    }

    @Override
    public Index getUpdateIndex(String id, String esType, Object object) {
        return new Index.Builder(object).index(Config.ES_INDICES).type(esType).id(id).refresh(true).build();
    }

    @Override
    public Delete getDeleteIndex(String id, String esType) {
        return new Delete.Builder(id).index(Config.ES_INDICES).type(esType).build();
    }

    @Override
    public boolean executeESClientRequest(List indexList, String esType) {
        Bulk bulk = new Bulk.Builder()
                .defaultIndex(Config.ES_INDICES)
                .defaultType(esType)
                .addAction(indexList)
                .build();
        indexList.clear();
        try {
            JestResult result = client.execute(bulk);
            return result != null && result.isSucceeded();
        } catch (Exception ignore) {
        }
        return false;
    }

    public boolean delete(String id, String esType) {
        try {
            DocumentResult result = client.execute(new Delete.Builder(id)
                    .index(Config.ES_INDICES)
                    .type(esType)
                    .build());
            return result.isSucceeded();
        } catch (Exception e) {
            throw new RuntimeException("delete exception", e);
        }
    }
}
