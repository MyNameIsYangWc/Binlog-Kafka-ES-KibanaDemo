package top.lzzly.sync.kafka.server;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import org.springframework.stereotype.Service;
import top.lzzly.sync.kafka.config.Config;

import java.util.List;

/**
 * @Author : Liuzz
 * @Description: ES 通用业务逻辑层
 * @Date : 2018/9/14  09:24
 * @Modified By :
 */

@Service
public class ESService{

    private JestClient client;

    public ESService(JestClient client) {
        this.client = client;
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

    public Index getUpdateIndex(String id, String esType, Object object) {
        return new Index.Builder(object).index(Config.ES_INDICES).type(esType).id(id).refresh(true).build();
    }

    public Delete getDeleteIndex(String id, String esType) {
        return new Delete.Builder(id).index(Config.ES_INDICES).type(esType).build();
    }

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
